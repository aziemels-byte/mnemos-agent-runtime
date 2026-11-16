import { createHash, randomUUID } from 'node:crypto';
import { hostname } from 'node:os';
import { setTimeout as sleep } from 'node:timers/promises';
import { identityState, processIdentity, processState } from './directory-lock.mjs';
import { FileJobStore, boundResult } from './job-store.mjs';
import { groupCarriesMarker, runSupervised, scanTree, stopTree } from './process-tree.mjs';
import {
  CANCELLED_BEFORE_DISPATCH, VALID_JOB_ID, VALID_PHASE, boundedSettlement, jsonInput, reconciledState,
  reconciliationRequest, settlementOutcome, thrownOutcome
} from './phase-job-registry.mjs';

const MAX_INPUT_BYTES = 65536;
const MAX_RUNNING = 512;
// The executor was never called because its dispatch could not be recorded.
const DISPATCH_NOT_RECORDED = Object.freeze({ status: 'FAILED', reason: 'DISPATCH_NOT_RECORDED',
  cancellationConfirmed: false, effectAbsent: true, reconciliationRequired: false });
// Errors after which writing the outcome again cannot succeed: the record was
// changed by someone else, is unreadable, or refuses the transition.
const PERMANENT = new Set(['JOB_RECORD_CHANGED', 'JOB_STATE_INVALID', 'JOB_TRANSITION_INVALID', 'JOB_NOT_FOUND', 'JOB_RECORD_INVALID']);
// Recorded when the process that owned a RUNNING job is provably gone. Before
// dispatch the executor was never called; after it, the effect is ambiguous.
const OWNER_DIED_BEFORE_DISPATCH = Object.freeze({ status: 'CANCELLED', reason: 'OWNER_DIED_BEFORE_DISPATCH',
  cancellationConfirmed: true, effectAbsent: true, reconciliationRequired: false });
const OWNER_DIED_AFTER_DISPATCH = Object.freeze({ status: 'UNKNOWN', reason: 'OWNER_DIED_AFTER_DISPATCH',
  cancellationConfirmed: false, effectAbsent: false, reconciliationRequired: true });

// Executions owned by registries in this process, listed before their record
// becomes visible and removed once their outcome is recorded (or can no longer
// be). A RUNNING record with this process's PID whose execution is not listed
// was left by an earlier process that had the same PID: it is not running.
const liveExecutions = new Set();

// This process's identity, recorded as the owner of the jobs it starts. On
// Linux it includes the boot ID and start time; elsewhere only { pid, host }.
let ownIdentity;
function ownerIdentity() {
  ownIdentity ??= processIdentity(process.pid);
  const { bootId, startTicks } = ownIdentity;
  return bootId === undefined ? {} : { bootId, startTicks };
}

/**
 * ALIVE, DEAD (provably gone), OTHER_HOST or UNREADABLE (cannot be verified).
 * Only DEAD permits reclassifying the owner's job. An owner recorded with its
 * process identity is DEAD once the host rebooted or its PID belongs to a
 * process that started at another time; one recorded without it (another
 * platform, an earlier version) is judged by its PID alone.
 */
function ownerState(owner) {
  if (owner?.pid === process.pid && owner.host === hostname()) {
    return liveExecutions.has(owner.executionId) ? 'ALIVE' : 'DEAD';
  }
  if (owner?.startTicks === undefined) return processState(owner);
  const state = identityState(owner);
  return state === 'UNVERIFIED' ? 'ALIVE' : state; // cannot compare: possibly alive
}

const OWNER_DIED_REASONS = new Set(['OWNER_DIED_BEFORE_DISPATCH', 'OWNER_DIED_AFTER_DISPATCH']);
const MAX_TERMINATION_WAIT_MS = 60_000;

/**
 * Whether the process trees a job recorded may still run, once its owner is gone:
 * RUNNING (a recorded supervisor or leader is verifiably the same process, or on
 * Linux a process carries a tree's environment marker), UNVERIFIED (a recorded
 * PID runs but cannot be verified, as on other platforms) or NONE.
 */
function processesState(record) {
  const trees = record.processes ?? [];
  let unverified = false;
  for (const tree of trees) {
    for (const identity of [tree.supervisor, tree.leader]) {
      if (!identity) continue;
      const state = identityState(identity);
      if (state === 'ALIVE') return 'RUNNING';
      if (state !== 'DEAD') unverified = true;
    }
    if (process.platform === 'linux' && scanTree(null, tree.marker, [], true).length > 0) return 'RUNNING';
  }
  return unverified ? 'UNVERIFIED' : 'NONE';
}
const sha256 = text => createHash('sha256').update(text).digest('hex');
const jobError = (code, message, details = {}) => Object.assign(new Error(message), { code, ...details });

function requireJobId(jobId) {
  if (typeof jobId !== 'string' || !VALID_JOB_ID.test(jobId)) throw new TypeError('Invalid job ID');
  return jobId;
}

/** The public view of a stored record. */
function snapshotOf(record) {
  return structuredClone({
    jobId: record.jobId,
    objective: record.objective,
    status: record.status,
    phase: record.phase,
    dispatched: record.dispatched,
    cancellationRequested: record.cancellationRequested,
    cancellationConfirmed: record.cancellationConfirmed,
    reconciliationRequired: record.reconciliationRequired,
    effectAbsent: record.effectAbsent,
    reason: record.reason,
    result: record.result,
    resultDiscarded: record.resultDiscarded,
    reconciliation: record.reconciliation ?? null,
    processes: record.processes ?? [],
    orphanTermination: record.orphanTermination ?? null,
    resumable: record.resumable === true,
    resumeOf: record.resumeOf ?? null,
    progress: record.progress ?? null,
    resumedBy: record.resumedBy ?? null,
    owner: record.owner,
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
    sequence: record.sequence
  });
}

/**
 * Phase-aware jobs whose identity and status survive a restart, shared by
 * processes on one host through a state directory. Same execution contract as
 * PhaseJobRegistry; every method returns a promise.
 *
 * Write-ahead order, each step durable before the next:
 *   1. start() records the job (RUNNING, phase "queued") before it resolves.
 *   2. dispatched: true is recorded before the executor is called.
 *   3. The outcome is recorded after the executor settles, only while the record
 *      is still this execution's.
 * Phase changes and cancellation requests are recorded in order in the
 * background; they never delay the executor or the abort. A job ID is run at
 * most once, across processes and restarts; a restart never replays a job.
 */
export class FilePhaseJobRegistry {
  #store;
  #live = new Map(); // jobs executing, or whose outcome is not yet recorded, in this instance
  #reserved = 0;

  constructor({ directory, execute, maxRunning = 64, clock = () => new Date(), lockTimeoutMs = 5_000, pollIntervalMs = 50 } = {}) {
    if (typeof execute !== 'function') throw new TypeError('execute is required');
    if (!Number.isInteger(maxRunning) || maxRunning < 1 || maxRunning > MAX_RUNNING) {
      throw new TypeError(`maxRunning must be an integer between 1 and ${MAX_RUNNING}`);
    }
    if (!Number.isInteger(pollIntervalMs) || pollIntervalMs < 1 || pollIntervalMs > 60_000) {
      throw new TypeError('pollIntervalMs must be an integer between 1 and 60000');
    }
    this.pollIntervalMs = pollIntervalMs;
    this.#store = new FileJobStore({ directory, clock, lockTimeoutMs });
    this.directory = this.#store.directory;
    this.lockFile = this.#store.lockFile;
    this.execute = execute;
    this.maxRunning = maxRunning;
    this.clock = clock;
  }

  /**
   * Record a new job and run it, or return the existing job with this ID without
   * running it again. Resolves once the job is durably recorded. Rejects with
   * JOB_IDENTITY_CONFLICT when the ID is taken by a different objective or input.
   */
  async start({ jobId, objective, input = null, resumable = false } = {}) {
    requireJobId(jobId);
    if (resumable !== true && resumable !== false) throw new TypeError('resumable must be a boolean');
    if (typeof objective !== 'string' || !objective.trim() || objective.length > 4000) {
      throw new TypeError('A bounded, non-empty objective is required');
    }
    const copy = jsonInput(input);
    const text = JSON.stringify(copy);
    if (Buffer.byteLength(text) > MAX_INPUT_BYTES) throw new TypeError('input must be JSON-serializable and at most 65536 bytes');
    const fingerprint = sha256(text);
    const existing = this.#live.get(jobId) ?? await this.#store.read(jobId);
    if (existing) return this.#sameIdentity(existing, objective, fingerprint, resumable);

    if (this.#live.size + this.#reserved >= this.maxRunning) {
      throw jobError('JOB_CAPACITY_EXHAUSTED', 'Job capacity exhausted', { running: this.#live.size });
    }
    this.#reserved++;
    const executionId = randomUUID();
    // Listed before the record becomes visible, so this process never sees it as dead.
    liveExecutions.add(executionId);
    let created;
    try {
      created = await this.#store.create({ jobId, objective, fingerprint, resumable,
        owner: { pid: process.pid, host: hostname(), executionId, ...ownerIdentity() } });
    } catch (error) {
      liveExecutions.delete(executionId);
      throw error;
    } finally {
      this.#reserved--;
    }
    if (!created.created) {
      liveExecutions.delete(executionId);
      return this.#sameIdentity(this.#live.get(jobId) ?? created.record, objective, fingerprint, resumable);
    }
    return this.#launch(created.record, copy, null);
  }

  /**
   * Continue a stopped, resumable job under a new job ID, from its last saved
   * progress. Never automatic; never while any attempt may still run.
   *
   *   resume({ jobId, expectedSequence, successorJobId, input, explicitAuthorization: true })
   *
   * - The job must have been started with resumable: true and be UNKNOWN,
   *   CANCELLED or FAILED (not COMPLETED), with its stop verified: not RUNNING and
   *   no recorded process still running (JOB_STOP_UNVERIFIED otherwise).
   * - input is the job's input again (only its fingerprint is stored); it must
   *   match. The successor has the same objective and input.
   * - Under one lock, the job is linked to its successor (resumedBy; an UNKNOWN
   *   job becomes CANCELLED with reason RESUMED) and the successor is recorded
   *   with resumeOf { jobId, rootJobId, attempt } and the saved progress. The
   *   successor then runs here; its executor receives resume: { fromJobId,
   *   rootJobId, attempt, progress }.
   * - If a crash came between those two writes, calling resume() again with the
   *   same successorJobId completes it (recover() lists it as resumeIncomplete).
   */
  async resume({ jobId, expectedSequence, successorJobId, input = null, explicitAuthorization = false } = {}) {
    requireJobId(jobId);
    requireJobId(successorJobId);
    if (successorJobId === jobId) throw new TypeError('The successor needs a new job ID');
    if (explicitAuthorization !== true) throw jobError('JOB_AUTHORIZATION_REQUIRED', 'Explicit authorization is required to resume a job');
    if (!Number.isSafeInteger(expectedSequence) || expectedSequence < 1) throw new TypeError('expectedSequence (from the inspected state) is required');
    const copy = jsonInput(input);
    const text = JSON.stringify(copy);
    if (Buffer.byteLength(text) > MAX_INPUT_BYTES) throw new TypeError('input must be JSON-serializable and at most 65536 bytes');
    if (this.#live.has(jobId)) throw jobError('JOB_RUNNING_HERE', 'The job is executing in this registry', { jobId });
    const liveSuccessor = this.#live.get(successorJobId);
    if (liveSuccessor) {
      if (liveSuccessor.record.resumeOf?.jobId === jobId) return this.#snapshot(liveSuccessor);
      throw jobError('JOB_IDENTITY_CONFLICT', 'The successor job ID is already taken', { jobId: successorJobId });
    }

    const record = await this.#store.read(jobId);
    if (!record) throw jobError('JOB_NOT_FOUND', 'No record exists for this job', { jobId });
    if (record.sequence !== expectedSequence) {
      throw jobError('JOB_RECORD_CHANGED', 'The job changed since it was inspected; inspect it again',
        { jobId, expectedSequence, currentSequence: record.sequence });
    }
    if (record.resumable !== true) throw jobError('JOB_NOT_RESUMABLE', 'The job was not started with resumable: true', { jobId });
    if (record.status === 'COMPLETED') throw jobError('JOB_NOT_RESUMABLE', 'A completed job has nothing to resume', { jobId });
    if (record.fingerprint !== sha256(text)) throw jobError('JOB_IDENTITY_CONFLICT', 'The input differs from the job being resumed', { jobId });
    if (record.status === 'RUNNING') {
      throw jobError('JOB_STOP_UNVERIFIED', 'The job may still run: let it settle, resolve it (get) or stop it first',
        { jobId, ownerState: ownerState(record.owner) });
    }
    const processes = processesState(record);
    if (processes !== 'NONE') {
      throw jobError('JOB_STOP_UNVERIFIED', 'Processes recorded by the job may still run', { jobId, processesState: processes });
    }
    // A job settled by an operator override: its owner may still be running it.
    if (record.reconciliation?.override && ownerState(record.owner) !== 'DEAD') {
      throw jobError('JOB_STOP_UNVERIFIED', 'The job was reconciled over a running or unverified owner, which may still run it',
        { jobId, ownerState: ownerState(record.owner) });
    }
    if (record.resumedBy !== undefined && record.resumedBy !== successorJobId) {
      throw jobError('JOB_ALREADY_RESUMED', 'The job was already resumed by another attempt', { jobId, resumedBy: record.resumedBy });
    }

    if (this.#live.size + this.#reserved >= this.maxRunning) {
      throw jobError('JOB_CAPACITY_EXHAUSTED', 'Job capacity exhausted', { running: this.#live.size });
    }
    this.#reserved++;
    const executionId = randomUUID();
    liveExecutions.add(executionId);
    let resumed;
    try {
      const changes = record.status === 'UNKNOWN'
        ? { status: 'CANCELLED', reason: 'RESUMED', reconciliationRequired: false, cancellationConfirmed: true, effectAbsent: false }
        : {};
      resumed = await this.#store.resume({ jobId, expectedSequence, changes,
        successor: { jobId: successorJobId, owner: { pid: process.pid, host: hostname(), executionId, ...ownerIdentity() } } });
    } catch (error) {
      liveExecutions.delete(executionId);
      throw error;
    } finally {
      this.#reserved--;
    }
    if (!resumed.created) { // resumed before: report that attempt, never run it twice
      liveExecutions.delete(executionId);
      return this.#resolve(resumed.successor);
    }
    const { successor } = resumed;
    return this.#launch(successor, copy, { fromJobId: jobId, rootJobId: successor.resumeOf.rootJobId,
      attempt: successor.resumeOf.attempt, progress: successor.progress ?? null });
  }

  #launch(record, input, resume) {
    const entry = {
      jobId: record.jobId, objective: record.objective, fingerprint: record.fingerprint, input, record, resume,
      state: 'running', phase: 'queued', phaseQueued: false,
      cancellationRequested: false, cancellationRequestedAt: null,
      controller: new AbortController(), queue: Promise.resolve(), decided: null
    };
    this.#live.set(record.jobId, entry);
    entry.settlement = this.#run(entry);
    return this.#snapshot(entry);
  }

  /**
   * The job's current state, or null when no such job was ever recorded. A
   * RUNNING job owned by another process carries ownerState. If that owner is
   * provably gone, the job is reclassified and the reclassification recorded
   * (see #resolve); it is never run again.
   */
  async get(jobId) {
    const live = this.#live.get(requireJobId(jobId));
    if (live) return this.#snapshot(live);
    const record = await this.#store.read(jobId);
    return record ? this.#resolve(record) : null;
  }

  /**
   * Wait until the job is no longer RUNNING: for a job executing in this
   * instance, until its outcome is recorded; for a job owned by another
   * process, by polling its record (an owner that dies is reclassified). With
   * timeoutMs, resolves to the current state when it elapses; a RUNNING result
   * is not proof of anything. Without it, a job owned by another host is waited
   * for indefinitely.
   */
  async wait(jobId, { timeoutMs } = {}) {
    requireJobId(jobId);
    if (timeoutMs !== undefined && (!Number.isInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > 2_147_483_647)) {
      throw new TypeError('timeoutMs must be a non-negative integer');
    }
    const deadline = timeoutMs === undefined ? Infinity : performance.now() + timeoutMs;
    for (;;) {
      const live = this.#live.get(jobId);
      if (live) {
        if (timeoutMs === undefined) await live.settlement;
        else await boundedSettlement(live.settlement, Math.max(0, Math.ceil(deadline - performance.now())));
        // The outcome is recorded, or failed to be recorded (UNKNOWN), or was
        // superseded by a reconciliation (read it); or the time is up.
        if (live.state === 'superseded') return this.get(jobId);
        return this.#snapshot(live);
      }
      const view = await this.get(jobId);
      const remaining = deadline - performance.now();
      if (view?.status !== 'RUNNING' || remaining <= 0) return view;
      await sleep(Math.min(this.pollIntervalMs, Math.ceil(remaining)));
    }
  }

  /**
   * Abort a job executing in this instance and wait up to timeoutMs for it to
   * settle. The request is recorded in order before the outcome, without
   * delaying the abort. A job running in another process cannot be aborted from
   * here (JOB_NOT_OWNED).
   */
  async requestCancellation(jobId, { timeoutMs = 1000 } = {}) {
    if (!Number.isInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > 30000) {
      throw new TypeError('timeoutMs must be an integer between 0 and 30000');
    }
    const entry = this.#live.get(requireJobId(jobId));
    if (!entry) {
      const view = await this.get(jobId);
      if (view?.status !== 'RUNNING') return view;
      throw jobError('JOB_NOT_OWNED', view.orphaned
        ? 'The job\'s owner is gone but its processes may still run; stop them with terminateOrphans()'
        : 'The job is not executing in this registry instance; cancel it where it runs',
      { jobId, owner: view.owner, ownerState: view.ownerState, orphaned: view.orphaned === true });
    }
    if (entry.state !== 'running') return this.#snapshot(entry);
    const first = !entry.cancellationRequested;
    entry.cancellationRequested = true;
    // Abort first: recording the request must never delay or prevent it. The
    // request is still recorded before the outcome, whose write is queued only
    // after the executor settles.
    entry.controller.abort();
    if (first) {
      try {
        entry.cancellationRequestedAt = this.#now();
        this.#write(entry, () => entry.record.cancellationRequested ? null
          : { cancellationRequested: true, cancellationRequestedAt: entry.cancellationRequestedAt }).catch(() => {});
      } catch { /* no timestamp: the request is reported here but not recorded */ }
    }
    await boundedSettlement(entry.settlement, timeoutMs);
    if (entry.state === 'superseded') return this.get(jobId);
    // An executor ignoring abort remains RUNNING; a timeout is not proof of termination.
    return this.#snapshot(entry);
  }

  /**
   * Every recorded job, each resolved like get(); records that cannot be read
   * are listed in `invalid` (treat those jobs as UNKNOWN).
   */
  async list() {
    const { records, invalid } = await this.#store.list();
    const jobs = [];
    for (const record of records) {
      const live = this.#live.get(record.jobId);
      jobs.push(live ? this.#snapshot(live) : await this.#resolve(record));
    }
    return { jobs, invalid };
  }

  /**
   * The restart step: resolve every recorded job, recording the outcome of each
   * job whose owner is provably gone. Never runs a job. Reports the jobs still
   * RUNNING under an owner that cannot be verified (another host), which need
   * an operator.
   */
  async recover() {
    const { jobs, invalid } = await this.list();
    return {
      jobs, invalid,
      reclassified: jobs.filter(job => OWNER_DIED_REASONS.has(job.reason)).map(job => job.jobId),
      unrecorded: jobs.filter(job => job.reason === 'OWNER_DIED_NOT_RECORDED').map(job => job.jobId),
      unverified: jobs.filter(job => job.status === 'RUNNING' && ['OTHER_HOST', 'UNREADABLE'].includes(job.ownerState))
        .map(job => job.jobId),
      orphaned: jobs.filter(job => job.orphaned === true).map(job => job.jobId),
      // Candidates for resume(): stopped, with their stop verified, and not resumed yet.
      resumable: jobs.filter(job => job.resumable && ['UNKNOWN', 'CANCELLED', 'FAILED'].includes(job.status) &&
        job.resumedBy === null && processesState(job) === 'NONE').map(job => job.jobId),
      // Linked to a successor that was never recorded (a crash between the two writes of resume()).
      // (A successor whose record is unreadable is listed in `invalid`, not here.)
      resumeIncomplete: jobs.filter(job => job.resumedBy !== null && !jobs.some(other => other.jobId === job.resumedBy) &&
        !invalid.some(entry => entry.file === `${sha256(job.resumedBy)}.job.json`)).map(job => job.jobId)
    };
  }

  /**
   * Resolve a job with authoritative evidence. Never runs the job.
   *
   *   reconcile({ jobId, expectedSequence, disposition, evidence, result, explicitAuthorization: true })
   *
   * - expectedSequence: the sequence of the state you inspected (get()); any
   *   change since then refuses with JOB_RECORD_CHANGED: inspect again.
   * - disposition: COMPLETED, FAILED or CANCELLED. evidence: { authoritative:
   *   true, source, effectAbsent? } (plain JSON, at most 8 KiB; kept for audit).
   *   effectAbsent: true in the evidence marks a FAILED or CANCELLED job safe to
   *   retry under a new job ID. result: plain JSON of at most 64 KiB.
   * - UNKNOWN jobs, and RUNNING jobs whose owner is provably gone, need nothing
   *   more. A RUNNING job whose owner is alive elsewhere or cannot be verified
   *   (another host) also needs overrideRunningOwner: true; the override is
   *   audited, and that owner's later outcome is refused. A job executing in
   *   this process is never reconciled (JOB_RUNNING_HERE).
   */
  async reconcile({ jobId, expectedSequence, overrideRunningOwner = false, ...request } = {}) {
    requireJobId(jobId);
    const normalized = reconciliationRequest(request);
    if (!Number.isSafeInteger(expectedSequence) || expectedSequence < 1) {
      throw new TypeError('expectedSequence (from the inspected state) is required');
    }
    if (overrideRunningOwner !== true && overrideRunningOwner !== false) throw new TypeError('overrideRunningOwner must be a boolean');
    const { result, resultDiscarded } = boundResult(normalized.result);
    if (resultDiscarded) throw new TypeError('result must be plain JSON of at most 64 KiB');
    if (this.#live.has(jobId)) throw jobError('JOB_RUNNING_HERE', 'The job is executing in this registry; it cannot be reconciled', { jobId });

    const record = await this.#store.read(jobId);
    if (!record) throw jobError('JOB_NOT_FOUND', 'No record exists for this job', { jobId });
    if (record.sequence !== expectedSequence) {
      throw jobError('JOB_RECORD_CHANGED', 'The job changed since it was inspected; inspect it again',
        { jobId, expectedSequence, currentSequence: record.sequence });
    }
    if (!['RUNNING', 'UNKNOWN'].includes(record.status)) {
      throw jobError('JOB_RECONCILIATION_NOT_REQUIRED', `The job already has a definite outcome (${record.status})`, { jobId });
    }
    let override = null;
    let state;
    if (record.status === 'RUNNING') {
      state = ownerState(record.owner);
      if (state === 'ALIVE' && record.owner.pid === process.pid && record.owner.host === hostname()) {
        throw jobError('JOB_RUNNING_HERE', 'The job is executing in this process; it cannot be reconciled', { jobId });
      }
      if (state !== 'DEAD') {
        if (!overrideRunningOwner) {
          throw jobError('JOB_OWNER_UNVERIFIED', `The job's owner is ${state === 'ALIVE' ? 'running' : 'not verifiable'}; ` +
            'confirm it has stopped, then pass overrideRunningOwner: true', { jobId, ownerState: state, owner: structuredClone(record.owner) });
        }
        override = { owner: structuredClone(record.owner), ownerState: state };
      } else {
        const processes = processesState(record);
        if (processes !== 'NONE') {
          if (!overrideRunningOwner) {
            throw jobError('JOB_PROCESSES_RUNNING', 'The job\'s owner is gone but its processes may still run; stop them ' +
              '(terminateOrphans) or confirm they have stopped and pass overrideRunningOwner: true', { jobId, processesState: processes });
          }
          override = { owner: structuredClone(record.owner), ownerState: state, processesState: processes };
        }
      }
    }
    const changes = reconciledState(record, { ...normalized, result }, { at: this.#now(), override, ownerState: state });
    // Compare-and-swap: refused if anything (including the owner) wrote since it was inspected.
    return snapshotOf(await this.#store.update(jobId, { expectedSequence, changes: { ...changes, resultDiscarded } }));
  }

  /**
   * A jobInspector for GoalRecoveryCoordinator: checkpointed job IDs are
   * resolved against the job records, which are the source of truth for status.
   *
   *   new GoalRecoveryCoordinator({ store, jobInspector: jobs.jobInspector() })
   *
   * Resolves like get() (a dead owner's job is reclassified and recorded; nothing
   * is run) to { status, reason, effectAbsent, reconciliationRequired }. A job
   * with no record is UNKNOWN (JOB_RECORD_MISSING): it may never have started,
   * or its record was lost; only reconciliation outside this store can tell. An
   * unreadable record is UNKNOWN (JOB_STATE_INVALID).
   */
  jobInspector() {
    return async job => {
      let view;
      try { view = await this.get(job.id); }
      catch (error) {
        if (error.code === 'JOB_STATE_INVALID') return { status: 'UNKNOWN', reason: 'JOB_STATE_INVALID', effectAbsent: false };
        throw error; // the coordinator reports INSPECTION_FAILED
      }
      if (!view) return { status: 'UNKNOWN', reason: 'JOB_RECORD_MISSING', effectAbsent: false, reconciliationRequired: true };
      // A resumed job continues in its successor: report the latest attempt.
      for (let hops = 0; view.resumedBy !== null && hops < 1000; hops++) {
        const next = await this.get(view.resumedBy);
        if (!next) return { status: 'UNKNOWN', reason: 'RESUME_INCOMPLETE', effectAbsent: false, reconciliationRequired: true };
        view = next;
      }
      return { status: view.status, reason: view.reason, effectAbsent: view.effectAbsent,
        reconciliationRequired: view.reconciliationRequired };
    };
  }

  /**
   * Linux: stop the processes of a job whose owner (and its supervisor) died
   * while they ran, using the trees recorded in the job (see runProcess).
   *
   *   terminateOrphans({ jobId, expectedSequence, explicitAuthorization: true, graceMs, killWaitMs })
   *
   * Each recorded tree is frozen, terminated and verified like terminate():
   * its group only while the group ID verifiably still names it (its leader is
   * the same process, or a member carries the tree's marker), and every process
   * carrying its marker. A supervisor still running is killed first. If every
   * tree is confirmed gone, the job is recorded UNKNOWN (ORPHANS_TERMINATED) with
   * the receipts; otherwise nothing is recorded. Resolves to { confirmed, receipts, job }.
   */
  async terminateOrphans({ jobId, expectedSequence, explicitAuthorization = false, graceMs = 5_000, killWaitMs = 5_000 } = {}) {
    requireJobId(jobId);
    if (explicitAuthorization !== true) throw jobError('JOB_AUTHORIZATION_REQUIRED', 'Explicit authorization is required to stop orphaned processes');
    if (!Number.isSafeInteger(expectedSequence) || expectedSequence < 1) throw new TypeError('expectedSequence (from the inspected state) is required');
    for (const [value, label] of [[graceMs, 'graceMs'], [killWaitMs, 'killWaitMs']]) {
      if (!Number.isInteger(value) || value < 0 || value > MAX_TERMINATION_WAIT_MS) {
        throw new TypeError(`${label} must be an integer between 0 and ${MAX_TERMINATION_WAIT_MS}`);
      }
    }
    if (this.#live.has(jobId)) throw jobError('JOB_RUNNING_HERE', 'The job is executing in this registry', { jobId });
    if (process.platform !== 'linux') {
      throw jobError('JOB_ORPHANS_UNSUPPORTED', 'Orphaned processes can be found and verified only on Linux; confirm they have ' +
        'stopped and reconcile with overrideRunningOwner: true', { jobId });
    }
    const record = await this.#store.read(jobId);
    if (!record) throw jobError('JOB_NOT_FOUND', 'No record exists for this job', { jobId });
    if (record.sequence !== expectedSequence) {
      throw jobError('JOB_RECORD_CHANGED', 'The job changed since it was inspected; inspect it again',
        { jobId, expectedSequence, currentSequence: record.sequence });
    }
    if (record.status !== 'RUNNING' || !(record.processes?.length > 0)) {
      throw jobError('JOB_NO_ORPHANS', 'Only a RUNNING job with recorded processes can have orphaned processes', { jobId });
    }
    const owner = ownerState(record.owner);
    if (owner === 'ALIVE') throw jobError('JOB_NOT_OWNED', 'The job\'s owner is running; cancel the job there', { jobId });
    if (owner !== 'DEAD') throw jobError('JOB_OWNER_UNVERIFIED', 'The job\'s owner cannot be verified', { jobId, ownerState: owner });

    const receipts = [];
    for (const tree of record.processes) {
      // Its owner is gone: a supervisor still running is cleaning up or stuck. Take over.
      if (identityState(tree.supervisor) === 'ALIVE') { try { process.kill(tree.supervisor.pid, 'SIGKILL'); } catch { /* exited */ } }
      const pgid = tree.leader && (identityState(tree.leader) === 'ALIVE' || groupCarriesMarker(tree.leader.pid, tree.marker))
        ? tree.leader.pid : null;
      const result = await stopTree(pgid, tree.marker, [], graceMs, killWaitMs);
      receipts.push({ marker: tree.marker, pgid, confirmed: result.confirmed, signals: result.signals, frozen: result.frozen,
        stopped: result.processes.map(({ pid }) => pid) });
    }
    const confirmed = receipts.every(receipt => receipt.confirmed);
    if (!confirmed) return { confirmed, receipts, job: await this.get(jobId) };
    const updated = await this.#store.update(jobId, { expectedSequence, changes: {
      status: 'UNKNOWN', reconciliationRequired: true, reason: 'ORPHANS_TERMINATED',
      orphanTermination: { at: this.#now(), receipts } } });
    return { confirmed, receipts, job: snapshotOf(updated) };
  }

  inspectLock() { return this.#store.inspectLock(); }
  forceUnlock(request) { return this.#store.forceUnlock(request); }

  #sameIdentity(existing, objective, fingerprint, resumable) {
    const identity = existing.record ?? existing;
    if (identity.objective !== objective || identity.fingerprint !== fingerprint || (identity.resumable === true) !== resumable) {
      throw jobError('JOB_IDENTITY_CONFLICT', 'Job identity cannot be reused with different input', { jobId: identity.jobId });
    }
    return existing.record ? this.#snapshot(existing) : this.#resolve(existing);
  }

  /**
   * The view of a stored record not executing in this instance. A RUNNING job
   * whose owner is provably gone becomes CANCELLED (never dispatched: the
   * executor was not called) or UNKNOWN (dispatched: the effect is ambiguous),
   * recorded by compare-and-swap so that every process agrees, and a later PID
   * reuse cannot make it look alive again. If it cannot be recorded now, the job
   * is reported UNKNOWN (OWNER_DIED_NOT_RECORDED) and the next read tries again.
   */
  async #resolve(record, attempts = 3) {
    if (record.status !== 'RUNNING') return snapshotOf(record);
    const state = ownerState(record.owner);
    if (state !== 'DEAD') return { ...snapshotOf(record), ownerState: state };
    // Its owner is gone but its processes may run on: the work is still RUNNING.
    const processes = processesState(record);
    if (processes !== 'NONE') return { ...snapshotOf(record), ownerState: 'DEAD', orphaned: true, processesState: processes };
    const changes = record.dispatched ? OWNER_DIED_AFTER_DISPATCH : OWNER_DIED_BEFORE_DISPATCH;
    try {
      return snapshotOf(await this.#store.update(record.jobId, { expectedSequence: record.sequence, changes: { ...changes } }));
    } catch (error) {
      if (error.code === 'JOB_RECORD_CHANGED' && attempts > 1) {
        // Another process recorded it first (or the record moved on): resolve what is stored now.
        const current = await this.#store.read(record.jobId);
        if (current) return this.#resolve(current, attempts - 1);
      }
      if (error.code === 'JOB_STATE_INVALID') throw error;
      return { ...snapshotOf(record), ...OWNER_DIED_AFTER_DISPATCH, reason: 'OWNER_DIED_NOT_RECORDED', ownerState: 'DEAD' };
    }
  }

  #now() {
    return this.clock().toISOString();
  }

  // Runs the job once. Every outcome, including failures to record it, ends here.
  async #run(entry) {
    await null; // let start() return first
    const { signal } = entry.controller;
    let decided = null;
    let stored = { result: null, resultDiscarded: false };
    if (signal.aborted) decided = CANCELLED_BEFORE_DISPATCH;
    else {
      try { await this.#write(entry, () => ({ dispatched: true, dispatchedAt: this.#now() })); }
      catch { decided = DISPATCH_NOT_RECORDED; }
      if (!decided && signal.aborted) decided = CANCELLED_BEFORE_DISPATCH;
    }
    if (!decided) {
      try {
        const value = await this.execute({
          jobId: entry.jobId, objective: entry.objective, input: structuredClone(entry.input), signal,
          setPhase: phase => this.#setPhase(entry, phase),
          runProcess: options => this.#runProcess(entry, options),
          saveProgress: state => this.#saveProgress(entry, state),
          effectKey: name => this.#effectKey(entry, name),
          resume: entry.resume ? structuredClone(entry.resume) : null
        });
        decided = settlementOutcome(value, signal.aborted);
        stored = boundResult(value);
      } catch {
        // Raw executor errors may contain credentials; never retain them here.
        decided = thrownOutcome(signal.aborted);
        stored = { result: null, resultDiscarded: false };
      }
    }
    entry.decided = { ...decided, ...stored };
    entry.state = 'recording';
    await this.#recordOutcome(entry);
  }

  /**
   * Run a command as a supervised process tree for this job (see runSupervised):
   * aborted with the job, and recorded in the job's record before it starts (the
   * tree's environment marker and its supervisor) and once it runs (its leader),
   * so that a restart can find it. If the record cannot be written first, the
   * command never starts and the result says so (effectAbsent: true).
   */
  #runProcess(entry, { command, args, cwd, env, stdio, graceMs, killWaitMs, trackIntervalMs } = {}) {
    const identityOf = ({ pid, host, bootId, startTicks }) => (bootId === undefined ? { pid, host } : { pid, host, bootId, startTicks });
    return runSupervised({ command, args, cwd, env, stdio, graceMs, killWaitMs, trackIntervalMs, signal: entry.controller.signal,
      beforeStart: async ({ supervisor, marker }) => {
        if (entry.state !== 'running') throw new Error('The job is no longer running');
        if (!supervisor) throw new Error('The process supervisor is not running');
        await this.#write(entry, () => ({ processes: [...(entry.record.processes ?? []),
          { marker, supervisor: identityOf(supervisor), leader: null, recordedAt: this.#now() }] }));
      },
      onStart: ({ pid, identity, marker }) => this.#write(entry, () => ({
        processes: (entry.record.processes ?? []).map(recorded => recorded.marker === marker
          ? { ...recorded, leader: identityOf(identity ?? { pid, host: hostname() }) } : recorded)
      }))
    });
  }

  /**
   * Durably record the job's progress (plain JSON of at most 64 KiB) with the
   * current phase, for a later resume. Resolves to the progress sequence once it
   * is written. Only for jobs started with resumable: true, while they run.
   */
  async #saveProgress(entry, state) {
    if (entry.record.resumable !== true) throw new TypeError('saveProgress requires a job started with resumable: true');
    if (entry.state !== 'running') throw new Error('The job is no longer running');
    let copy;
    try { copy = jsonInput(state ?? null); }
    catch { throw new TypeError('progress must be plain JSON'); }
    if (Buffer.byteLength(JSON.stringify(copy)) > 64 * 1024) throw new TypeError('progress exceeds 64 KiB');
    let sequence;
    await this.#write(entry, () => {
      sequence = (entry.record.progress?.sequence ?? 0) + 1;
      return { progress: { sequence, phase: entry.phase, state: copy, savedAt: this.#now() } };
    });
    return sequence;
  }

  // The same in every attempt of a job, for effects that must happen once
  // however often the job is resumed (see FileIdempotencyRegistry).
  #effectKey(entry, name) {
    if (typeof name !== 'string' || !name.trim() || name.length > 200) throw new TypeError('effectKey needs a name of 1 to 200 characters');
    return `${entry.record.resumeOf?.rootJobId ?? entry.jobId}:${name}`;
  }

  #setPhase(entry, phase) {
    if (typeof phase !== 'string' || !VALID_PHASE.test(phase)) throw new TypeError('Invalid job phase');
    if (entry.state !== 'running') return false;
    entry.phase = phase;
    // Coalesced: a queued write records whatever phase is current when it runs.
    if (!entry.phaseQueued) {
      entry.phaseQueued = true;
      this.#write(entry, () => {
        entry.phaseQueued = false;
        return entry.state === 'running' && entry.phase !== entry.record.phase ? { phase: entry.phase } : null;
      }).catch(() => { /* the outcome write records the final phase */ });
    }
    return true;
  }

  #outcomeChanges(entry) {
    const { status, reason, cancellationConfirmed, effectAbsent, reconciliationRequired, result, resultDiscarded } = entry.decided;
    const changes = { status, phase: entry.phase, reason, cancellationConfirmed, effectAbsent, reconciliationRequired,
      result, resultDiscarded };
    if (entry.cancellationRequestedAt && !entry.record.cancellationRequested) {
      Object.assign(changes, { cancellationRequested: true, cancellationRequestedAt: entry.cancellationRequestedAt });
    }
    return changes;
  }

  async #recordOutcome(entry, attempt = 0) {
    try {
      await this.#write(entry, () => this.#outcomeChanges(entry));
      entry.state = 'recorded';
      this.#live.delete(entry.jobId);
      liveExecutions.delete(entry.record.owner.executionId);
    } catch (error) {
      // Until the outcome is recorded this process reports UNKNOWN. Transient
      // failures are retried in the background; after a restart, the stored
      // record is what counts.
      entry.state = 'unrecorded';
      if (PERMANENT.has(error.code)) {
        // This execution will not write again: let other instances judge the
        // record as they would after this process exits.
        liveExecutions.delete(entry.record.owner.executionId);
        if (error.code === 'JOB_RECORD_CHANGED') {
          // Someone else decided the outcome (an operator reconciliation): their
          // record stands and this execution's late outcome is not recorded.
          entry.state = 'superseded';
          this.#live.delete(entry.jobId);
        }
      } else {
        const timer = setTimeout(() => this.#recordOutcome(entry, attempt + 1), Math.min(25 * 2 ** attempt, 2_000));
        timer.unref?.();
      }
    }
  }

  // Writes of one job run in order, each compare-and-swap on the last sequence.
  #write(entry, build) {
    const run = entry.queue.then(async () => {
      const changes = build();
      if (changes === null) return entry.record;
      try {
        entry.record = await this.#store.update(entry.jobId, { expectedSequence: entry.record.sequence, changes });
      } catch (error) {
        if (error.code !== 'JOB_RECORD_CHANGED') throw error;
        // A write of ours may have been stored even though it reported an error.
        // While the record is still RUNNING under this execution, no one else has
        // decided its outcome, so continue from the stored record.
        const current = await this.#store.read(entry.jobId);
        if (current?.status !== 'RUNNING' || current.owner.executionId !== entry.record.owner.executionId) throw error;
        entry.record = current;
        entry.record = await this.#store.update(entry.jobId, { expectedSequence: current.sequence, changes: build() ?? {} });
      }
      return entry.record;
    });
    entry.queue = run.catch(() => {});
    return run;
  }

  #snapshot(entry) {
    const view = snapshotOf(entry.record);
    if (entry.state === 'recorded') return view;
    if (entry.state === 'unrecorded') {
      // The outcome is known here but not recorded: report it as uncertain.
      return { ...view, status: 'UNKNOWN', phase: entry.phase, reconciliationRequired: true, cancellationConfirmed: false,
        effectAbsent: false, reason: 'OUTCOME_NOT_RECORDED', result: null, resultDiscarded: false,
        cancellationRequested: view.cancellationRequested || entry.cancellationRequested };
    }
    // Running (or recording its outcome): the phase and request may be ahead of the record.
    return { ...view, status: 'RUNNING', phase: entry.phase,
      cancellationRequested: view.cancellationRequested || entry.cancellationRequested };
  }
}
