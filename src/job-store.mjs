import { readdir } from 'node:fs/promises';
import path from 'node:path';
import { DirectoryLock, readBoundedFile, replaceFile } from './directory-lock.mjs';
import { appendJobEvent, jobEvent, jobFileStem, readJobHistory, searchJobHistory } from './job-history.mjs';
import { validIdentity } from './process-identity.mjs';
import {
  MAX_EVIDENCE_BYTES, RECONCILED_STATUSES, VALID_JOB_ID, VALID_PHASE, jobError, jsonInput, requireJobId
} from './phase-job-registry.mjs';

// Internal module: the durable record behind the file-backed job registry.
// Not exported from index.mjs.

export const JOB_RECORD_SCHEMA = 'mnemos.phase-job-record.v1';
export const MAX_RESULT_BYTES = 64 * 1024;
const MAX_RECORD_BYTES = 128 * 1024;
const MAX_OBJECTIVE_LENGTH = 4000;
const STATUSES = new Set(['RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', 'UNKNOWN']);
const TERMINAL = new Set(['COMPLETED', 'FAILED', 'CANCELLED']);
const FINGERPRINT = /^[0-9a-f]{64}$/;
const REASON = /^[A-Z][A-Z0-9_]{0,63}$/;
const RECORD_FILE = /^[0-9a-f]{64}\.job\.json$/;
const JOB_TEMPORARY = /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}\.job\.tmp$/;
// Fields a caller may change through update(). Identity (jobId, objective,
// fingerprint, owner, createdAt) is fixed at creation; sequence and updatedAt
// are maintained by the store.
const MUTABLE = new Set(['status', 'phase', 'dispatched', 'dispatchedAt', 'cancellationRequested',
  'cancellationRequestedAt', 'cancellationConfirmed', 'reconciliationRequired', 'effectAbsent',
  'reason', 'result', 'resultDiscarded', 'reconciliation', 'processes', 'orphanTermination', 'progress', 'resumedBy']);
const MARKER = /^MNEMOS_TREE_[0-9a-f]{32}$/;
const MAX_PROCESSES = 64;
// resumable and resumeOf are absent in older records and in jobs that are not resumable.
const IDENTITY = ['jobId', 'objective', 'fingerprint', 'createdAt', 'owner', 'resumable', 'resumeOf'];
const MAX_ATTEMPTS = 1000;

const invalidRecord = (message, details) => jobError('JOB_RECORD_INVALID', message, details);
const isTimestamp = value => typeof value === 'string' && Number.isFinite(Date.parse(value));
const isBoolean = value => value === true || value === false;
const fileName = jobId => `${jobFileStem(jobId)}.job.json`;

// threadId is absent in records of earlier versions.
function validOwner(owner) {
  return validIdentity(owner) &&
    typeof owner.executionId === 'string' && owner.executionId.length > 0 && owner.executionId.length <= 128 &&
    (owner.threadId === undefined || (Number.isSafeInteger(owner.threadId) && owner.threadId >= 0));
}

// Process trees a job started: [{ marker, supervisor, leader, recordedAt }].
// Absent in older records. leader is null until the command has started.
function processesProblem(processes) {
  if (processes === undefined) return null;
  if (!Array.isArray(processes) || processes.length > MAX_PROCESSES) return 'processes';
  const markers = new Set();
  for (const entry of processes) {
    if (typeof entry?.marker !== 'string' || !MARKER.test(entry.marker) || markers.has(entry.marker) ||
      !validIdentity(entry.supervisor) || !(entry.leader === null || validIdentity(entry.leader)) ||
      !isTimestamp(entry.recordedAt)) return 'processes';
    markers.add(entry.marker);
  }
  return null;
}

// Audit of an operator reconciliation. Absent (older records) or null when none.
function reconciliationProblem(record) {
  const audit = record.reconciliation;
  if (audit === undefined || audit === null) return record.reason === 'RECONCILED' ? 'reconciliation' : null;
  if (typeof audit !== 'object' || Array.isArray(audit)) return 'reconciliation';
  if (!isTimestamp(audit.at) || !RECONCILED_STATUSES.includes(audit.disposition)) return 'reconciliation';
  if (audit.disposition !== record.status || record.reason !== 'RECONCILED') return 'reconciliation';
  if (audit.evidence?.authoritative !== true || typeof audit.evidence.source !== 'string') return 'reconciliation';
  if (!['RUNNING', 'UNKNOWN'].includes(audit.previous?.status)) return 'reconciliation';
  if (audit.override !== null && !(validOwner(audit.override?.owner) && typeof audit.override.ownerState === 'string')) {
    return 'reconciliation';
  }
  try {
    if (Buffer.byteLength(JSON.stringify(jsonInput(audit.evidence))) > MAX_EVIDENCE_BYTES) return 'reconciliation';
  } catch { return 'reconciliation'; }
  return null;
}

// Audit of terminateOrphans(): { at, receipts: [{ marker, confirmed, signals, frozen, stopped }] }.
// Reason ORPHANS_TERMINATED requires it; it stays when the job is reconciled later.
function orphanTerminationProblem(record) {
  const audit = record.orphanTermination;
  if (audit === undefined || audit === null) return record.reason === 'ORPHANS_TERMINATED' ? 'orphanTermination' : null;
  if (record.status === 'RUNNING') return 'orphanTermination';
  if (!isTimestamp(audit.at) || !Array.isArray(audit.receipts) || audit.receipts.length === 0 ||
    audit.receipts.length > MAX_PROCESSES) return 'orphanTermination';
  for (const receipt of audit.receipts) {
    if (typeof receipt?.marker !== 'string' || !MARKER.test(receipt.marker) || receipt.confirmed !== true ||
      !Array.isArray(receipt.signals) || !receipt.signals.every(signal => typeof signal === 'string') ||
      !isBoolean(receipt.frozen) || !Array.isArray(receipt.stopped) || receipt.stopped.length > 4096 ||
      !receipt.stopped.every(pid => Number.isSafeInteger(pid) && pid > 0)) return 'orphanTermination';
  }
  return null;
}

/**
 * Resumption (a job started with resumable: true):
 * - progress: { sequence, phase, state, savedAt }, saved by the executor while
 *   RUNNING; state is plain JSON of at most 64 KiB.
 * - resumeOf: { jobId, rootJobId, attempt }, the attempt this one continues.
 * - resumedBy: the job ID of the attempt that continues this one; with reason
 *   RESUMED for a job that was UNKNOWN.
 */
function resumptionProblem(record) {
  const resumable = record.resumable === true;
  if (record.resumable !== undefined && record.resumable !== true) return 'resumable';
  const { progress, resumeOf, resumedBy } = record;
  if (progress !== undefined) {
    if (!resumable || !Number.isSafeInteger(progress?.sequence) || progress.sequence < 1 ||
      typeof progress.phase !== 'string' || !VALID_PHASE.test(progress.phase) || !isTimestamp(progress.savedAt)) return 'progress';
    try { if (resultBytes(jsonInput(progress.state)) > MAX_RESULT_BYTES) return 'progress'; }
    catch { return 'progress'; }
  }
  if (resumeOf !== undefined && (!resumable || typeof resumeOf?.jobId !== 'string' || !VALID_JOB_ID.test(resumeOf.jobId) ||
    typeof resumeOf.rootJobId !== 'string' || !VALID_JOB_ID.test(resumeOf.rootJobId) || resumeOf.jobId === record.jobId ||
    !Number.isSafeInteger(resumeOf.attempt) || resumeOf.attempt < 2 || resumeOf.attempt > MAX_ATTEMPTS)) return 'resumeOf';
  if (resumedBy !== undefined && (!resumable || typeof resumedBy !== 'string' || !VALID_JOB_ID.test(resumedBy) ||
    resumedBy === record.jobId || !['CANCELLED', 'FAILED'].includes(record.status))) return 'resumedBy';
  if ((record.reason === 'RESUMED') !== (resumedBy !== undefined && record.status === 'CANCELLED' && record.reason === 'RESUMED')) {
    return 'resumedBy';
  }
  return null;
}

function resultBytes(result) {
  return Buffer.byteLength(JSON.stringify(result));
}

/**
 * A storable copy of an executor result: plain JSON of at most 64 KiB. Anything
 * else is discarded. The result never decides the job's status.
 */
export function boundResult(value) {
  try {
    const result = jsonInput(value ?? null);
    if (resultBytes(result) <= MAX_RESULT_BYTES) return { result, resultDiscarded: false };
  } catch { /* not plain JSON */ }
  return { result: null, resultDiscarded: true };
}

/** The violated rule for a complete record, or null when it is consistent. */
function recordProblem(record) {
  if (typeof record?.jobId !== 'string' || !VALID_JOB_ID.test(record.jobId)) return 'jobId';
  if (typeof record.objective !== 'string' || !record.objective.trim() || record.objective.length > MAX_OBJECTIVE_LENGTH) return 'objective';
  if (typeof record.fingerprint !== 'string' || !FINGERPRINT.test(record.fingerprint)) return 'fingerprint';
  if (!validOwner(record.owner)) return 'owner';
  if (!Number.isSafeInteger(record.sequence) || record.sequence < 1) return 'sequence';
  if (!isTimestamp(record.createdAt) || !isTimestamp(record.updatedAt)) return 'timestamps';
  if (!STATUSES.has(record.status)) return 'status';
  if (typeof record.phase !== 'string' || !VALID_PHASE.test(record.phase)) return 'phase';
  for (const field of ['dispatched', 'cancellationRequested', 'cancellationConfirmed',
    'reconciliationRequired', 'effectAbsent', 'resultDiscarded']) {
    if (!isBoolean(record[field])) return field;
  }
  if (record.dispatched ? !isTimestamp(record.dispatchedAt) : record.dispatchedAt !== null) return 'dispatchedAt';
  if (record.cancellationRequested ? !isTimestamp(record.cancellationRequestedAt) : record.cancellationRequestedAt !== null) {
    return 'cancellationRequestedAt';
  }
  if (record.reason !== null && !(typeof record.reason === 'string' && REASON.test(record.reason))) return 'reason';
  // Only UNKNOWN requires reconciliation, and it always does.
  if (record.reconciliationRequired !== (record.status === 'UNKNOWN')) return 'reconciliationRequired';
  if (record.cancellationConfirmed && record.status !== 'CANCELLED') return 'cancellationConfirmed';
  // A retry is allowed only on explicit evidence that nothing happened.
  if (record.effectAbsent && !['FAILED', 'CANCELLED'].includes(record.status)) return 'effectAbsent';
  try { if (resultBytes(jsonInput(record.result)) > MAX_RESULT_BYTES) return 'result'; }
  catch { return 'result'; } // not plain JSON
  if (record.resultDiscarded && record.result !== null) return 'resultDiscarded';
  if (record.status === 'RUNNING' && (record.result !== null || record.resultDiscarded || record.reason !== null)) {
    return 'status';
  }
  return processesProblem(record.processes) ?? orphanTerminationProblem(record) ?? resumptionProblem(record) ??
    reconciliationProblem(record);
}

/** The violated rule for a transition from previous to next, or null when it is allowed. */
function transitionProblem(previous, next) {
  for (const field of IDENTITY) {
    if (JSON.stringify(previous[field]) !== JSON.stringify(next[field])) return `${field} is immutable`;
  }
  if (TERMINAL.has(previous.status)) {
    // The one exception: a CANCELLED or FAILED job may be linked, once, to the attempt that resumes it.
    const { resumedBy, sequence, updatedAt, ...rest } = next;
    const { resumedBy: before, sequence: _sequence, updatedAt: _updatedAt, ...previousRest } = previous;
    const linkOnly = previous.status !== 'COMPLETED' && before === undefined && resumedBy !== undefined &&
      JSON.stringify(Object.keys(rest).sort().map(key => [key, rest[key]])) ===
      JSON.stringify(Object.keys(previousRest).sort().map(key => [key, previousRest[key]]));
    return linkOnly ? null : `${previous.status} is final`;
  }
  // UNKNOWN leaves only through reconciliation, to a definite outcome.
  if (previous.status === 'UNKNOWN' && !TERMINAL.has(next.status)) return 'UNKNOWN may only become a definite outcome';
  if (previous.status !== 'RUNNING' && next.phase !== previous.phase) return 'phase changes only while RUNNING';
  // The executor only runs after dispatch is recorded, so nothing undispatched can complete.
  if (next.status === 'COMPLETED' && !next.dispatched) return 'a job that was never dispatched cannot complete';
  if (previous.dispatched && (!next.dispatched || next.dispatchedAt !== previous.dispatchedAt)) return 'dispatch cannot be undone';
  if (previous.cancellationRequested && (!next.cancellationRequested ||
    next.cancellationRequestedAt !== previous.cancellationRequestedAt)) return 'a cancellation request cannot be withdrawn';
  // Progress is saved while RUNNING, one step at a time, and never removed.
  if (JSON.stringify(next.progress) !== JSON.stringify(previous.progress)) {
    if (previous.status !== 'RUNNING') return 'progress is saved only while RUNNING';
    if (next.progress === undefined || next.progress?.sequence !== (previous.progress?.sequence ?? 0) + 1) {
      return 'progress advances one step at a time';
    }
  }
  // A job is resumed once; resumedBy never changes.
  if (previous.resumedBy !== undefined && next.resumedBy !== previous.resumedBy) return 'resumedBy cannot change';
  if (previous.resumedBy === undefined && next.resumedBy !== undefined && previous.status === 'RUNNING') {
    return 'a running job cannot be resumed';
  }
  // Process records are append-only while the job runs; a leader is set once.
  const before = previous.processes ?? [];
  const after = Array.isArray(next.processes) ? next.processes : []; // malformed: refused by recordProblem
  if (after.length < before.length) return 'process records cannot be removed';
  for (let index = 0; index < after.length; index++) {
    const old = before[index];
    if (!old) {
      if (previous.status !== 'RUNNING') return 'processes are recorded only while RUNNING';
      continue;
    }
    const entry = after[index];
    if (entry?.marker !== old.marker || JSON.stringify(entry?.supervisor) !== JSON.stringify(old.supervisor) ||
      entry?.recordedAt !== old.recordedAt) return 'process records cannot be changed';
    if (JSON.stringify(entry?.leader) !== JSON.stringify(old.leader)) {
      if (old.leader !== null) return 'a recorded leader cannot be changed';
      if (previous.status !== 'RUNNING') return 'processes are recorded only while RUNNING';
    }
  }
  // The orphan audit is set once, when a RUNNING job becomes UNKNOWN (ORPHANS_TERMINATED), and never changed.
  if (JSON.stringify(next.orphanTermination ?? null) !== JSON.stringify(previous.orphanTermination ?? null) &&
    !(previous.status === 'RUNNING' && next.status === 'UNKNOWN' && next.reason === 'ORPHANS_TERMINATED')) {
    return 'the orphan audit is set only when orphans are terminated';
  }
  const audited = next.reconciliation !== undefined && next.reconciliation !== null;
  if (audited && next.reconciliation?.previous?.status !== previous.status) return 'the reconciliation must name the state it replaces';
  return null;
}

/**
 * Durable job records, one file per job, shared by processes on one host.
 * - Every write runs under a directory lock (jobs.lock) and replaces the file
 *   atomically; the file and the directory entry are flushed before it resolves.
 * - Updates are compare-and-swap on the record's sequence and follow the job
 *   state machine; identity fields never change and final states are final.
 * - An unreadable or inconsistent record is an error (JOB_STATE_INVALID), never
 *   treated as absent: that would let the job start again.
 */
export class FileJobStore {
  #lock;

  constructor({ directory, clock = () => new Date(), lockTimeoutMs = 5_000 } = {}) {
    if (typeof directory !== 'string' || !directory.trim()) throw new TypeError('A job state directory is required');
    if (typeof clock !== 'function') throw new TypeError('clock must be a function');
    this.directory = path.resolve(directory);
    this.clock = clock;
    this.#lock = new DirectoryLock({ directory: this.directory, name: 'jobs.lock', timeoutMs: lockTimeoutMs,
      timeoutCode: 'JOB_LOCK_TIMEOUT', ownedTemporary: name => JOB_TEMPORARY.test(name) });
    this.lockFile = this.#lock.lockFile;
  }

  /** The stored record, or null when none exists. Never takes the lock. */
  async read(jobId) {
    const record = await this.#readFile(path.join(this.directory, fileName(requireJobId(jobId))));
    if (record && record.jobId !== jobId) {
      throw jobError('JOB_STATE_INVALID', 'Job record belongs to another job; reconciliation required', { jobId });
    }
    return record;
  }

  /**
   * Create the record of a new RUNNING job in phase "queued", not yet dispatched.
   * Resolves to { created: true, record }, or { created: false, record } with
   * the existing record (whatever its identity) when the job ID is taken.
   */
  async create({ jobId, objective, fingerprint, owner, resumable = false } = {}) {
    const record = this.#newRecord({ jobId, objective, fingerprint, owner, resumable });
    return this.#transaction(async () => {
      const existing = await this.read(jobId);
      if (existing) return { created: false, record: existing };
      await this.#write(record, null);
      return { created: true, record: structuredClone(record) };
    });
  }

  /**
   * Resume a job under one lock: link (and, if UNKNOWN, close) the predecessor at
   * expectedSequence with `changes`, then create the successor RUNNING, continuing
   * from the predecessor's progress. If the predecessor is already linked to this
   * successor (a crash came between the two writes), only the successor is
   * created; if the successor exists and continues this predecessor, nothing is
   * written. Resolves to { predecessor, successor, created }.
   */
  async resume({ jobId, expectedSequence, changes, successor } = {}) {
    requireJobId(jobId);
    if (!Number.isSafeInteger(expectedSequence) || expectedSequence < 1) throw new TypeError('Invalid expectedSequence');
    return this.#transaction(async () => {
      const current = await this.read(jobId);
      if (!current) throw jobError('JOB_NOT_FOUND', 'No record exists for this job', { jobId });
      if (current.sequence !== expectedSequence) {
        throw jobError('JOB_RECORD_CHANGED', 'The job record changed since it was read; read it again',
          { jobId, expectedSequence, currentSequence: current.sequence });
      }
      let predecessor = current;
      if (current.resumedBy === undefined) {
        predecessor = { ...current, ...structuredClone(changes), resumedBy: successor.jobId, sequence: current.sequence + 1, updatedAt: this.#now() };
        const refusal = transitionProblem(current, predecessor);
        if (refusal) throw jobError('JOB_TRANSITION_INVALID', `Invalid job transition: ${refusal}`, { jobId, from: current.status });
        const problem = recordProblem(predecessor);
        if (problem) throw invalidRecord(`Invalid job record: ${problem}`, { jobId, field: problem });
      } else if (current.resumedBy !== successor.jobId) {
        throw jobError('JOB_ALREADY_RESUMED', 'The job was already resumed by another attempt', { jobId, resumedBy: current.resumedBy });
      }
      const existing = await this.read(successor.jobId);
      if (existing) {
        if (existing.resumeOf?.jobId !== jobId) {
          throw jobError('JOB_IDENTITY_CONFLICT', 'The successor job ID is already taken', { jobId: successor.jobId });
        }
        return { predecessor: structuredClone(current), successor: existing, created: false };
      }
      const record = this.#newRecord({ ...successor, objective: current.objective, fingerprint: current.fingerprint, resumable: true,
        resumeOf: { jobId, rootJobId: current.resumeOf?.rootJobId ?? jobId, attempt: (current.resumeOf?.attempt ?? 1) + 1 },
        progress: current.progress });
      if (predecessor !== current) await this.#write(predecessor, current);
      await this.#write(record, null);
      return { predecessor: structuredClone(predecessor), successor: structuredClone(record), created: true };
    });
  }

  #newRecord({ jobId, objective, fingerprint, owner, resumable = false, resumeOf, progress }) {
    requireJobId(jobId);
    if (resumable !== true && resumable !== false) throw invalidRecord('resumable must be a boolean', { jobId, field: 'resumable' });
    const now = this.#now();
    const record = {
      jobId, objective, fingerprint, owner: structuredClone(owner), createdAt: now, updatedAt: now, sequence: 1,
      status: 'RUNNING', phase: 'queued', dispatched: false, dispatchedAt: null,
      cancellationRequested: false, cancellationRequestedAt: null, cancellationConfirmed: false,
      reconciliationRequired: false, effectAbsent: false, reason: null, result: null, resultDiscarded: false,
      ...(resumable ? { resumable: true } : {}),
      ...(resumeOf ? { resumeOf: structuredClone(resumeOf) } : {}),
      ...(progress ? { progress: structuredClone(progress) } : {})
    };
    const problem = recordProblem(record);
    if (problem) throw invalidRecord(`Invalid job record: ${problem}`, { jobId, field: problem });
    return record;
  }

  /**
   * Apply changes to the record if it is still at expectedSequence. Resolves to
   * the new record. Fails with JOB_NOT_FOUND, JOB_RECORD_CHANGED (another write
   * came first; nothing is written), JOB_TRANSITION_INVALID or JOB_RECORD_INVALID.
   */
  async update(jobId, { expectedSequence, changes } = {}) {
    requireJobId(jobId);
    if (!Number.isSafeInteger(expectedSequence) || expectedSequence < 1) throw new TypeError('Invalid expectedSequence');
    if (changes === null || typeof changes !== 'object' || Array.isArray(changes)) throw new TypeError('changes must be an object');
    for (const field of Object.keys(changes)) {
      if (!MUTABLE.has(field)) throw invalidRecord(`Field cannot be changed: ${field}`, { jobId, field });
    }
    let patch;
    let field = null;
    try {
      patch = structuredClone(changes);
      // Stored canonically.
      field = 'result';
      if ('result' in patch) patch.result = jsonInput(patch.result);
      field = 'progress';
      if (patch.progress !== undefined) patch.progress = jsonInput(patch.progress);
    } catch {
      throw invalidRecord('Job record changes must be plain JSON; use boundResult() for results', { jobId, field: field ?? 'changes' });
    }
    return this.#transaction(async () => {
      const current = await this.read(jobId);
      if (!current) throw jobError('JOB_NOT_FOUND', 'No record exists for this job', { jobId });
      if (current.sequence !== expectedSequence) {
        throw jobError('JOB_RECORD_CHANGED', 'The job record changed since it was read; read it again',
          { jobId, expectedSequence, currentSequence: current.sequence });
      }
      const next = { ...current, ...patch, sequence: current.sequence + 1, updatedAt: this.#now() };
      // Transition rules first: a final state is reported as final, whatever the change.
      const refusal = transitionProblem(current, next);
      if (refusal) {
        throw jobError('JOB_TRANSITION_INVALID', `Invalid job transition: ${refusal}`,
          { jobId, from: current.status, to: next.status });
      }
      const problem = recordProblem(next);
      if (problem) throw invalidRecord(`Invalid job record: ${problem}`, { jobId, field: problem });
      await this.#write(next, current);
      return structuredClone(next);
    });
  }

  /**
   * Every record in the directory. Records that cannot be read are listed in
   * `invalid` by file name; callers must treat those jobs as UNKNOWN.
   */
  async list() {
    let names = [];
    try { names = await readdir(this.directory); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    const records = [];
    const invalid = [];
    for (const name of names.filter(entry => RECORD_FILE.test(entry)).sort()) {
      try {
        const record = await this.#readFile(path.join(this.directory, name));
        if (!record) continue; // removed after readdir
        if (fileName(record.jobId) !== name) throw jobError('JOB_STATE_INVALID', 'Job record is stored under another job ID');
        records.push(record);
      } catch (error) {
        invalid.push({ file: name, code: error.code ?? 'JOB_STATE_INVALID' });
      }
    }
    records.sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.jobId.localeCompare(b.jobId));
    return { records, invalid };
  }

  /**
   * One job's event history (see job-history.mjs): { jobId, events, complete,
   * missing, unreadableLines }, or null when no record exists. Never takes the lock.
   */
  async history(jobId) {
    const record = await this.read(jobId);
    return record ? readJobHistory(this.directory, jobId, record.sequence) : null;
  }

  /** Events of all jobs matching the filters (see job-history.mjs). Never takes the lock. */
  searchHistory(filters) {
    return searchJobHistory(this.directory, filters);
  }

  /** Read-only view of the store lock for operators; never takes the lock. */
  inspectLock() { return this.#lock.inspect(); }

  /** Operator override; see the README runbook. */
  forceUnlock(request) { return this.#lock.forceUnlock(request); }

  #now() {
    const now = this.clock();
    if (!(now instanceof Date) || !Number.isFinite(now.getTime())) throw new TypeError('clock must return a valid Date');
    return now.toISOString();
  }

  // The lock creates the directory and flushes its entry into the parent.
  #transaction(operation) {
    return this.#lock.run(operation);
  }

  async #readFile(file) {
    const bytes = await readBoundedFile(file, MAX_RECORD_BYTES,
      () => jobError('JOB_STATE_INVALID', 'Job record is not a file of at most 128 KiB', { file }));
    if (bytes === null) return null;
    const text = bytes.toString('utf8');
    let stored;
    try { stored = JSON.parse(text); }
    catch { throw jobError('JOB_STATE_INVALID', 'Job record is not valid JSON; reconciliation required', { file }); }
    const problem = stored?.schema === JOB_RECORD_SCHEMA ? recordProblem(stored.record) : 'schema';
    if (problem) {
      throw jobError('JOB_STATE_INVALID', `Job record is invalid (${problem}); reconciliation required`, { file, field: problem });
    }
    return stored.record;
  }

  // Write the record, then append its event to the job's history, both under the
  // caller's lock. previous is null for a new record. A failed append never fails
  // the write: it shows as a gap in the history.
  async #write(record, previous) {
    const text = JSON.stringify({ schema: JOB_RECORD_SCHEMA, record }) + '\n';
    if (Buffer.byteLength(text) > MAX_RECORD_BYTES) throw invalidRecord('Job record exceeds 128 KiB', { jobId: record.jobId });
    await replaceFile(path.join(this.directory, fileName(record.jobId)), text, '.job.tmp', { syncDirectory: true });
    await appendJobEvent(this.directory, jobEvent(previous, record));
  }
}
