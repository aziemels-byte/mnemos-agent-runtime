import { createHash } from 'node:crypto';
import path from 'node:path';
import { DirectoryLock, readBoundedFile, replaceFile } from './directory-lock.mjs';
import { VALID_JOB_ID, VALID_PHASE } from './phase-job-registry.mjs';

const MAX_BYTES = 1024 * 1024;
const VALID_GOAL_ID = /^[a-z0-9][a-z0-9_-]{0,127}$/i;
const KNOWN_STATUS = new Set(['COMPLETED', 'RUNNING', 'CANCELLED', 'FAILED', 'UNKNOWN']);
const REASON = /^[A-Z][A-Z0-9_]{0,63}$/;
const MAX_INSPECTION_TIMEOUT_MS = 300_000;
const MAX_RECOVERY_TIMEOUT_MS = 3_600_000;
const REVISION = /^[0-9a-f]{64}$/;
const CHECKPOINT_TEMPORARY = /^(?:[0-9a-f]{32}\.tmp|[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\.checkpoint\.tmp)$/;
const revisionOf = bytes => createHash('sha256').update(bytes).digest('hex');

/**
 * Atomic same-directory replacement. Saves are serialized across processes on
 * one host by a directory lock, and can be conditional on the revision (SHA-256
 * of the stored bytes) the caller last read, so a stale writer cannot silently
 * overwrite a newer checkpoint. Reads never take the lock.
 */
export class FileCheckpointStore {
  #writes = Promise.resolve();
  #lock;

  constructor(directory, { lockTimeoutMs = 5_000 } = {}) {
    if (typeof directory !== 'string' || !directory.trim()) throw new TypeError('A checkpoint directory is required');
    this.directory = path.resolve(directory);
    this.file = path.join(this.directory, 'checkpoint.json');
    // Temporaries of earlier versions were named <32 hex>.tmp.
    this.#lock = new DirectoryLock({ directory: this.directory, name: 'checkpoint.lock', timeoutMs: lockTimeoutMs,
      timeoutCode: 'CHECKPOINT_LOCK_TIMEOUT',
      ownedTemporary: name => CHECKPOINT_TEMPORARY.test(name) });
    this.lockFile = this.#lock.lockFile;
  }

  /**
   * expectedRevision: undefined saves unconditionally; null requires that no
   * checkpoint exists; a revision requires the stored bytes to still match it.
   * Resolves to { revision } of the written checkpoint; every failure, invalid
   * arguments included, rejects. Saves are queued in call order.
   */
  async save(value, { expectedRevision } = {}) {
    const json = JSON.stringify(value, null, 2);
    if (typeof json !== 'string') throw new TypeError('A checkpoint must be a JSON value');
    const text = json + '\n';
    if (Buffer.byteLength(text) > MAX_BYTES) throw new RangeError('Checkpoint exceeds 1 MiB');
    if (expectedRevision !== undefined && expectedRevision !== null &&
      !(typeof expectedRevision === 'string' && REVISION.test(expectedRevision))) {
      throw new TypeError('Invalid expectedRevision');
    }
    const operation = this.#writes.then(() => this.#lock.run(async () => {
      if (expectedRevision !== undefined) {
        const current = await this.#readBytes();
        const currentRevision = current === null ? null : revisionOf(current);
        if (currentRevision !== expectedRevision) {
          throw Object.assign(new Error('Checkpoint changed since it was read; reload and reconcile'),
            { code: 'CHECKPOINT_CONFLICT', expectedRevision, currentRevision });
        }
      }
      await replaceFile(this.file, text, '.checkpoint.tmp', { syncDirectory: true });
      return { revision: revisionOf(text) };
    }));
    this.#writes = operation.catch(() => {});
    return operation;
  }

  /** Resolves to { value, revision }, or null when no checkpoint exists. */
  async read() {
    await this.#writes;
    const bytes = await this.#readBytes();
    if (bytes === null) return null;
    return { value: JSON.parse(bytes.toString('utf8')), revision: revisionOf(bytes) };
  }

  async load() {
    return (await this.read())?.value ?? null;
  }

  /** Read-only view of the checkpoint lock for operators; never takes the lock. */
  inspectLock() { return this.#lock.inspect(); }

  /** Operator override; see the README runbook. */
  forceUnlock(request) { return this.#lock.forceUnlock(request); }

  #readBytes() {
    return readBoundedFile(this.file, MAX_BYTES,
      reason => new RangeError(reason === 'TOO_LARGE' ? 'Checkpoint exceeds 1 MiB' : 'Invalid checkpoint file'));
  }
}

function validCheckpoint(value) {
  return value?.version === 1 && typeof value.goal?.goalId === 'string' &&
    VALID_GOAL_ID.test(value.goal.goalId) && typeof value.checkpointedAt === 'string' &&
    Number.isFinite(Date.parse(value.checkpointedAt)) && Array.isArray(value.activeJobs) &&
    value.activeJobs.length <= 1000 && value.activeJobs.every(job =>
      typeof job?.id === 'string' && VALID_JOB_ID.test(job.id)) &&
    new Set(value.activeJobs.map(job => job.id)).size === value.activeJobs.length;
}

// Checked only when saving: a checkpoint records which jobs belong to a goal. A
// status or phase stored with a job is a hint for readers, never authoritative,
// but new checkpoints may not store values no job can have. Older checkpoints
// with such values still load.
function validJobHints(activeJobs) {
  return activeJobs.every(job => (job.status === undefined || KNOWN_STATUS.has(job.status)) &&
    (job.phase === undefined || (typeof job.phase === 'string' && VALID_PHASE.test(job.phase))));
}

function boundedInteger(value, label, max) {
  if (!Number.isInteger(value) || value < 1 || value > max) {
    throw new TypeError(`${label} must be an integer between 1 and ${max}`);
  }
  return value;
}

/**
 * Ask the inspector about one job, for at most timeoutMs. The inspector gets an
 * AbortSignal that aborts on timeout; an inspector that ignores it is abandoned,
 * never awaited. Any failure is uncertainty, never permission to replay.
 */
function inspectJob(inspector, job, timeoutMs) {
  const controller = new AbortController();
  return new Promise(resolve => {
    const timer = setTimeout(() => {
      controller.abort(Object.assign(new Error('Job inspection timed out'), { code: 'INSPECTION_TIMEOUT' }));
      resolve({ status: 'UNKNOWN', reason: 'INSPECTION_TIMEOUT' });
    }, timeoutMs);
    Promise.resolve()
      .then(() => inspector(structuredClone(job), { signal: controller.signal }))
      .then(inspection => {
        // Throwing getters are inspection failures.
        const status = inspection?.status;
        if (!KNOWN_STATUS.has(status)) return resolve({ status: 'UNKNOWN', reason: 'INSPECTION_UNSUPPORTED_STATUS' });
        const reason = typeof inspection.reason === 'string' && REASON.test(inspection.reason) ? inspection.reason : undefined;
        // A retry is safe only on an explicit report for a job that did not complete.
        const effectAbsent = inspection.effectAbsent === true && (status === 'FAILED' || status === 'CANCELLED');
        resolve({ status, reason, effectAbsent });
      })
      .catch(() => resolve({ status: 'UNKNOWN', reason: 'INSPECTION_FAILED' }))
      .finally(() => clearTimeout(timer));
  });
}

function invalidRecovery(reason) {
  return { goal: null, dag: null, jobs: [], recoveryRequired: true, recoveryError: reason };
}

/** Restore state and reconcile jobs. This class never executes or replays a task. */
export class GoalRecoveryCoordinator {
  // Revision of the checkpoint this coordinator last read or wrote:
  // undefined = never seen, null = known to be absent, string = that revision.
  #revision = undefined;
  #serial = Promise.resolve();
  #recovery;

  /**
   * inspectionTimeoutMs bounds each jobInspector call; recoveryTimeoutMs bounds
   * all inspections of one recover(). A job whose inspection times out, or that
   * is not inspected before the recovery budget runs out, is UNKNOWN.
   */
  constructor({ store, jobInspector = async () => ({ status: 'UNKNOWN' }),
    inspectionTimeoutMs = 10_000, recoveryTimeoutMs = 120_000 } = {}) {
    if (typeof store?.save !== 'function' || typeof store?.load !== 'function') {
      throw new TypeError('A checkpoint store is required');
    }
    if (typeof jobInspector !== 'function') throw new TypeError('jobInspector must be a function');
    this.store = store;
    this.jobInspector = jobInspector;
    this.inspectionTimeoutMs = boundedInteger(inspectionTimeoutMs, 'inspectionTimeoutMs', MAX_INSPECTION_TIMEOUT_MS);
    this.recoveryTimeoutMs = boundedInteger(recoveryTimeoutMs, 'recoveryTimeoutMs', MAX_RECOVERY_TIMEOUT_MS);
  }

  /** The last observed or saved revision (undefined: none seen yet). */
  get lastKnownRevision() { return this.#revision; }

  // Reads and saves of one coordinator run in call order, so overlapping calls
  // each see the revision left by the previous one.
  #inOrder(operation) {
    const next = this.#serial.then(operation);
    this.#serial = next.catch(() => {});
    return next;
  }

  /**
   * With a store that supports conditional saves (read()), a checkpoint replaces
   * only the checkpoint this coordinator last saw: the revision from its last
   * recover() or checkpoint(), or "none exists" if it has seen neither. Another
   * writer in between causes CHECKPOINT_CONFLICT; recover() and reconcile first.
   * When recoveries overlap or a recovery is in progress, pass expectedRevision
   * from the snapshot being saved. unconditional: true saves regardless (last
   * writer wins). Stores without read() always save unconditionally.
   */
  async checkpoint({ goal, dag = null, activeJobs = [], expectedRevision, unconditional = false } = {}) {
    const value = structuredClone({ version: 1, goal, dag, activeJobs, checkpointedAt: new Date().toISOString() });
    if (!validCheckpoint(value)) throw new TypeError('Invalid checkpoint shape or duplicate job ID');
    if (!validJobHints(value.activeJobs)) throw new TypeError('Invalid checkpoint job status or phase');
    if (unconditional !== false && unconditional !== true) throw new TypeError('unconditional must be a boolean');
    if (unconditional && expectedRevision !== undefined) throw new TypeError('Use either expectedRevision or unconditional, not both');
    const conditional = typeof this.store.read === 'function';
    if (expectedRevision !== undefined && !conditional) {
      // Ignoring the condition would silently allow the overwrite it exists to prevent.
      throw new TypeError('This checkpoint store does not support conditional saves');
    }
    if (conditional && this.#recovery) {
      if (expectedRevision === undefined && !unconditional) {
        throw Object.assign(new Error('Use the recovered snapshot revision or wait and recover again'),
          { code: 'CHECKPOINT_REVISION_REQUIRED' });
      }
      if (this.#recovery.pending) this.#recovery.overlapped = true;
    }
    return this.#inOrder(async () => {
      let saved;
      if (!conditional || unconditional) {
        saved = await this.store.save(value);
      } else {
        const required = expectedRevision !== undefined ? expectedRevision : (this.#revision ?? null);
        saved = await this.store.save(value, { expectedRevision: required });
      }
      if (typeof saved?.revision !== 'string') return value;
      this.#revision = saved.revision;
      return { ...value, checkpointRevision: saved.revision };
    });
  }

  async recover() {
    if (typeof this.store.read !== 'function') {
      let value;
      try { value = await this.store.load(); }
      catch { return invalidRecovery('CHECKPOINT_UNREADABLE'); }
      return this.#inspectCheckpoint(value);
    }

    const previous = this.#recovery;
    const recovery = previous?.pending ? previous : { pending: 0, overlapped: false };
    if (recovery.pending > 0) recovery.overlapped = true;
    recovery.pending++;
    this.#recovery = recovery;
    let stored;
    try {
      try {
        stored = await this.#inOrder(async () => {
          const checkpoint = await this.store.read();
          this.#revision = checkpoint === null ? null : checkpoint.revision;
          return checkpoint;
        });
      } catch { return invalidRecovery('CHECKPOINT_UNREADABLE'); }
      return await this.#inspectCheckpoint(stored === null ? null : stored.value, stored?.revision);
    } finally {
      // Two callers may hold different snapshots even after both recoveries settle.
      // A successful recovery that runs on its own restores the default revision.
      if (--recovery.pending === 0 && !recovery.overlapped) {
        this.#recovery = stored === undefined ? previous : undefined;
      }
    }
  }

  async #inspectCheckpoint(value, revision) {
    if (value === null) return null;
    const withRevision = result => revision === undefined ? result : { ...result, checkpointRevision: revision };
    if (!validCheckpoint(value)) return withRevision(invalidRecovery('CHECKPOINT_INVALID'));
    const jobs = [];
    const deadline = performance.now() + this.recoveryTimeoutMs;
    for (const job of value.activeJobs) {
      const remaining = Math.floor(deadline - performance.now());
      // recoveryReason: the inspector's reason, or why the inspection itself failed.
      const { status, reason, effectAbsent = false } = remaining < 1
        ? { status: 'UNKNOWN', reason: 'RECOVERY_TIMEOUT' }
        : await inspectJob(this.jobInspector, job, Math.min(this.inspectionTimeoutMs, remaining));
      const entry = { ...structuredClone(job), recoveryStatus: status, recoveryEffectAbsent: effectAbsent,
        // A status stored in the checkpoint is only a hint; flag it when the inspection disagrees.
        checkpointStatusStale: job.status !== undefined && job.status !== status };
      if (reason) entry.recoveryReason = reason;
      else delete entry.recoveryReason; // never echo a stale reason stored in the checkpoint
      jobs.push(entry);
    }
    return withRevision({
      ...structuredClone(value), jobs,
      recoveryRequired: jobs.some(job => job.recoveryStatus !== 'COMPLETED')
    });
  }
}
