// Job ID grammar shared with checkpoint validation (checkpoint-recovery.mjs), so
// every job ID a checkpoint accepts can be looked up here: 1 to 128 characters.
export const VALID_JOB_ID = /^[a-z0-9][a-z0-9_-]{0,127}$/i;
export const VALID_PHASE = /^[a-z][a-z0-9_]{1,63}$/i;

/** An error carrying a JOB_* code and details, as both registries report them. */
export const jobError = (code, message, details = {}) => Object.assign(new Error(message), { code, ...details });

/** The job ID, if it follows the grammar above; a TypeError otherwise. */
export function requireJobId(jobId) {
  if (typeof jobId !== 'string' || !VALID_JOB_ID.test(jobId)) throw new TypeError('Invalid job ID');
  return jobId;
}

/** A detached copy of plain JSON data with sorted keys; throws for anything else. */
export function jsonInput(value, depth = 0) {
  if (depth > 32) throw new TypeError('input exceeds maximum JSON depth');
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (Array.isArray(value)) return value.map(item => jsonInput(item, depth + 1));
  if (typeof value === 'object' && [Object.prototype, null].includes(Object.getPrototypeOf(value))) {
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, jsonInput(value[key], depth + 1)]));
  }
  throw new TypeError('input must contain only JSON values');
}

const outcome = (status, reason, { cancellationConfirmed = false, effectAbsent = false } = {}) => ({
  status, reason, cancellationConfirmed, effectAbsent, reconciliationRequired: status === 'UNKNOWN'
});

/** A job cancelled before its executor was called: nothing ran. */
export const CANCELLED_BEFORE_DISPATCH = Object.freeze(
  outcome('CANCELLED', 'CANCELLED_BEFORE_DISPATCH', { cancellationConfirmed: true, effectAbsent: true }));

/**
 * The outcome of an executor's report, shared by the in-memory and the
 * file-backed registry. A cancellation counts as confirmed only with a
 * termination receipt; otherwise an aborted job is UNKNOWN. An executor that
 * cannot tell whether its effect happened reports UNKNOWN (for example
 * runSupervised when a command's start was not confirmed). effectAbsent (a
 * retry is safe) is true only when the executor explicitly reports it for a
 * FAILED or confirmed CANCELLED job. Reading the report may throw (hostile
 * getters); callers then use thrownOutcome().
 */
export function settlementOutcome(result, aborted) {
  const reported = result?.status;
  const confirmed = result?.termination?.confirmed === true;
  const effectAbsent = result?.effectAbsent === true;
  if (aborted && reported === 'CANCELLED' && confirmed) {
    return outcome('CANCELLED', 'CANCELLED_BY_REQUEST', { cancellationConfirmed: true, effectAbsent });
  }
  if (aborted && reported !== 'COMPLETED') return outcome('UNKNOWN', 'CANCELLATION_UNCONFIRMED');
  if (reported === 'UNKNOWN') return outcome('UNKNOWN', 'EXECUTOR_REPORTED_UNKNOWN');
  if (reported === 'COMPLETED') return outcome('COMPLETED', null);
  if (reported === 'FAILED') return outcome('FAILED', 'EXECUTOR_REPORTED_FAILURE', { effectAbsent });
  return outcome('FAILED', 'EXECUTOR_STATUS_UNEXPECTED');
}

/** The outcome when the executor threw: an error never proves that nothing happened. */
export function thrownOutcome(aborted) {
  return aborted ? outcome('UNKNOWN', 'EXECUTOR_THREW_AFTER_ABORT') : outcome('FAILED', 'EXECUTOR_THREW');
}

export const MAX_EVIDENCE_BYTES = 8 * 1024;
export const RECONCILED_STATUSES = Object.freeze(['COMPLETED', 'FAILED', 'CANCELLED']);

/**
 * Validate a reconciliation request shared by both registries. Evidence must be
 * authoritative, name its source and be plain JSON of at most 8 KiB; it is kept
 * for audit, so it must not contain credentials. Returns normalized values.
 */
export function reconciliationRequest({ disposition, evidence, result = null, explicitAuthorization = false } = {}) {
  if (explicitAuthorization !== true) {
    throw jobError('JOB_AUTHORIZATION_REQUIRED', 'Explicit authorization is required to reconcile a job');
  }
  if (!RECONCILED_STATUSES.includes(disposition)) {
    throw new TypeError(`disposition must be one of ${RECONCILED_STATUSES.join(', ')}`);
  }
  if (evidence?.authoritative !== true) {
    throw jobError('JOB_EVIDENCE_REQUIRED', 'Reconciliation requires authoritative evidence (evidence.authoritative: true)');
  }
  let normalized;
  try { normalized = jsonInput(evidence); }
  catch { throw new TypeError('evidence must be plain JSON'); }
  if (typeof normalized.source !== 'string' || !normalized.source.trim() || normalized.source.length > 200) {
    throw new TypeError('evidence.source must name the authority (1 to 200 characters)');
  }
  if (normalized.effectAbsent !== undefined && typeof normalized.effectAbsent !== 'boolean') {
    throw new TypeError('evidence.effectAbsent must be a boolean');
  }
  if (Buffer.byteLength(JSON.stringify(normalized)) > MAX_EVIDENCE_BYTES) throw new TypeError('evidence exceeds 8 KiB');
  let storedResult;
  try { storedResult = jsonInput(result); }
  catch { throw new TypeError('result must be plain JSON'); }
  return { disposition, evidence: normalized, result: storedResult };
}

/**
 * The state a reconciled job takes. effectAbsent (a retry is safe) only when the
 * evidence says so explicitly, and never for COMPLETED.
 */
export function reconciledState(previous, { disposition, evidence, result }, { at, override = null, ownerState } = {}) {
  return {
    status: disposition,
    reason: 'RECONCILED',
    reconciliationRequired: false,
    cancellationConfirmed: disposition === 'CANCELLED',
    effectAbsent: disposition !== 'COMPLETED' && evidence.effectAbsent === true,
    result,
    resultDiscarded: false,
    reconciliation: {
      at, disposition, evidence, override,
      previous: { status: previous.status, reason: previous.reason ?? null, ...(ownerState ? { ownerState } : {}) }
    }
  };
}

export function boundedSettlement(promise, timeoutMs) {
  return new Promise(resolve => {
    const timer = setTimeout(() => resolve(false), timeoutMs);
    promise.then(() => { clearTimeout(timer); resolve(true); });
  });
}

/** In-memory ownership and cancellation for cooperative asynchronous jobs. */
export class PhaseJobRegistry {
  #jobs = new Map();

  constructor({ execute, maxEntries = 64 } = {}) {
    if (typeof execute !== 'function') throw new TypeError('execute is required');
    if (!Number.isInteger(maxEntries) || maxEntries < 1 || maxEntries > 512) {
      throw new TypeError('maxEntries must be an integer between 1 and 512');
    }
    this.execute = execute;
    this.maxEntries = maxEntries;
  }

  #lookup(id) {
    return this.#jobs.get(requireJobId(id));
  }

  #snapshot(job) {
    if (!job) return null;
    return structuredClone({
      jobId: job.jobId,
      status: job.status,
      phase: job.phase,
      cancellationRequested: job.cancellationRequested,
      cancellationConfirmed: job.cancellationConfirmed,
      reconciliationRequired: job.reconciliationRequired,
      effectAbsent: job.effectAbsent,
      reason: job.reason,
      result: job.result,
      resultDiscarded: job.resultDiscarded,
      reconciliation: job.reconciliation
    });
  }

  start({ jobId, objective, input = null } = {}) {
    const existing = this.#lookup(jobId);
    if (typeof objective !== 'string' || !objective.trim() || objective.length > 4000) {
      throw new TypeError('A bounded, non-empty objective is required');
    }
    const copy = jsonInput(input);
    const fingerprint = JSON.stringify(copy);
    if (fingerprint === undefined || Buffer.byteLength(fingerprint) > 65536) {
      throw new TypeError('input must be JSON-serializable and at most 65536 bytes');
    }
    if (existing) {
      if (existing.objective !== objective || existing.fingerprint !== fingerprint) {
        throw new Error('Job identity cannot be reused with different input');
      }
      return this.#snapshot(existing);
    }
    // Only settled jobs with a definite outcome may be forgotten. Running jobs and
    // jobs that require reconciliation are kept: forgetting an UNKNOWN job would
    // let the same identity start, and run, again.
    for (const [id, job] of this.#jobs) {
      if (this.#jobs.size < this.maxEntries) break;
      if (job.status !== 'RUNNING' && !job.reconciliationRequired) this.#jobs.delete(id);
    }
    if (this.#jobs.size >= this.maxEntries) {
      const pinned = [...this.#jobs.values()];
      throw Object.assign(new Error('Job capacity exhausted'), {
        code: 'JOB_CAPACITY_EXHAUSTED',
        running: pinned.filter(entry => entry.status === 'RUNNING').length,
        reconciliationRequired: pinned.filter(entry => entry.reconciliationRequired).length
      });
    }

    const job = {
      jobId, objective, fingerprint,
      status: 'RUNNING', phase: 'queued', result: null,
      controller: new AbortController(),
      cancellationRequested: false, cancellationConfirmed: false,
      reconciliationRequired: false, effectAbsent: false, reason: null, resultDiscarded: false, reconciliation: null
    };
    const settle = (decided, result = null, resultDiscarded = false) => {
      Object.assign(job, decided, { result, resultDiscarded });
    };
    this.#jobs.set(jobId, job);
    const notDispatched = Symbol('not dispatched');
    job.settlement = Promise.resolve().then(() => {
      if (job.controller.signal.aborted) return notDispatched;
      return this.execute({
      jobId, objective, input: copy, signal: job.controller.signal,
      setPhase: phase => {
        if (typeof phase !== 'string' || !VALID_PHASE.test(phase)) throw new TypeError('Invalid job phase');
        if (job.status !== 'RUNNING') return false;
        job.phase = phase;
        return true;
      }
      });
    }).then(result => {
      if (result === notDispatched) return settle(CANCELLED_BEFORE_DISPATCH);
      // The outcome is decided from the executor's report alone. A result that
      // cannot be copied is discarded; it never changes the reported outcome.
      const decided = settlementOutcome(result, job.controller.signal.aborted);
      try { settle(decided, structuredClone(result ?? null)); }
      catch { settle(decided, null, true); }
    }).catch(() => {
      // Raw executor errors may contain credentials; never retain them here.
      settle(thrownOutcome(job.controller.signal.aborted));
    });
    return this.#snapshot(job);
  }

  get(jobId) { return this.#snapshot(this.#lookup(jobId)); }

  async wait(jobId) {
    const job = this.#lookup(jobId);
    if (!job) return null;
    await job.settlement;
    return this.#snapshot(job);
  }

  /**
   * Resolve an UNKNOWN job with authoritative evidence: { jobId, disposition
   * (COMPLETED, FAILED or CANCELLED), evidence: { authoritative: true, source,
   * effectAbsent? }, result?, explicitAuthorization: true }. Never runs the job.
   * The reconciled job has a definite outcome, so it no longer pins capacity.
   */
  reconcile({ jobId, ...request } = {}) {
    const job = this.#lookup(jobId);
    const normalized = reconciliationRequest(request);
    if (!job) throw jobError('JOB_NOT_FOUND', 'No such job', { jobId });
    if (job.status === 'RUNNING') throw jobError('JOB_RUNNING_HERE', 'The job is running in this process; it cannot be reconciled', { jobId });
    if (job.status !== 'UNKNOWN') {
      throw jobError('JOB_RECONCILIATION_NOT_REQUIRED', `The job already has a definite outcome (${job.status})`, { jobId });
    }
    Object.assign(job, reconciledState(job, normalized, { at: new Date().toISOString() }));
    return this.#snapshot(job);
  }

  async requestCancellation(jobId, { timeoutMs = 1000 } = {}) {
    if (!Number.isInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > 30000) {
      throw new TypeError('timeoutMs must be an integer between 0 and 30000');
    }
    const job = this.#lookup(jobId);
    if (!job) return null;
    if (job.status !== 'RUNNING') return this.#snapshot(job);
    job.cancellationRequested = true;
    job.controller.abort();
    await boundedSettlement(job.settlement, timeoutMs);
    // An executor ignoring abort remains RUNNING; a timeout is not proof of termination.
    return this.#snapshot(job);
  }
}
