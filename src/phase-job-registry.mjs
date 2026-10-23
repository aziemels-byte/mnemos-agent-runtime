const VALID_ID = /^[a-z0-9][a-z0-9_-]{2,127}$/i;
const VALID_PHASE = /^[a-z][a-z0-9_]{1,63}$/i;

function jsonInput(value, depth = 0) {
  if (depth > 32) throw new TypeError('input exceeds maximum JSON depth');
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (Array.isArray(value)) return value.map(item => jsonInput(item, depth + 1));
  if (typeof value === 'object' && [Object.prototype, null].includes(Object.getPrototypeOf(value))) {
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, jsonInput(value[key], depth + 1)]));
  }
  throw new TypeError('input must contain only JSON values');
}

function boundedSettlement(promise, timeoutMs) {
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
    if (typeof id !== 'string' || !VALID_ID.test(id)) throw new TypeError('Invalid job ID');
    return this.#jobs.get(id);
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
      result: job.result
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
    for (const [id, job] of this.#jobs) {
      if (this.#jobs.size < this.maxEntries) break;
      if (job.status !== 'RUNNING') this.#jobs.delete(id);
    }
    if (this.#jobs.size >= this.maxEntries) throw new Error('Job capacity exhausted');

    const job = {
      jobId, objective, fingerprint,
      status: 'RUNNING', phase: 'queued', result: null,
      controller: new AbortController(),
      cancellationRequested: false, cancellationConfirmed: false,
      reconciliationRequired: false
    };
    this.#jobs.set(jobId, job);
    job.settlement = Promise.resolve().then(() => {
      if (job.controller.signal.aborted) return { status: 'CANCELLED', termination: { confirmed: true } };
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
      const copy = structuredClone(result ?? null);
      const aborted = job.controller.signal.aborted;
      if (aborted && result?.status === 'CANCELLED' && result?.termination?.confirmed === true) {
        job.status = 'CANCELLED';
        job.cancellationConfirmed = true;
      } else if (aborted && result?.status !== 'COMPLETED') {
        job.status = 'UNKNOWN';
        job.reconciliationRequired = true;
      } else {
        job.status = result?.status === 'COMPLETED' ? 'COMPLETED' : 'FAILED';
      }
      job.result = copy;
    }).catch(() => {
      // Raw executor errors may contain credentials; never retain them here.
      job.status = job.controller.signal.aborted ? 'UNKNOWN' : 'FAILED';
      job.cancellationConfirmed = false;
      job.reconciliationRequired = job.status === 'UNKNOWN';
      job.result = null;
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
