import { createHash } from 'node:crypto';

const copy = value => structuredClone(value);
function requiredString(value, label) {
  if (typeof value !== 'string' || !value.trim()) throw new TypeError(label + ' must be a non-empty string');
  return value.trim();
}
function requestFingerprint(request) {
  // Treat the request as immutable. JSON key order is significant by design.
  return createHash('sha256').update(JSON.stringify(request ?? {})).digest('hex');
}
function metadataError(error, status, key, retryAllowed = false) {
  const reported = error instanceof Error ? error : new Error(String(error));
  reported.idempotency = { status, key, retryAllowed, reconciliationRequired: status === 'IDEMPOTENCY_UNKNOWN' };
  return reported;
}

/**
 * Records effect-intent before executing it. An ambiguous effect is never retried
 * automatically, including after process recovery.
 *
 * Store adapter: appendEvent(type, key, { record }), listEvents({ typePrefix }).
 * When no store is supplied the guarantee is only within this process lifetime.
 */
export class IdempotencyRegistry {
  constructor({ store = null, clock = () => new Date() } = {}) {
    this.store = store;
    this.clock = clock;
    this.records = new Map();
    this.inflight = new Map();
    this.reconciling = new Set();
    this.restoring = false;
  }

  async persist(type, record) {
    if (this.store) await this.store.appendEvent(type, record.key, { record: copy(record) });
    this.records.set(record.key, copy(record));
    return record;
  }

  async restore() {
    if (this.restoring) throw new Error('Restoration is already in progress');
    if (this.inflight.size) throw new Error('Cannot restore while operations are running');
    if (this.reconciling.size) throw new Error('Cannot restore while reconciliation is running');
    if (!this.store) return this.snapshot();
    // Reserve the registry before calling the asynchronous journal adapter.
    this.restoring = true;
    try {
      const events = await this.store.listEvents({ typePrefix: 'idempotency.' });
      this.records.clear();
      for (const event of events) {
        const record = event?.data?.record;
        if (typeof record?.key === 'string' && typeof record.fingerprint === 'string') {
          this.records.set(record.key, copy(record));
        }
      }
      for (const record of [...this.records.values()]) {
        if (record.state !== 'STARTED') continue;
        const unknown = { ...record, state: 'UNKNOWN', unknownAt: this.clock().toISOString(),
          reconciliationRequired: true, reason: 'INTERRUPTED_AFTER_EFFECT_START' };
        this.records.set(record.key, unknown); // fail closed even if the journal write fails
        await this.persist('idempotency.unknown', unknown);
      }
      return this.snapshot();
    } finally {
      this.restoring = false;
    }
  }

  async execute({ key, request = {}, operation, retryFailed = false, failureProvesNoEffect = false } = {}) {
    key = requiredString(key, 'key');
    if (this.restoring) throw new Error('Restoration is in progress');
    if (this.reconciling.has(key)) throw new Error('Reconciliation is running for this key');
    if (typeof operation !== 'function') throw new TypeError('operation must be a function');
    const fingerprint = requestFingerprint(request);
    const active = this.inflight.get(key);
    if (active) {
      if (active.fingerprint !== fingerprint) return { status: 'IDEMPOTENCY_CONFLICT', key, executed: false };
      return active.promise; // one effect, multiple waiters
    }
    const previous = this.records.get(key);
    if (previous) {
      if (previous.fingerprint !== fingerprint) return { status: 'IDEMPOTENCY_CONFLICT', key, executed: false };
      if (previous.state === 'COMPLETED') return { status: 'IDEMPOTENT_REPLAY', key, executed: false, result: copy(previous.result) };
      if (previous.state === 'UNKNOWN' || previous.state === 'STARTED') {
        return { status: 'IDEMPOTENCY_UNKNOWN', key, executed: false, reconciliationRequired: true };
      }
      if (previous.state === 'FAILED' && !(retryFailed && previous.effectAbsent === true)) {
        return { status: 'IDEMPOTENCY_FAILED', key, executed: false, retryAllowed: previous.effectAbsent === true };
      }
    }
    const started = { key, fingerprint, state: 'STARTED', startedAt: this.clock().toISOString(),
      attempt: (previous?.attempt ?? 0) + 1 };
    // Install the reservation before a journal adapter can call back into us.
    const promise = Promise.resolve().then(() => this.runEffect(started, operation, failureProvesNoEffect));
    this.inflight.set(key, { fingerprint, promise });
    try { return await promise; }
    finally { if (this.inflight.get(key)?.promise === promise) this.inflight.delete(key); }
  }

  async runEffect(started, operation, failureProvesNoEffect) {
    await this.persist('idempotency.started', started);
    let result;
    try { result = await operation(); }
    catch (error) {
      const noEffect = failureProvesNoEffect === true;
      const failed = { ...started, state: noEffect ? 'FAILED' : 'UNKNOWN',
        failedAt: this.clock().toISOString(), effectAbsent: noEffect,
        reconciliationRequired: !noEffect, reason: noEffect ? 'AUTHORITATIVE_NO_EFFECT' : 'EFFECT_OUTCOME_AMBIGUOUS' };
      this.records.set(started.key, failed); // never retry silently on a failed journal write
      await this.persist(noEffect ? 'idempotency.failed' : 'idempotency.unknown', failed);
      throw metadataError(error, noEffect ? 'IDEMPOTENCY_FAILED' : 'IDEMPOTENCY_UNKNOWN', started.key, noEffect);
    }
    const completed = { ...started, state: 'COMPLETED', completedAt: this.clock().toISOString(),
      result: copy(result), reconciliationRequired: false };
    try {
      await this.persist('idempotency.completed', completed);
    } catch (error) {
      const unknown = { ...started, state: 'UNKNOWN', unknownAt: this.clock().toISOString(),
        reconciliationRequired: true, reason: 'COMPLETION_PERSISTENCE_FAILED' };
      this.records.set(started.key, unknown);
      try { await this.store?.appendEvent('idempotency.unknown', started.key, { record: copy(unknown) }); } catch {}
      throw metadataError(error, 'IDEMPOTENCY_UNKNOWN', started.key);
    }
    return { status: 'IDEMPOTENCY_EXECUTED', key: started.key, executed: true, result: copy(result) };
  }

  async reconcile({ key, request = {}, disposition, result = null, evidence } = {}) {
    key = requiredString(key, 'key');
    if (this.restoring) throw new Error('Restoration is in progress');
    if (this.inflight.has(key)) throw new Error('Cannot reconcile while the operation is running');
    if (this.reconciling.has(key)) throw new Error('Reconciliation is already running for this key');
    const record = this.records.get(key);
    if (!record) return { status: 'IDEMPOTENCY_NOT_FOUND', key, reconciled: false };
    if (record.fingerprint !== requestFingerprint(request)) return { status: 'IDEMPOTENCY_CONFLICT', key, reconciled: false };
    if (!['UNKNOWN', 'STARTED'].includes(record.state)) return { status: 'IDEMPOTENCY_RECONCILIATION_NOT_REQUIRED', key, reconciled: false };
    if (evidence?.authoritative !== true) return { status: 'IDEMPOTENCY_EVIDENCE_REQUIRED', key, reconciled: false };
    // Hold the key through journal persistence, including failures. Other keys
    // can continue executing; no stale reconciliation may replace a receipt.
    this.reconciling.add(key);
    try {
      if (disposition === 'COMPLETED') {
        const completed = { ...record, state: 'COMPLETED', result: copy(result), completedAt: this.clock().toISOString(),
          reconciliationRequired: false, evidence: copy(evidence) };
        await this.persist('idempotency.reconciled_completed', completed);
        return { status: 'IDEMPOTENCY_RECONCILED_COMPLETED', key, reconciled: true, result: copy(result) };
      }
      if (disposition === 'FAILED_NO_EFFECT' && evidence.effectAbsent === true) {
        const failed = { ...record, state: 'FAILED', effectAbsent: true, reconciliationRequired: false,
          evidence: copy(evidence) };
        await this.persist('idempotency.reconciled_failed', failed);
        return { status: 'IDEMPOTENCY_RECONCILED_FAILED', key, reconciled: true, retryAllowed: true };
      }
      return { status: 'IDEMPOTENCY_INVALID_RECONCILIATION', key, reconciled: false };
    } finally {
      this.reconciling.delete(key);
    }
  }

  snapshot() {
    const states = {};
    for (const record of this.records.values()) states[record.state] = (states[record.state] ?? 0) + 1;
    return { schema: 'mnemos.idempotency.v2', count: this.records.size, states };
  }
}
