import { createHash, randomUUID } from 'node:crypto';
import { open, readdir } from 'node:fs/promises';
import { hostname } from 'node:os';
import path from 'node:path';
import { DirectoryLock, processState, replaceFile } from './directory-lock.mjs';

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

// Decision rules shared by the in-memory and the file-backed registry.

/** Response for an existing record, or null when the effect may run (again). */
function existingOutcome(previous, fingerprint, retryFailed, key) {
  if (!previous) return null;
  if (previous.fingerprint !== fingerprint) return { status: 'IDEMPOTENCY_CONFLICT', key, executed: false };
  if (previous.state === 'COMPLETED') return { status: 'IDEMPOTENT_REPLAY', key, executed: false, result: copy(previous.result) };
  if (previous.state === 'UNKNOWN' || previous.state === 'STARTED') {
    return { status: 'IDEMPOTENCY_UNKNOWN', key, executed: false, reconciliationRequired: true };
  }
  if (previous.state === 'FAILED' && !(retryFailed && previous.effectAbsent === true)) {
    return { status: 'IDEMPOTENCY_FAILED', key, executed: false, retryAllowed: previous.effectAbsent === true };
  }
  return null;
}

function failureRecord(started, noEffect, now) {
  return { ...started, state: noEffect ? 'FAILED' : 'UNKNOWN', failedAt: now, effectAbsent: noEffect,
    reconciliationRequired: !noEffect, reason: noEffect ? 'AUTHORITATIVE_NO_EFFECT' : 'EFFECT_OUTCOME_AMBIGUOUS' };
}

function unknownRecord(record, reason, now) {
  return { ...record, state: 'UNKNOWN', unknownAt: now, reconciliationRequired: true, reason };
}

/** Response when reconciliation cannot proceed, or null when it may. */
function reconciliationRefusal(record, request, evidence, key) {
  if (!record) return { status: 'IDEMPOTENCY_NOT_FOUND', key, reconciled: false };
  if (record.fingerprint !== requestFingerprint(request)) return { status: 'IDEMPOTENCY_CONFLICT', key, reconciled: false };
  if (!['UNKNOWN', 'STARTED'].includes(record.state)) return { status: 'IDEMPOTENCY_RECONCILIATION_NOT_REQUIRED', key, reconciled: false };
  if (evidence?.authoritative !== true) return { status: 'IDEMPOTENCY_EVIDENCE_REQUIRED', key, reconciled: false };
  return null;
}

/** The journal event and record to persist for a reconciliation, plus the response. */
function reconciliationTransition(record, disposition, result, evidence, now, key) {
  if (disposition === 'COMPLETED') {
    return { type: 'idempotency.reconciled_completed',
      record: { ...record, state: 'COMPLETED', result: copy(result), completedAt: now, reconciliationRequired: false, evidence: copy(evidence) },
      response: { status: 'IDEMPOTENCY_RECONCILED_COMPLETED', key, reconciled: true, result: copy(result) } };
  }
  if (disposition === 'FAILED_NO_EFFECT' && evidence.effectAbsent === true) {
    return { type: 'idempotency.reconciled_failed',
      record: { ...record, state: 'FAILED', effectAbsent: true, reconciliationRequired: false, evidence: copy(evidence) },
      response: { status: 'IDEMPOTENCY_RECONCILED_FAILED', key, reconciled: true, retryAllowed: true } };
  }
  return { response: { status: 'IDEMPOTENCY_INVALID_RECONCILIATION', key, reconciled: false } };
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
        const unknown = unknownRecord(record, 'INTERRUPTED_AFTER_EFFECT_START', this.clock().toISOString());
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
    const outcome = existingOutcome(previous, fingerprint, retryFailed, key);
    if (outcome) return outcome;
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
      const failed = failureRecord(started, noEffect, this.clock().toISOString());
      this.records.set(started.key, failed); // never retry silently on a failed journal write
      await this.persist(noEffect ? 'idempotency.failed' : 'idempotency.unknown', failed);
      throw metadataError(error, noEffect ? 'IDEMPOTENCY_FAILED' : 'IDEMPOTENCY_UNKNOWN', started.key, noEffect);
    }
    const completed = { ...started, state: 'COMPLETED', completedAt: this.clock().toISOString(),
      result: copy(result), reconciliationRequired: false };
    try {
      await this.persist('idempotency.completed', completed);
    } catch (error) {
      const unknown = unknownRecord(started, 'COMPLETION_PERSISTENCE_FAILED', this.clock().toISOString());
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
    const refusal = reconciliationRefusal(record, request, evidence, key);
    if (refusal) return refusal;
    // Hold the key through journal persistence, including failures. Other keys
    // can continue executing; no stale reconciliation may replace a receipt.
    this.reconciling.add(key);
    try {
      const transition = reconciliationTransition(record, disposition, result, evidence, this.clock().toISOString(), key);
      if (transition.record) await this.persist(transition.type, transition.record);
      return transition.response;
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


const RECORD_SCHEMA = 'mnemos.idempotency-record.v1';
const MAX_RECORD_BYTES = 1024 * 1024;
const STATES = new Set(['STARTED', 'COMPLETED', 'FAILED', 'UNKNOWN']);
const RECORD_FILE = /^[0-9a-f]{64}\.record\.json$/;
const RECORD_TEMPORARY = /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}\.record\.tmp$/;
// Executions running in this process. A STARTED record owned by this PID whose
// execution is not listed here was left by an earlier process that reused the PID,
// or by a finalization that failed; either way it is not running.
const liveExecutions = new Set();

function stateError(message, details = {}) {
  return Object.assign(new Error(message), { code: 'IDEMPOTENCY_STATE_INVALID', ...details });
}

function ownerState(owner) {
  if (owner?.pid === process.pid && owner.host === hostname()) {
    return liveExecutions.has(owner.executionId) ? 'ALIVE' : 'DEAD';
  }
  return processState(owner);
}

/**
 * Idempotency shared by processes on one host through a state directory.
 * Under a directory lock each execution atomically checks the key and records
 * STARTED with its owner before the effect runs; the effect itself runs without
 * the lock. A STARTED record whose owner is alive is IN_PROGRESS for everyone
 * else; only a provably dead owner turns it into UNKNOWN. Ambiguous outcomes
 * are never retried automatically. Results must be JSON and at most ~1 MiB.
 */
export class FileIdempotencyRegistry {
  #lock;
  #inflight = new Map();

  constructor({ directory, clock = () => new Date(), lockTimeoutMs = 5_000 } = {}) {
    if (typeof directory !== 'string' || !directory.trim()) throw new TypeError('An idempotency state directory is required');
    this.directory = path.resolve(directory);
    this.clock = clock;
    this.#lock = new DirectoryLock({ directory: this.directory, name: 'idempotency.lock', timeoutMs: lockTimeoutMs,
      timeoutCode: 'IDEMPOTENCY_LOCK_TIMEOUT', ownedTemporary: name => RECORD_TEMPORARY.test(name) });
    this.lockFile = this.#lock.lockFile;
  }

  async execute({ key, request = {}, operation, retryFailed = false, failureProvesNoEffect = false } = {}) {
    key = requiredString(key, 'key');
    if (typeof operation !== 'function') throw new TypeError('operation must be a function');
    const fingerprint = requestFingerprint(request);
    const active = this.#inflight.get(key);
    if (active) {
      if (active.fingerprint !== fingerprint) return { status: 'IDEMPOTENCY_CONFLICT', key, executed: false };
      return active.promise; // one effect, multiple waiters in this instance
    }
    const promise = Promise.resolve().then(() => this.#run(key, fingerprint, operation, retryFailed, failureProvesNoEffect));
    this.#inflight.set(key, { fingerprint, promise });
    try { return await promise; }
    finally { if (this.#inflight.get(key)?.promise === promise) this.#inflight.delete(key); }
  }

  async #run(key, fingerprint, operation, retryFailed, failureProvesNoEffect) {
    const executionId = randomUUID();
    const reservation = await this.#lock.run(async () => {
      let previous = await this.#read(key);
      if (previous?.state === 'STARTED' && previous.fingerprint === fingerprint) {
        if (ownerState(previous.owner) !== 'DEAD') return { response: { status: 'IDEMPOTENCY_IN_PROGRESS', key, executed: false } };
        previous = unknownRecord(previous, 'INTERRUPTED_AFTER_EFFECT_START', this.clock().toISOString());
        await this.#write(previous);
      }
      const outcome = existingOutcome(previous, fingerprint, retryFailed, key);
      if (outcome) return { response: outcome };
      const started = { key, fingerprint, state: 'STARTED', startedAt: this.clock().toISOString(),
        attempt: (previous?.attempt ?? 0) + 1, owner: { pid: process.pid, host: hostname(), executionId } };
      // Listed before the record becomes visible, so this process never sees it as dead.
      liveExecutions.add(executionId);
      try { await this.#write(started); }
      catch (error) { liveExecutions.delete(executionId); throw error; }
      return { started };
    });
    if (reservation.response) return reservation.response;

    const { started } = reservation;
    try {
      let outcome;
      try { outcome = { succeeded: true, result: await operation() }; }
      catch (error) { outcome = { succeeded: false, error }; }
      return await this.#finish(started, outcome, failureProvesNoEffect === true);
    } finally {
      liveExecutions.delete(executionId);
    }
  }

  async #finish(started, { succeeded, result, error }, noEffect) {
    const key = started.key;
    try {
      await this.#replaceOwn(started, succeeded
        ? { ...started, state: 'COMPLETED', completedAt: this.clock().toISOString(), result: copy(result), reconciliationRequired: false }
        : failureRecord(started, noEffect, this.clock().toISOString()));
    } catch (persistError) {
      // The effect may have happened but its outcome is not recorded: fail closed.
      if (persistError.code !== 'IDEMPOTENCY_RECORD_CHANGED') {
        const unknown = unknownRecord(started, succeeded ? 'COMPLETION_PERSISTENCE_FAILED' : 'FAILURE_PERSISTENCE_FAILED',
          this.clock().toISOString());
        try { await this.#replaceOwn(started, unknown); }
        catch (error) { if (error.code !== 'IDEMPOTENCY_RECORD_CHANGED') this.#retryUnknown(started, unknown); }
      }
      throw metadataError(succeeded ? persistError : error, 'IDEMPOTENCY_UNKNOWN', key);
    }
    if (!succeeded) throw metadataError(error, noEffect ? 'IDEMPOTENCY_FAILED' : 'IDEMPOTENCY_UNKNOWN', key, noEffect);
    return { status: 'IDEMPOTENCY_EXECUTED', key, executed: true, result: copy(result) };
  }

  // Replace the record only while it is still this execution's STARTED record.
  #replaceOwn(started, record) {
    return this.#lock.run(async () => {
      const current = await this.#read(started.key);
      if (current?.state !== 'STARTED' || current.owner?.executionId !== started.owner.executionId) {
        throw Object.assign(new Error('The record changed while the effect ran'), { code: 'IDEMPOTENCY_RECORD_CHANGED' });
      }
      await this.#write(record);
    });
  }

  // Other processes judge a STARTED record by its owner's PID, which stays alive
  // after this execution ends. Until UNKNOWN is written they would report the key
  // IN_PROGRESS for this process's lifetime, so keep trying in the background.
  // Stops once written, once the record is no longer ours, if the record is
  // unreadable (an operator must act), or when the process exits (timers are
  // unref'd; a dead owner is then detected by other processes).
  #retryUnknown(started, unknown, attempt = 0) {
    const timer = setTimeout(() => {
      this.#replaceOwn(started, unknown).catch(error => {
        if (!['IDEMPOTENCY_RECORD_CHANGED', 'IDEMPOTENCY_STATE_INVALID'].includes(error.code)) {
          this.#retryUnknown(started, unknown, attempt + 1);
        }
      });
    }, Math.min(25 * 2 ** attempt, 2_000));
    timer.unref?.();
  }

  /**
   * Resolve an UNKNOWN (or STARTED, dead-owner) record with authoritative evidence.
   *
   * A STARTED record whose owner cannot be proven dead requires an override.
   * Pass expectedExecutionId from inspect(key) and explicitAuthorization: true.
   * The override applies only to that execution and is refused when the calling
   * process is still running it. An overridden owner's late outcome write is
   * refused and it reports UNKNOWN.
   */
  async reconcile({ key, request = {}, disposition, result = null, evidence, expectedExecutionId,
    explicitAuthorization = false } = {}) {
    key = requiredString(key, 'key');
    const override = expectedExecutionId !== undefined;
    if (override) {
      if (explicitAuthorization !== true) throw new Error('Explicit authorization required to override a running or unverified owner');
      if (typeof expectedExecutionId !== 'string' || !expectedExecutionId) throw new TypeError('Invalid expectedExecutionId');
    }
    if (this.#inflight.has(key)) throw new Error('Cannot reconcile while the operation is running');
    return this.#lock.run(async () => {
      const record = await this.#read(key);
      const observedOwnerState = record?.state === 'STARTED' && record.fingerprint === requestFingerprint(request)
        ? ownerState(record.owner) : null;
      const ownerNotProvablyDead = observedOwnerState !== null && observedOwnerState !== 'DEAD';
      if (override && record) {
        if (record.owner?.executionId !== expectedExecutionId) {
          throw Object.assign(new Error('The record changed since it was inspected; inspect it again'),
            { code: 'IDEMPOTENCY_RECORD_CHANGED', key });
        }
        if (observedOwnerState === 'ALIVE' && record.owner.pid === process.pid && record.owner.host === hostname()) {
          throw new Error('Cannot reconcile while the operation is running'); // certain: it runs in this process
        }
      } else if (ownerNotProvablyDead) {
        if (observedOwnerState === 'ALIVE') throw new Error('Cannot reconcile while the operation is running');
        throw Object.assign(new Error('Cannot reconcile while the operation owner cannot be verified; inspect it before requesting an override'),
          { code: 'IDEMPOTENCY_OWNER_UNVERIFIED', ownerState: observedOwnerState });
      }
      const refusal = reconciliationRefusal(record, request, evidence, key);
      if (refusal) return refusal;
      const transition = reconciliationTransition(record, disposition, result, evidence, this.clock().toISOString(), key);
      if (!transition.record) return transition.response;
      const audited = ownerNotProvablyDead
        ? { ...transition.record, liveOwnerOverride: { executionId: expectedExecutionId, owner: copy(record.owner),
          at: this.clock().toISOString() } }
        : transition.record;
      await this.#write(audited);
      return transition.response;
    });
  }

  /** Read-only view of one key's record, or null. Never takes the lock. */
  async inspect(key) {
    const record = await this.#read(requiredString(key, 'key'));
    if (!record) return null;
    const view = copy(record);
    if (record.state === 'STARTED') view.ownerState = ownerState(record.owner);
    return view;
  }

  async snapshot() {
    const states = {};
    let count = 0;
    let names = [];
    try { names = await readdir(this.directory); } catch (error) { if (error.code !== 'ENOENT') throw error; }
    for (const name of names) {
      if (!RECORD_FILE.test(name)) continue;
      const record = await this.#readFile(path.join(this.directory, name));
      if (!record) continue;
      count++;
      states[record.state] = (states[record.state] ?? 0) + 1;
    }
    return { schema: 'mnemos.idempotency.v2', count, states, coordination: 'same-host-file-lock' };
  }

  inspectLock() { return this.#lock.inspect(); }
  forceUnlock(request) { return this.#lock.forceUnlock(request); }

  #file(key) {
    return path.join(this.directory, `${createHash('sha256').update(key).digest('hex')}.record.json`);
  }

  async #read(key) {
    const record = await this.#readFile(this.#file(key));
    if (record && record.key !== key) throw stateError('Idempotency record belongs to another key', { key });
    return record;
  }

  // Never treats an unreadable record as absent: that would run the effect again.
  async #readFile(file) {
    let text;
    try {
      const handle = await open(file, 'r');
      try {
        if ((await handle.stat()).size > MAX_RECORD_BYTES) throw stateError('Idempotency record exceeds 1 MiB', { file });
        text = await handle.readFile('utf8');
      } finally { await handle.close(); }
    } catch (error) {
      if (error.code === 'ENOENT') return null;
      throw error;
    }
    let stored;
    try { stored = JSON.parse(text); }
    catch { throw stateError('Idempotency record is not valid JSON; reconciliation required', { file }); }
    const record = stored?.record;
    if (stored?.schema !== RECORD_SCHEMA || typeof record?.key !== 'string' || !/^[0-9a-f]{64}$/.test(record.fingerprint) ||
      !STATES.has(record.state) || !Number.isSafeInteger(record.attempt) || record.attempt < 1 ||
      (record.state === 'STARTED' && typeof record.owner?.executionId !== 'string')) {
      throw stateError('Idempotency record is invalid; reconciliation required', { file });
    }
    return record;
  }

  async #write(record) {
    const text = JSON.stringify({ schema: RECORD_SCHEMA, record }, null, 2) + '\n';
    if (Buffer.byteLength(text) > MAX_RECORD_BYTES) throw new RangeError('Idempotency record exceeds 1 MiB');
    await replaceFile(this.#file(record.key), text, '.record.tmp');
  }
}
