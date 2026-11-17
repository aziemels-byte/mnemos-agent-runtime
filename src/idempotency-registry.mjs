import { createHash, randomUUID } from 'node:crypto';
import { readdir } from 'node:fs/promises';
import { hostname } from 'node:os';
import path from 'node:path';
import { DirectoryLock, readBoundedFile, replaceFile, retryLater } from './directory-lock.mjs';
import { executionOwner, isExecutionLive, markExecutionEnded, markExecutionLive, ownerState } from './process-identity.mjs';

const copy = value => structuredClone(value);
function requiredString(value, label) {
  if (typeof value !== 'string' || !value.trim()) throw new TypeError(label + ' must be a non-empty string');
  return value.trim();
}
const sha256 = text => createHash('sha256').update(text).digest('hex');
// JSON.stringify with the keys of every object sorted: requests that differ only
// in key order are the same request. Accepts what JSON.stringify accepts.
const sortedKeys = (key, value) => value !== null && typeof value === 'object' && !Array.isArray(value)
  ? Object.fromEntries(Object.keys(value).sort().map(name => [name, value[name]])) : value;

/**
 * A request's fingerprint (key order does not matter), and the fingerprint
 * earlier versions stored (key order mattered). Treat the request as immutable.
 */
function requestIdentity(request) {
  const value = request ?? {};
  return { fingerprint: sha256(JSON.stringify(value, sortedKeys)), legacy: sha256(JSON.stringify(value)) };
}

// Whether a record was made for this request. A record of an earlier version
// stores the legacy fingerprint: both are serializations of the same request, so
// either one matching proves it is the same request.
const sameRequest = (record, identity) => record.fingerprint === identity.fingerprint || record.fingerprint === identity.legacy;

function metadataError(error, status, key, retryAllowed = false) {
  const reported = error instanceof Error ? error : new Error(String(error));
  reported.idempotency = { status, key, retryAllowed, reconciliationRequired: status === 'IDEMPOTENCY_UNKNOWN' };
  return reported;
}

// Decision rules shared by the in-memory and the file-backed registry.

/** Response for an existing record, or null when the effect may run (again). */
function existingOutcome(previous, identity, retryFailed, key) {
  if (!previous) return null;
  if (!sameRequest(previous, identity)) return { status: 'IDEMPOTENCY_CONFLICT', key, executed: false };
  if (previous.state === 'COMPLETED') return { status: 'IDEMPOTENT_REPLAY', key, executed: false, result: copy(previous.result) };
  if (previous.state === 'UNKNOWN' || previous.state === 'STARTED') {
    return { status: 'IDEMPOTENCY_UNKNOWN', key, executed: false, reconciliationRequired: true };
  }
  if (previous.state === 'FAILED' && !(retryFailed && previous.effectAbsent === true)) {
    return { status: 'IDEMPOTENCY_FAILED', key, executed: false, retryAllowed: previous.effectAbsent === true };
  }
  return null;
}

/**
 * Whether a failure certainly had no effect. failureProvesNoEffect is true, or
 * a function of the error that returns true only for such a failure; anything
 * else, including a function that throws, means the effect may have happened.
 */
function provesNoEffect(failureProvesNoEffect, error) {
  if (typeof failureProvesNoEffect !== 'function') return failureProvesNoEffect === true;
  try { return failureProvesNoEffect(error) === true; } catch { return false; }
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
  if (!sameRequest(record, requestIdentity(request))) return { status: 'IDEMPOTENCY_CONFLICT', key, reconciled: false };
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
    const identity = requestIdentity(request);
    const { fingerprint } = identity;
    const active = this.inflight.get(key);
    if (active) {
      if (active.fingerprint !== fingerprint) return { status: 'IDEMPOTENCY_CONFLICT', key, executed: false };
      return active.promise; // one effect, multiple waiters
    }
    const previous = this.records.get(key);
    const outcome = existingOutcome(previous, identity, retryFailed, key);
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
      const noEffect = provesNoEffect(failureProvesNoEffect, error);
      const failed = failureRecord(started, noEffect, this.clock().toISOString());
      this.records.set(started.key, failed); // never retry silently on a failed journal write
      try {
        await this.persist(noEffect ? 'idempotency.failed' : 'idempotency.unknown', failed);
      } catch {
        // The journal still says STARTED, which restore() turns into UNKNOWN: say so
        // here too, with the operation's own error rather than the journal's.
        this.records.set(started.key, unknownRecord(started, 'FAILURE_PERSISTENCE_FAILED', this.clock().toISOString()));
        throw metadataError(error, 'IDEMPOTENCY_UNKNOWN', started.key);
      }
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
// Writes over this execution's own record that will not succeed by trying again.
const FINAL_WRITE_ERRORS = new Set(['IDEMPOTENCY_RECORD_CHANGED', 'IDEMPOTENCY_STATE_INVALID']);

function stateError(message, details = {}) {
  return Object.assign(new Error(message), { code: 'IDEMPOTENCY_STATE_INVALID', ...details });
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
    const identity = requestIdentity(request);
    const active = this.#inflight.get(key);
    if (active) {
      if (active.fingerprint !== identity.fingerprint) return { status: 'IDEMPOTENCY_CONFLICT', key, executed: false };
      return active.promise; // one effect, multiple waiters in this instance
    }
    const promise = Promise.resolve().then(() => this.#run(key, identity, operation, retryFailed, failureProvesNoEffect));
    this.#inflight.set(key, { fingerprint: identity.fingerprint, promise });
    try { return await promise; }
    finally { if (this.#inflight.get(key)?.promise === promise) this.#inflight.delete(key); }
  }

  async #run(key, identity, operation, retryFailed, failureProvesNoEffect) {
    const executionId = randomUUID();
    const reservation = await this.#lock.run(async () => {
      let previous = await this.#read(key);
      if (previous?.state === 'STARTED' && sameRequest(previous, identity)) {
        if (ownerState(previous.owner) !== 'DEAD') return { response: { status: 'IDEMPOTENCY_IN_PROGRESS', key, executed: false } };
        previous = unknownRecord(previous, 'INTERRUPTED_AFTER_EFFECT_START', this.clock().toISOString());
        await this.#write(previous);
      }
      const outcome = existingOutcome(previous, identity, retryFailed, key);
      if (outcome) return { response: outcome };
      const started = { key, fingerprint: identity.fingerprint, state: 'STARTED', startedAt: this.clock().toISOString(),
        attempt: (previous?.attempt ?? 0) + 1, owner: executionOwner(executionId) };
      // Listed before the record becomes visible, so this process never sees it as dead.
      markExecutionLive(executionId);
      try { await this.#write(started); }
      catch (error) { markExecutionEnded(executionId); throw error; }
      return { started };
    });
    if (reservation.response) return reservation.response;

    const { started } = reservation;
    try {
      let outcome;
      try { outcome = { succeeded: true, result: await operation() }; }
      catch (error) { outcome = { succeeded: false, error }; }
      return await this.#finish(started, outcome, !outcome.succeeded && provesNoEffect(failureProvesNoEffect, outcome.error));
    } finally {
      markExecutionEnded(executionId);
    }
  }

  async #finish(started, { succeeded, result, error }, noEffect) {
    const key = started.key;
    const now = this.clock().toISOString();
    let outcome;
    try {
      outcome = succeeded
        ? { ...started, state: 'COMPLETED', completedAt: now, result: copy(result), reconciliationRequired: false }
        : failureRecord(started, noEffect, now);
      this.#encode(outcome);
    } catch (unrecordable) {
      // A result that is not JSON, or too large, can never be recorded. The
      // effect happened (or may have): record UNKNOWN instead.
      await this.#recordOwn(started, unknownRecord(started, succeeded ? 'COMPLETION_PERSISTENCE_FAILED' : 'FAILURE_PERSISTENCE_FAILED', now));
      throw metadataError(succeeded ? unrecordable : error, 'IDEMPOTENCY_UNKNOWN', key);
    }
    const persistError = await this.#recordOwn(started, outcome);
    // Not recorded (yet): it is retried in the background, or someone else decided. Uncertain here.
    if (persistError) throw metadataError(succeeded ? persistError : error, 'IDEMPOTENCY_UNKNOWN', key);
    if (!succeeded) throw metadataError(error, noEffect ? 'IDEMPOTENCY_FAILED' : 'IDEMPOTENCY_UNKNOWN', key, noEffect);
    return { status: 'IDEMPOTENCY_EXECUTED', key, executed: true, result: copy(result) };
  }

  /**
   * Record what this execution knows over its own STARTED record. Resolves to
   * null once written, or to the error. A write that may succeed later is
   * retried in the background; one refused because the record is no longer
   * ours (IDEMPOTENCY_RECORD_CHANGED) or is unreadable is not.
   */
  async #recordOwn(started, record) {
    try {
      await this.#replaceOwn(started, record);
      return null;
    } catch (error) {
      if (!FINAL_WRITE_ERRORS.has(error.code)) this.#retryOwn(started, record);
      return error;
    }
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
  // after this execution ends. Until the outcome this execution knows is written
  // they would report the key IN_PROGRESS for this process's lifetime, so keep
  // trying in the background. Each attempt writes only over this execution's own
  // STARTED record: it stops once written, once the record is no longer ours (in
  // this thread a later call already turned it UNKNOWN, which then stands), if
  // the record is unreadable (an operator must act), or when the process exits
  // (timers are unref'd; a dead owner is then detected by other processes).
  #retryOwn(started, record, attempt = 0) {
    retryLater(attempt, () => this.#replaceOwn(started, record).catch(error => {
      if (!FINAL_WRITE_ERRORS.has(error.code)) this.#retryOwn(started, record, attempt + 1);
    }));
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
      const observedOwnerState = record?.state === 'STARTED' && sameRequest(record, requestIdentity(request))
        ? ownerState(record.owner) : null;
      const ownerNotProvablyDead = observedOwnerState !== null && observedOwnerState !== 'DEAD';
      if (override && record) {
        if (record.owner?.executionId !== expectedExecutionId) {
          throw Object.assign(new Error('The record changed since it was inspected; inspect it again'),
            { code: 'IDEMPOTENCY_RECORD_CHANGED', key });
        }
        if (observedOwnerState === 'ALIVE' && record.owner.pid === process.pid && record.owner.host === hostname() &&
          isExecutionLive(record.owner.executionId)) {
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
    const bytes = await readBoundedFile(file, MAX_RECORD_BYTES, reason =>
      stateError(reason === 'TOO_LARGE' ? 'Idempotency record exceeds 1 MiB' : 'Idempotency record is not a file', { file }));
    if (bytes === null) return null;
    const text = bytes.toString('utf8');
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

  // The stored text of a record; throws for a result that is not JSON or too large.
  #encode(record) {
    const text = JSON.stringify({ schema: RECORD_SCHEMA, record }, null, 2) + '\n';
    if (Buffer.byteLength(text) > MAX_RECORD_BYTES) throw new RangeError('Idempotency record exceeds 1 MiB');
    return text;
  }

  async #write(record) {
    await replaceFile(this.#file(record.key), this.#encode(record), '.record.tmp', { syncDirectory: true });
  }
}
