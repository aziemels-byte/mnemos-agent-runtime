import { randomUUID } from 'node:crypto';
import { open } from 'node:fs/promises';
import path from 'node:path';
import { DirectoryLock, replaceFile } from './directory-lock.mjs';

const copy = value => structuredClone(value);
const resourceId = value => /^(gpu:\d+|cpu:[a-z0-9._-]+|workspace:[a-z0-9._-]+|capability:[a-z0-9._-]+)$/i.test(value);
// IDs are matched case-insensitively, so they must also be compared that way:
// otherwise 'gpu:0' and 'GPU:0' would be granted as two exclusive resources.
const normalizeResource = value => typeof value === 'string' ? value.toLowerCase() : value;
function authorize(node, computeAuthorized) {
  const control = node.resourceControl;
  if (!control) return;
  if (['OFFLINE', 'QUARANTINED'].includes(control.allocationState) || control.computeAccess === 'DENIED') {
    throw new Error('Resource control denies execution');
  }
  if (control.computeAccess === 'EXPLICIT_AUTHORIZED' && !computeAuthorized) {
    throw new Error('Explicit compute authorization required');
  }
}

/**
 * Single-process exclusive leases (see FileResourceLeaseManager for same-host
 * cross-process coordination). This registry authorizes neither remote
 * compute nor OS process execution; caller-owned execution remains separate.
 */
export class ResourceLeaseManager {
  constructor({ nodeRegistry, clock = () => new Date(), createId = randomUUID, maxTtlMs = 3_600_000 } = {}) {
    if (!nodeRegistry || typeof nodeRegistry.require !== 'function') throw new TypeError('nodeRegistry.require is required');
    this.nodeRegistry = nodeRegistry;
    this.clock = clock;
    this.createId = createId;
    this.maxTtlMs = maxTtlMs;
    this.leases = new Map();
    // Highest fencing token issued. It only increases, including across releases.
    this.lastFencingToken = 0;
  }

  reclaimExpired() {
    const now = this.clock().getTime();
    for (const [id, lease] of this.leases) if (Date.parse(lease.expiresAt) <= now) this.leases.delete(id);
  }

  eligible(nodeId, computeAuthorized) {
    const node = this.nodeRegistry.require(nodeId);
    if (node.trust !== 'VERIFIED' || node.status !== 'AVAILABLE') throw new Error('Node is not execution eligible');
    authorize(node, computeAuthorized);
  }

  duration(ttlMs) {
    const duration = Number(ttlMs);
    if (!Number.isFinite(duration) || duration <= 0 || duration > this.maxTtlMs) throw new RangeError('Invalid lease TTL');
    return duration;
  }

  acquire({ ownerId, nodeId, resources, ttlMs = 300_000, explicitAuthorization = false, computeAuthorized = false } = {}) {
    this.reclaimExpired();
    if (explicitAuthorization !== true) throw new Error('Explicit lease authorization required');
    if (!/^[a-z0-9._:-]+$/i.test(String(ownerId ?? ''))) throw new TypeError('Invalid ownerId');
    if (typeof nodeId !== 'string' || !nodeId) throw new TypeError('Invalid nodeId');
    this.eligible(nodeId, computeAuthorized);
    if (!Array.isArray(resources)) throw new TypeError('resources must be an array');
    const normalized = [...new Set(resources.map(normalizeResource))].sort();
    if (!normalized.length || normalized.some(r => typeof r !== 'string' || !resourceId(r))) {
      throw new TypeError('Unsupported resource ID');
    }
    const duration = this.duration(ttlMs);
    const conflict = [...this.leases.values()].find(l => l.nodeId === nodeId && l.resources.some(r => normalized.includes(r)));
    if (conflict) return { status: 'LEASE_UNAVAILABLE', conflictingLeaseId: conflict.leaseId, resources: normalized };
    const fencingToken = this.lastFencingToken + 1;
    if (!Number.isSafeInteger(fencingToken)) throw new RangeError('Fencing token space exhausted');
    const now = this.clock();
    const lease = { schema: 'mnemos.resource-lease.v1', leaseId: 'lease:' + this.createId(), ownerId,
      nodeId, resources: normalized, mode: 'exclusive', fencingToken, acquiredAt: now.toISOString(),
      expiresAt: new Date(now.getTime() + duration).toISOString() };
    if (this.leases.has(lease.leaseId)) throw new Error('Duplicate generated lease ID');
    this.leases.set(lease.leaseId, lease);
    this.lastFencingToken = fencingToken;
    return { status: 'LEASE_ACQUIRED', lease: copy(lease) };
  }

  require(leaseId) {
    this.reclaimExpired();
    const lease = this.leases.get(leaseId);
    if (!lease) throw new Error('Unknown or expired lease');
    return copy(lease);
  }

  renew({ leaseId, ownerId, ttlMs = 300_000, explicitAuthorization = false, computeAuthorized = false } = {}) {
    if (explicitAuthorization !== true) throw new Error('Explicit lease authorization required');
    const lease = this.require(leaseId);
    if (lease.ownerId !== ownerId) throw new Error('Lease owner mismatch');
    this.eligible(lease.nodeId, computeAuthorized);
    const renewed = { ...lease, expiresAt: new Date(this.clock().getTime() + this.duration(ttlMs)).toISOString() };
    this.leases.set(leaseId, renewed);
    return { status: 'LEASE_RENEWED', lease: copy(renewed) };
  }

  release({ leaseId, ownerId, explicitAuthorization = false } = {}) {
    if (explicitAuthorization !== true) throw new Error('Explicit lease authorization required');
    const lease = this.require(leaseId);
    if (lease.ownerId !== ownerId) throw new Error('Lease owner mismatch');
    this.leases.delete(leaseId);
    return { status: 'LEASE_RELEASED', lease: copy(lease) };
  }

  validateExecutionLease({ leaseId, ownerId, nodeId, resource, computeAuthorized = false } = {}) {
    const lease = this.require(leaseId);
    if (lease.ownerId !== ownerId) throw new Error('Lease owner mismatch');
    if (lease.nodeId !== nodeId) throw new Error('Lease node mismatch');
    if (!lease.resources.includes(normalizeResource(resource))) throw new Error('Lease does not cover resource');
    this.eligible(nodeId, computeAuthorized);
    return copy(lease);
  }

  list() {
    this.reclaimExpired();
    return [...this.leases.values()].map(copy).sort((a, b) => a.leaseId.localeCompare(b.leaseId));
  }

  snapshot() {
    return { schema: 'mnemos.resource-leases.v1', leases: this.list(), lastFencingToken: this.lastFencingToken,
      policy: { exclusiveOnly: true, automaticAcquire: false, explicitAuthorizationRequired: true } };
  }
}

/**
 * Renews a lease on a timer and reports loss through an AbortSignal. The lease
 * counts as lost when a renewal fails or when the local deadline passes. The
 * deadline is measured from the start of the last successful renewal request,
 * minus a safety margin, so a hung renewal cannot extend it. It never
 * reacquires. The signal is advisory: a paused holder cannot observe it, so
 * writes must still be fenced (see FencingGuard).
 * Works with ResourceLeaseManager and FileResourceLeaseManager.
 */
export function keepLeaseAlive({ manager, lease, ownerId, ttlMs = 300_000, renewEveryMs, safetyMarginMs,
  explicitAuthorization = false, computeAuthorized = false, onLost = null, clock = () => new Date() } = {}) {
  if (explicitAuthorization !== true) throw new Error('Explicit lease authorization required');
  if (typeof manager?.renew !== 'function' || typeof manager?.release !== 'function') {
    throw new TypeError('manager.renew and manager.release are required');
  }
  const expiresAt = Date.parse(lease?.expiresAt);
  if (typeof lease?.leaseId !== 'string' || !Number.isFinite(expiresAt)) throw new TypeError('A lease is required');
  if (typeof ownerId !== 'string' || ownerId !== lease.ownerId) throw new Error('Lease owner mismatch');
  if (onLost !== null && typeof onLost !== 'function') throw new TypeError('onLost must be a function');
  const ttl = Number(ttlMs);
  const margin = safetyMarginMs ?? Math.floor(ttl / 10);
  const every = renewEveryMs ?? Math.floor(ttl / 3);
  if (!Number.isFinite(ttl) || ttl <= 0) throw new RangeError('Invalid lease TTL');
  if (!Number.isFinite(margin) || margin < 0 || !Number.isFinite(every) || every <= 0 || every >= ttl - margin) {
    throw new RangeError('renewEveryMs must be positive and shorter than ttlMs minus safetyMarginMs');
  }

  const controller = new AbortController();
  const request = { leaseId: lease.leaseId, ownerId, explicitAuthorization: true };
  let current = copy(lease);
  let state = 'HOLDING';
  let renewTimer = null;
  let deadlineTimer = null;
  let inflight = Promise.resolve();
  let deadline = expiresAt - margin;

  const clearTimers = () => { clearTimeout(renewTimer); clearTimeout(deadlineTimer); };
  const lose = (code, message, cause) => {
    if (state !== 'HOLDING') return;
    state = 'LOST';
    clearTimers();
    const reason = Object.assign(new Error(message, { cause }), { code, leaseId: lease.leaseId });
    controller.abort(reason);
    // A failing callback must not hide the loss; the signal is already aborted.
    if (onLost) Promise.resolve().then(() => onLost(reason)).catch(() => {});
  };
  const loseAtDeadline = () => lose('LEASE_DEADLINE_PASSED', 'Lease deadline passed before renewal succeeded');
  const armDeadline = () => {
    clearTimeout(deadlineTimer);
    const remaining = deadline - clock().getTime();
    if (remaining <= 0) return loseAtDeadline();
    deadlineTimer = setTimeout(loseAtDeadline, remaining);
  };
  const renew = () => {
    if (state !== 'HOLDING') return;
    const startedAt = clock().getTime();
    // A paused event loop can resume a renewal before it runs the overdue deadline timer.
    if (startedAt >= deadline) return loseAtDeadline();
    inflight = (async () => {
      try {
        const result = await manager.renew({ ...request, ttlMs: ttl, computeAuthorized });
        if (state !== 'HOLDING') return;
        if (clock().getTime() >= deadline) return loseAtDeadline();
        current = copy(result.lease);
        deadline = Math.min(startedAt + ttl, Date.parse(result.lease.expiresAt)) - margin;
        armDeadline();
        if (state === 'HOLDING') renewTimer = setTimeout(renew, every);
      } catch (error) {
        lose('LEASE_RENEWAL_FAILED', 'Lease renewal failed; treat the lease as lost', error);
      }
    })();
  };

  armDeadline();
  if (state === 'HOLDING') renewTimer = setTimeout(renew, every);

  return {
    signal: controller.signal,
    get state() { return state; },
    get lease() { return copy(current); },
    async stop({ release = false } = {}) {
      const wasHolding = state === 'HOLDING';
      if (wasHolding) state = 'STOPPED';
      clearTimers();
      await inflight;
      if (!release) return { status: wasHolding ? 'KEEPER_STOPPED' : 'LEASE_LOST' };
      try {
        return await manager.release(request);
      } catch (error) {
        // After a loss the lease may already be gone; that is not a new failure.
        if (!wasHolding) return { status: 'LEASE_LOST' };
        throw error;
      }
    }
  };
}

/**
 * Enforces fencing tokens at the protected resource. A lease can expire while
 * its holder is paused, so a lease check made before acting is not enough: the
 * resource owner must call admit() and apply the write in the same synchronous
 * step (no await in between). Tokens equal to the highest seen are admitted,
 * older tokens are rejected. State is in memory: after a restart, construct the
 * guard with floor = the lease manager's lastFencingToken (fails closed: older
 * holders must reacquire).
 */
export class FencingGuard {
  #highest = new Map();

  constructor({ floor = 0 } = {}) {
    if (!Number.isSafeInteger(floor) || floor < 0) throw new RangeError('Invalid fencing floor');
    this.floor = floor;
  }

  admit(key, fencingToken) {
    if (typeof key !== 'string' || !key) throw new TypeError('A fencing key is required');
    if (!Number.isSafeInteger(fencingToken) || fencingToken <= 0) throw new TypeError('Invalid fencing token');
    const highestToken = this.highest(key);
    if (fencingToken < highestToken) {
      throw Object.assign(new Error('Stale fencing token; a newer lease holder has acted'),
        { code: 'FENCE_STALE', key, fencingToken, highestToken });
    }
    this.#highest.set(key, fencingToken);
    return { status: 'FENCE_ACCEPTED', key, fencingToken };
  }

  highest(key) {
    return this.#highest.get(key) ?? this.floor;
  }
}

const STATE_SCHEMA = 'mnemos.resource-lease-state.v1';
const STATE_TEMPORARY = /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}\.state\.tmp$/;
const MAX_STATE_BYTES = 1024 * 1024;

function validStoredLease(lease) {
  return lease?.schema === 'mnemos.resource-lease.v1' && typeof lease.leaseId === 'string' &&
    typeof lease.ownerId === 'string' && typeof lease.nodeId === 'string' && lease.mode === 'exclusive' &&
    Array.isArray(lease.resources) && lease.resources.length > 0 &&
    lease.resources.every(r => typeof r === 'string' && resourceId(r) && r === normalizeResource(r)) &&
    Number.isSafeInteger(lease.fencingToken) && lease.fencingToken > 0 &&
    Number.isFinite(Date.parse(lease.acquiredAt)) && Number.isFinite(Date.parse(lease.expiresAt));
}

function stateError(message, cause) {
  return Object.assign(new Error(message, { cause }), { code: 'LEASE_STATE_INVALID' });
}

/**
 * Exclusive leases shared by processes on one host through a state directory.
 * Every operation is a transaction: take the directory lock, load the state,
 * apply the same policy as ResourceLeaseManager, atomically replace the state
 * and unlock. Requires a local filesystem with atomic exclusive hard links; it
 * is not a multi-host or network-filesystem lock.
 */
export class FileResourceLeaseManager {
  #lock;

  constructor({ directory, nodeRegistry, clock = () => new Date(), createId = randomUUID,
    maxTtlMs = 3_600_000, lockTimeoutMs = 5_000 } = {}) {
    if (typeof directory !== 'string' || !directory.trim()) throw new TypeError('A lease state directory is required');
    if (!nodeRegistry || typeof nodeRegistry.require !== 'function') throw new TypeError('nodeRegistry.require is required');
    if (!Number.isFinite(lockTimeoutMs) || lockTimeoutMs < 0) throw new RangeError('Invalid lockTimeoutMs');
    this.directory = path.resolve(directory);
    this.stateFile = path.join(this.directory, 'leases.json');
    this.options = { nodeRegistry, clock, createId, maxTtlMs };
    this.lockTimeoutMs = lockTimeoutMs;
    this.#lock = new DirectoryLock({ directory: this.directory, name: 'leases.lock', timeoutMs: lockTimeoutMs,
      timeoutCode: 'LEASE_LOCK_TIMEOUT', ownedTemporary: name => STATE_TEMPORARY.test(name) });
    this.lockFile = this.#lock.lockFile;
  }

  acquire(request) {
    return this.#transaction(manager => {
      const ownerId = request?.ownerId;
      if (typeof ownerId !== 'string') throw new TypeError('Invalid ownerId');
      return manager.acquire({ ...request, ownerId });
    });
  }
  require(leaseId) { return this.#transaction(manager => manager.require(leaseId)); }
  renew(request) { return this.#transaction(manager => manager.renew(request)); }
  release(request) { return this.#transaction(manager => manager.release(request)); }
  validateExecutionLease(request) { return this.#transaction(manager => manager.validateExecutionLease(request)); }
  list() { return this.#transaction(manager => manager.list()); }

  async snapshot() {
    const snapshot = await this.#transaction(manager => manager.snapshot());
    snapshot.policy.coordination = 'same-host-file-lock';
    return snapshot;
  }

  /** Read-only view of the directory lock for operators; never takes the lock. */
  inspectLock() { return this.#lock.inspect(); }

  /** Operator override; see DirectoryLock.forceUnlock and the README runbook. */
  forceUnlock(request) { return this.#lock.forceUnlock(request); }

  #transaction(operation) {
    return this.#lock.run(async () => {
      const stored = await this.#load();
      const manager = new ResourceLeaseManager(this.options);
      for (const lease of stored.leases) manager.leases.set(lease.leaseId, lease);
      manager.lastFencingToken = stored.lastFencingToken;
      const before = JSON.stringify(stored);
      const result = operation(manager);
      const next = { lastFencingToken: manager.lastFencingToken, leases: [...manager.leases.values()] };
      if (JSON.stringify(next) !== before) await this.#save(next);
      return result;
    });
  }

  async #load() {
    let text;
    try {
      const file = await open(this.stateFile, 'r');
      try {
        if ((await file.stat()).size > MAX_STATE_BYTES) throw stateError('Lease state exceeds 1 MiB');
        text = await file.readFile('utf8');
      } finally { await file.close(); }
    } catch (error) {
      if (error.code === 'ENOENT') return { lastFencingToken: 0, leases: [] };
      throw error;
    }
    // Never treat unreadable state as empty: that would re-grant held resources.
    let value;
    try { value = JSON.parse(text); }
    catch (error) { throw stateError('Lease state is not valid JSON; manual reconciliation required', error); }
    const last = value?.lastFencingToken;
    if (value?.schema !== STATE_SCHEMA || !Number.isSafeInteger(last) || last < 0 ||
      !Array.isArray(value.leases) || !value.leases.every(validStoredLease)) {
      throw stateError('Lease state is invalid; manual reconciliation required');
    }
    // A token above the counter would be issued twice; a repeated token or ID is ambiguous.
    const tokens = value.leases.map(lease => lease.fencingToken);
    if (tokens.some(token => token > last) || new Set(tokens).size !== tokens.length ||
      new Set(value.leases.map(lease => lease.leaseId)).size !== value.leases.length) {
      throw stateError('Lease state has inconsistent lease IDs or fencing tokens; manual reconciliation required');
    }
    return { lastFencingToken: last, leases: value.leases };
  }

  async #save({ lastFencingToken, leases }) {
    const text = JSON.stringify({ schema: STATE_SCHEMA, lastFencingToken, leases }, null, 2) + '\n';
    if (Buffer.byteLength(text) > MAX_STATE_BYTES) throw new RangeError('Lease state exceeds 1 MiB');
    await replaceFile(this.stateFile, text, '.state.tmp');
  }
}
