import { randomUUID } from 'node:crypto';

const copy = value => structuredClone(value);
const resourceId = value => /^(gpu:\d+|cpu:[a-z0-9._-]+|workspace:[a-z0-9._-]+|capability:[a-z0-9._-]+)$/i.test(value);
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
 * Single-process exclusive leases. This registry authorizes neither remote
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
    const normalized = [...new Set(resources)].sort();
    if (!normalized.length || normalized.some(r => typeof r !== 'string' || !resourceId(r))) {
      throw new TypeError('Unsupported resource ID');
    }
    const duration = this.duration(ttlMs);
    const conflict = [...this.leases.values()].find(l => l.nodeId === nodeId && l.resources.some(r => normalized.includes(r)));
    if (conflict) return { status: 'LEASE_UNAVAILABLE', conflictingLeaseId: conflict.leaseId, resources: normalized };
    const now = this.clock();
    const lease = { schema: 'mnemos.resource-lease.v1', leaseId: 'lease:' + this.createId(), ownerId,
      nodeId, resources: normalized, mode: 'exclusive', acquiredAt: now.toISOString(),
      expiresAt: new Date(now.getTime() + duration).toISOString() };
    if (this.leases.has(lease.leaseId)) throw new Error('Duplicate generated lease ID');
    this.leases.set(lease.leaseId, lease);
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
    if (!lease.resources.includes(resource)) throw new Error('Lease does not cover resource');
    this.eligible(nodeId, computeAuthorized);
    return copy(lease);
  }

  list() {
    this.reclaimExpired();
    return [...this.leases.values()].map(copy).sort((a, b) => a.leaseId.localeCompare(b.leaseId));
  }

  snapshot() {
    return { schema: 'mnemos.resource-leases.v1', leases: this.list(),
      policy: { exclusiveOnly: true, automaticAcquire: false, explicitAuthorizationRequired: true } };
  }
}
