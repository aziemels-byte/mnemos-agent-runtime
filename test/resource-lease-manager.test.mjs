import test from 'node:test';
import assert from 'node:assert/strict';
import { ResourceLeaseManager } from '../src/index.mjs';

const nodeRegistry = (extra = {}) => ({
  require(id) {
    const node = extra[id] ?? (id === 'local' ? { trust: 'VERIFIED', status: 'AVAILABLE' } : null);
    if (!node) throw Error('unknown node');
    return node;
  }
});

test('exclusive resources cannot be leased twice on the same node', () => {
  const manager = new ResourceLeaseManager({ nodeRegistry: nodeRegistry(), createId: (() => { let n = 0; return () => String(++n); })() });
  const first = manager.acquire({ ownerId: 'task:one', nodeId: 'local', resources: ['gpu:0', 'workspace:build'], explicitAuthorization: true });
  assert.equal(first.status, 'LEASE_ACQUIRED');
  assert.deepEqual(first.lease.resources, ['gpu:0', 'workspace:build']);
  assert.equal(manager.acquire({ ownerId: 'task:two', nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true }).status, 'LEASE_UNAVAILABLE');
  assert.throws(() => manager.release({ leaseId: first.lease.leaseId, ownerId: 'wrong', explicitAuthorization: true }), /owner/);
  assert.equal(manager.release({ leaseId: first.lease.leaseId, ownerId: 'task:one', explicitAuthorization: true }).status, 'LEASE_RELEASED');
  assert.equal(manager.acquire({ ownerId: 'task:two', nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true }).status, 'LEASE_ACQUIRED');
});

test('leases require explicit authorization, valid IDs and eligible nodes', () => {
  const manager = new ResourceLeaseManager({ nodeRegistry: nodeRegistry({ blocked: { trust: 'UNVERIFIED', status: 'AVAILABLE' } }) });
  assert.throws(() => manager.acquire({ ownerId: 'task:a', nodeId: 'local', resources: ['gpu:0'] }), /authorization/);
  assert.throws(() => manager.acquire({ ownerId: 'task:a', nodeId: 'local', resources: ['device:secret'], explicitAuthorization: true }), /resource/);
  assert.throws(() => manager.acquire({ ownerId: 'task:a', nodeId: 'blocked', resources: ['cpu:0'], explicitAuthorization: true }), /eligible/);
  assert.throws(() => manager.acquire({ ownerId: 'task:a', nodeId: 'local', resources: ['gpu:0'], ttlMs: -5, explicitAuthorization: true }), /TTL/);
});

test('quarantine and remote compute control cannot be bypassed by a lease', () => {
  const manager = new ResourceLeaseManager({ nodeRegistry: nodeRegistry({
    denied: { trust: 'VERIFIED', status: 'AVAILABLE', resourceControl: { allocationState: 'QUARANTINED', computeAccess: 'DENIED' } },
    remote: { trust: 'VERIFIED', status: 'AVAILABLE', resourceControl: { allocationState: 'REMOTE_AVAILABLE', computeAccess: 'EXPLICIT_AUTHORIZED' } }
  }) });
  const request = { ownerId: 'task:a', nodeId: 'remote', resources: ['capability:llm'], explicitAuthorization: true };
  assert.throws(() => manager.acquire({ ...request, nodeId: 'denied' }), /denies execution/);
  assert.throws(() => manager.acquire(request), /compute authorization/);
  const acquired = manager.acquire({ ...request, computeAuthorized: true });
  assert.equal(acquired.status, 'LEASE_ACQUIRED');
  assert.throws(() => manager.validateExecutionLease({ leaseId: acquired.lease.leaseId, ownerId: 'task:a', nodeId: 'remote', resource: 'capability:llm' }), /compute authorization/);
  assert.equal(manager.validateExecutionLease({ leaseId: acquired.lease.leaseId, ownerId: 'task:a', nodeId: 'remote', resource: 'capability:llm', computeAuthorized: true }).mode, 'exclusive');
});

test('leases expire, renew and return defensive copies', () => {
  let now = new Date('2026-01-01T00:00:00.000Z');
  const manager = new ResourceLeaseManager({ nodeRegistry: nodeRegistry(), clock: () => now, createId: () => 'one' });
  const acquired = manager.acquire({ ownerId: 'task:a', nodeId: 'local', resources: ['cpu:local'], ttlMs: 1000, explicitAuthorization: true });
  acquired.lease.resources.push('gpu:0');
  assert.deepEqual(manager.list()[0].resources, ['cpu:local']);
  now = new Date('2026-01-01T00:00:00.500Z');
  assert.equal(manager.renew({ leaseId: acquired.lease.leaseId, ownerId: 'task:a', ttlMs: 1000, explicitAuthorization: true }).status, 'LEASE_RENEWED');
  now = new Date('2026-01-01T00:00:01.200Z');
  assert.equal(manager.list().length, 1);
  now = new Date('2026-01-01T00:00:01.500Z');
  assert.equal(manager.list().length, 0);
  assert.throws(() => manager.validateExecutionLease({ leaseId: acquired.lease.leaseId }), /expired/);
});
