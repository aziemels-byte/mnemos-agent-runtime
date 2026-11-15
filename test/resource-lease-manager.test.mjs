import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { FencingGuard, FileResourceLeaseManager, PhaseJobRegistry, ResourceLeaseManager, keepLeaseAlive } from '../src/index.mjs';
import { temporaryDirectory } from './helpers.mjs';

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

test('require returns a detached lease that cannot change conflicts, authorization or expiry', () => {
  let now = new Date('2026-01-01T00:00:00.000Z');
  const manager = new ResourceLeaseManager({ nodeRegistry: nodeRegistry(), clock: () => now });
  const request = { ownerId: 'task:owner', nodeId: 'local', resources: ['gpu:0'],
    ttlMs: 1000, explicitAuthorization: true };
  const { lease } = manager.acquire(request);
  const exposed = manager.require(lease.leaseId);
  exposed.resources.length = 0;
  exposed.ownerId = 'task:intruder';
  exposed.nodeId = 'other';
  exposed.expiresAt = '2027-01-01T00:00:00.000Z';
  assert.deepEqual(manager.require(lease.leaseId), lease);
  assert.equal(manager.acquire({ ...request, ownerId: 'task:intruder' }).status, 'LEASE_UNAVAILABLE');
  assert.throws(() => manager.renew({ leaseId: lease.leaseId, ownerId: 'task:intruder', explicitAuthorization: true }), /owner mismatch/);
  assert.throws(() => manager.release({ leaseId: lease.leaseId, ownerId: 'task:intruder', explicitAuthorization: true }), /owner mismatch/);
  assert.deepEqual(manager.validateExecutionLease({ leaseId: lease.leaseId,
    ownerId: request.ownerId, nodeId: request.nodeId, resource: 'gpu:0' }), lease);
  now = new Date('2026-01-01T00:00:01.000Z');
  assert.throws(() => manager.require(lease.leaseId), /expired/);
  assert.equal(manager.acquire({ ...request, ownerId: 'task:intruder' }).status, 'LEASE_ACQUIRED');
});

test('resource IDs are compared case-insensitively so variants cannot be leased twice', () => {
  const manager = new ResourceLeaseManager({ nodeRegistry: nodeRegistry() });
  const first = manager.acquire({ ownerId: 'task:a', nodeId: 'local', resources: ['GPU:0', 'gpu:0', 'Workspace:Build'], explicitAuthorization: true });
  assert.deepEqual(first.lease.resources, ['gpu:0', 'workspace:build']);
  assert.equal(manager.acquire({ ownerId: 'task:b', nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true }).status, 'LEASE_UNAVAILABLE');
  assert.equal(manager.acquire({ ownerId: 'task:b', nodeId: 'local', resources: ['WORKSPACE:build'], explicitAuthorization: true }).status, 'LEASE_UNAVAILABLE');
  assert.equal(manager.validateExecutionLease({ leaseId: first.lease.leaseId, ownerId: 'task:a', nodeId: 'local', resource: 'GPU:0' }).mode, 'exclusive');
});
const fileManager = (directory, options = {}) => new FileResourceLeaseManager({ directory, nodeRegistry: nodeRegistry(), ...options });

test('file leases keep authorization, expiry and concurrency rules within one process', async t => {
  const directory = await temporaryDirectory(t);
  let now = new Date('2026-01-01T00:00:00.000Z');
  const [a, b] = [fileManager(directory, { clock: () => now }), fileManager(directory, { clock: () => now })];
  const request = { ownerId: 'task:a', nodeId: 'local', resources: ['cpu:local'], ttlMs: 1000, explicitAuthorization: true };
  await assert.rejects(a.acquire({ ...request, explicitAuthorization: false }), /authorization/);
  await assert.rejects(a.acquire({ ...request, ownerId: 123 }), /Invalid ownerId/);
  const results = await Promise.all([a, b, a, b].map((manager, i) => manager.acquire({ ...request, ownerId: `task:${i}` })));
  assert.equal(results.filter(r => r.status === 'LEASE_ACQUIRED').length, 1);
  const { lease } = results.find(r => r.status === 'LEASE_ACQUIRED');
  await assert.rejects(b.renew({ leaseId: lease.leaseId, ownerId: 'task:intruder', explicitAuthorization: true }), /owner mismatch/);
  now = new Date('2026-01-01T00:00:00.900Z');
  assert.equal((await b.renew({ leaseId: lease.leaseId, ownerId: lease.ownerId, ttlMs: 1000, explicitAuthorization: true })).status, 'LEASE_RENEWED');
  now = new Date('2026-01-01T00:00:01.500Z');
  assert.equal((await a.acquire(request)).status, 'LEASE_UNAVAILABLE');
  now = new Date('2026-01-01T00:00:02.000Z');
  await assert.rejects(a.require(lease.leaseId), /expired/);
  assert.equal((await b.acquire(request)).status, 'LEASE_ACQUIRED');
  assert.equal((await a.snapshot()).policy.coordination, 'same-host-file-lock');
});

test('lease housekeeping preserves unrelated files with a state temporary suffix', async t => {
  const directory = await temporaryDirectory(t);
  const notes = path.join(directory, 'notes.state.tmp');
  await writeFile(notes, 'operator notes');
  await fileManager(directory).list();
  assert.equal(await readFile(notes, 'utf8'), 'operator notes');
});

test('fencing tokens increase on every grant, survive renewal and are never reused', () => {
  let now = new Date('2026-01-01T00:00:00.000Z');
  const manager = new ResourceLeaseManager({ nodeRegistry: nodeRegistry(), clock: () => now });
  const request = { nodeId: 'local', ttlMs: 1000, explicitAuthorization: true };
  const first = manager.acquire({ ...request, ownerId: 'task:a', resources: ['gpu:0'] }).lease;
  const second = manager.acquire({ ...request, ownerId: 'task:b', resources: ['gpu:1'] }).lease;
  assert.deepEqual([first.fencingToken, second.fencingToken], [1, 2]);
  // Refusals and rejected requests do not consume tokens.
  assert.equal(manager.acquire({ ...request, ownerId: 'task:c', resources: ['gpu:0'] }).status, 'LEASE_UNAVAILABLE');
  assert.throws(() => manager.acquire({ ...request, ownerId: 'task:c', resources: ['gpu:2'], explicitAuthorization: false }), /authorization/);
  assert.equal(manager.renew({ leaseId: first.leaseId, ownerId: 'task:a', ttlMs: 1000, explicitAuthorization: true }).lease.fencingToken, 1);
  manager.release({ leaseId: first.leaseId, ownerId: 'task:a', explicitAuthorization: true });
  assert.equal(manager.acquire({ ...request, ownerId: 'task:c', resources: ['gpu:0'] }).lease.fencingToken, 3);
  now = new Date('2026-01-01T00:00:05.000Z');
  assert.equal(manager.list().length, 0);
  assert.equal(manager.acquire({ ...request, ownerId: 'task:d', resources: ['gpu:0'] }).lease.fencingToken, 4);
  assert.equal(manager.snapshot().lastFencingToken, 4);
});

test('the file-backed fencing counter persists across instances and after all leases are released', async t => {
  const directory = await temporaryDirectory(t);
  const request = { ownerId: 'task:a', nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true };
  const { lease } = await fileManager(directory).acquire(request);
  assert.equal(lease.fencingToken, 1);
  await fileManager(directory).release({ leaseId: lease.leaseId, ownerId: 'task:a', explicitAuthorization: true });
  const stored = JSON.parse(await readFile(path.join(directory, 'leases.json'), 'utf8'));
  assert.deepEqual([stored.lastFencingToken, stored.leases], [1, []]);
  assert.equal((await fileManager(directory).acquire(request)).lease.fencingToken, 2);
  const snapshot = await fileManager(directory).snapshot();
  assert.equal(snapshot.lastFencingToken, 2);
  assert.equal(snapshot.policy.coordination, 'same-host-file-lock');
});

test('the fencing guard admits current and newer tokens and rejects older ones', () => {
  const guard = new FencingGuard();
  assert.equal(guard.admit('local/gpu:0', 1).status, 'FENCE_ACCEPTED');
  assert.equal(guard.admit('local/gpu:0', 1).status, 'FENCE_ACCEPTED');
  assert.equal(guard.admit('local/gpu:0', 3).status, 'FENCE_ACCEPTED');
  assert.throws(() => guard.admit('local/gpu:0', 2),
    error => error.code === 'FENCE_STALE' && error.fencingToken === 2 && error.highestToken === 3);
  assert.equal(guard.highest('local/gpu:0'), 3);
  assert.equal(guard.admit('local/gpu:1', 2).status, 'FENCE_ACCEPTED');
  for (const invalid of [0, -1, 1.5, '3', Number.NaN, undefined]) {
    assert.throws(() => guard.admit('local/gpu:0', invalid), TypeError);
  }
  assert.throws(() => guard.admit('', 5), TypeError);

  // After a restart the guard is seeded from the lease counter and fails closed.
  const restarted = new FencingGuard({ floor: 5 });
  assert.throws(() => restarted.admit('local/gpu:0', 4), error => error.code === 'FENCE_STALE');
  assert.equal(restarted.admit('local/gpu:0', 5).status, 'FENCE_ACCEPTED');
  for (const floor of [-1, 1.5, '5']) assert.throws(() => new FencingGuard({ floor }), RangeError);
});

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
// A node registry whose node can be taken out of service while a lease is held.
function switchableRegistry() {
  const node = { trust: 'VERIFIED', status: 'AVAILABLE' };
  return { node, require: id => { if (id !== 'local') throw Error('unknown node'); return node; } };
}

test('a lease keeper renews a lease past its TTL and releases it when stopped', async () => {
  const manager = new ResourceLeaseManager({ nodeRegistry: nodeRegistry() });
  const { lease } = manager.acquire({ ownerId: 'task:a', nodeId: 'local', resources: ['gpu:0'], ttlMs: 200, explicitAuthorization: true });
  let renewals = 0;
  const counting = { renew: args => { renewals++; return manager.renew(args); }, release: args => manager.release(args) };
  const keeper = keepLeaseAlive({ manager: counting, lease, ownerId: 'task:a', ttlMs: 200, renewEveryMs: 50, explicitAuthorization: true });
  await sleep(600);
  assert.equal(keeper.state, 'HOLDING');
  assert.equal(keeper.signal.aborted, false);
  assert.ok(renewals >= 5);
  assert.equal(manager.require(lease.leaseId).fencingToken, lease.fencingToken);
  assert.ok(Date.parse(keeper.lease.expiresAt) > Date.parse(lease.expiresAt));
  assert.equal((await keeper.stop({ release: true })).status, 'LEASE_RELEASED');
  assert.deepEqual(manager.list(), []);
  const before = renewals;
  await sleep(120);
  assert.equal(renewals, before, 'a stopped keeper never renews again');
});

test('a failed renewal aborts the signal once and the keeper never reacquires', async () => {
  const registry = switchableRegistry();
  const manager = new ResourceLeaseManager({ nodeRegistry: registry });
  const { lease } = manager.acquire({ ownerId: 'task:a', nodeId: 'local', resources: ['gpu:0'], ttlMs: 1000, explicitAuthorization: true });
  const losses = [];
  let renewals = 0;
  const counting = { renew: args => { renewals++; return manager.renew(args); }, release: args => manager.release(args) };
  const keeper = keepLeaseAlive({ manager: counting, lease, ownerId: 'task:a', ttlMs: 1000, renewEveryMs: 30,
    explicitAuthorization: true, onLost: reason => losses.push(reason) });
  await sleep(80);
  registry.node.status = 'OFFLINE'; // the node is withdrawn while the work runs
  await new Promise(resolve => keeper.signal.addEventListener('abort', resolve, { once: true }));
  assert.equal(keeper.state, 'LOST');
  assert.equal(keeper.signal.reason.code, 'LEASE_RENEWAL_FAILED');
  assert.match(keeper.signal.reason.cause.message, /not execution eligible/);
  const after = renewals;
  await sleep(100);
  assert.equal(renewals, after);
  assert.equal(losses.length, 1);
  assert.equal((await keeper.stop({ release: true })).status, 'LEASE_RELEASED'); // still ours, so it is released
  assert.equal(manager.list().length, 0);
});

test('a hung renewal cannot extend the lease: the local deadline fires first', async () => {
  const manager = new ResourceLeaseManager({ nodeRegistry: nodeRegistry() });
  const { lease } = manager.acquire({ ownerId: 'task:a', nodeId: 'local', resources: ['gpu:0'], ttlMs: 1000, explicitAuthorization: true });
  const hanging = { renew: () => new Promise(() => {}), release: args => manager.release(args) };
  const keeper = keepLeaseAlive({ manager: hanging, lease, ownerId: 'task:a', ttlMs: 1000, renewEveryMs: 100,
    safetyMarginMs: 300, explicitAuthorization: true });
  await new Promise(resolve => keeper.signal.addEventListener('abort', resolve, { once: true }));
  const abortedAt = Date.now();
  assert.equal(keeper.signal.reason.code, 'LEASE_DEADLINE_PASSED');
  // Scheduled 300 ms before expiry; the margin absorbs event-loop lag on a loaded machine.
  assert.ok(abortedAt < Date.parse(lease.expiresAt), `aborted ${abortedAt - Date.parse(lease.expiresAt)} ms after expiry`);
  assert.equal(keeper.state, 'LOST');
});

test('a slow renewal is trusted only from when it started, not when it returned', async () => {
  // Each renewal waits 200 ms (as if queued behind the directory lock) and then succeeds.
  // The manager's new expiresAt counts from the late transaction; the holder cannot rely on that.
  const manager = new ResourceLeaseManager({ nodeRegistry: nodeRegistry() });
  const { lease } = manager.acquire({ ownerId: 'task:a', nodeId: 'local', resources: ['gpu:0'], ttlMs: 300, explicitAuthorization: true });
  const slow = { renew: async args => { await sleep(200); return manager.renew(args); }, release: args => manager.release(args) };
  const keeper = keepLeaseAlive({ manager: slow, lease, ownerId: 'task:a', ttlMs: 300, renewEveryMs: 50,
    safetyMarginMs: 30, explicitAuthorization: true });
  const started = Date.now();
  await new Promise(resolve => keeper.signal.addEventListener('abort', resolve, { once: true }));
  assert.equal(keeper.signal.reason.code, 'LEASE_DEADLINE_PASSED');
  assert.ok(Date.now() - started < 1000);
  await keeper.stop();
});

test('a keeper cannot start or accept a renewal after its deadline regardless of timer order', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let now = 0;
  const clock = () => new Date(now);
  const manager = new ResourceLeaseManager({ nodeRegistry: nodeRegistry(), clock });
  const request = { ownerId: 'task:a', nodeId: 'local', ttlMs: 800, explicitAuthorization: true };
  const options = { clock, ownerId: request.ownerId, ttlMs: request.ttlMs, safetyMarginMs: 300,
    explicitAuthorization: true };
  const { lease: scheduledLease } = manager.acquire({ ...request, resources: ['gpu:0'] });
  const { lease: pendingLease } = manager.acquire({ ...request, resources: ['gpu:1'] });

  const scheduled = keepLeaseAlive({ ...options, manager, lease: scheduledLease, renewEveryMs: 200 });
  const pending = Promise.withResolvers();
  let renewalRequest;
  const slowManager = { renew(args) { renewalRequest = args; return pending.promise; }, release: args => manager.release(args) };
  const inflight = keepLeaseAlive({ ...options, manager: slowManager, lease: pendingLease, renewEveryMs: 100 });

  now = 100;
  t.mock.timers.tick(100); // The slow renewal starts before the deadline.
  now = 501;
  t.mock.timers.tick(100); // The other renewal wakes after the deadline but before its timer runs.
  pending.resolve(manager.renew(renewalRequest));
  await pending.promise;

  for (const keeper of [scheduled, inflight]) {
    assert.equal(keeper.state, 'LOST');
    assert.equal(keeper.signal.reason.code, 'LEASE_DEADLINE_PASSED');
  }
  assert.equal(manager.require(scheduledLease.leaseId).expiresAt, scheduledLease.expiresAt);
  assert.equal(inflight.lease.expiresAt, pendingLease.expiresAt);
});

test('a loss is reported once, and never after the caller stopped the keeper', async () => {
  const manager = new ResourceLeaseManager({ nodeRegistry: nodeRegistry() });
  const acquire = resource => manager.acquire({ ownerId: 'task:a', nodeId: 'local', resources: [resource], ttlMs: 300, explicitAuthorization: true }).lease;
  // Renewals hang past the deadline and only then fail.
  const lateFailure = { renew: async () => { await sleep(350); throw new Error('lock timeout'); }, release: args => manager.release(args) };
  const options = { manager: lateFailure, ownerId: 'task:a', ttlMs: 300, renewEveryMs: 50, safetyMarginMs: 30, explicitAuthorization: true };

  const losses = [];
  const deadlineFirst = keepLeaseAlive({ ...options, lease: acquire('gpu:0'), onLost: reason => losses.push(reason.code) });
  await sleep(700); // deadline at ~270 ms, failed renewal arrives at ~400 ms
  assert.deepEqual(losses, ['LEASE_DEADLINE_PASSED']);
  assert.equal(deadlineFirst.signal.reason.code, 'LEASE_DEADLINE_PASSED');
  assert.equal((await deadlineFirst.stop()).status, 'LEASE_LOST');

  const stoppedLosses = [];
  const stopped = keepLeaseAlive({ ...options, lease: acquire('gpu:1'), onLost: reason => stoppedLosses.push(reason.code) });
  await sleep(80); // a renewal is now in flight
  assert.equal((await stopped.stop()).status, 'KEEPER_STOPPED'); // waits for the in-flight renewal to fail
  await sleep(50);
  assert.deepEqual(stoppedLosses, []);
  assert.equal(stopped.state, 'STOPPED');
  assert.equal(stopped.signal.aborted, false);
});

test('losing a lease cancels the owned job through the phase job registry', async () => {
  const registry = switchableRegistry();
  const manager = new ResourceLeaseManager({ nodeRegistry: registry });
  const { lease } = manager.acquire({ ownerId: 'task:train', nodeId: 'local', resources: ['gpu:0'], ttlMs: 1000, explicitAuthorization: true });
  const guard = new FencingGuard();
  const committed = [];
  const jobs = new PhaseJobRegistry({ execute: async ({ signal, setPhase }) => {
    setPhase('training');
    while (!signal.aborted) {
      guard.admit('local/gpu:0', lease.fencingToken);
      committed.push(lease.fencingToken);
      await sleep(10);
    }
    return { status: 'CANCELLED', termination: { confirmed: true } };
  } });
  const keeper = keepLeaseAlive({ manager, lease, ownerId: 'task:train', ttlMs: 1000, renewEveryMs: 30,
    explicitAuthorization: true, onLost: () => jobs.requestCancellation('train_job') });
  jobs.start({ jobId: 'train_job', objective: 'Use the leased GPU' });
  await sleep(60);
  registry.node.status = 'OFFLINE';
  const settled = await jobs.wait('train_job');
  assert.equal(settled.status, 'CANCELLED');
  assert.equal(settled.cancellationConfirmed, true);
  assert.equal(settled.phase, 'training');
  assert.ok(committed.length > 0);
  assert.equal(keeper.signal.reason.code, 'LEASE_RENEWAL_FAILED');
  await keeper.stop();
});

test('a lease keeper validates its configuration', () => {
  const manager = new ResourceLeaseManager({ nodeRegistry: nodeRegistry() });
  const { lease } = manager.acquire({ ownerId: 'task:a', nodeId: 'local', resources: ['gpu:0'], ttlMs: 1000, explicitAuthorization: true });
  const base = { manager, lease, ownerId: 'task:a', ttlMs: 1000, explicitAuthorization: true };
  assert.throws(() => keepLeaseAlive({ ...base, explicitAuthorization: false }), /authorization/);
  assert.throws(() => keepLeaseAlive({ ...base, ownerId: 'task:other' }), /owner mismatch/);
  assert.throws(() => keepLeaseAlive({ ...base, lease: { leaseId: 'x' } }), /lease is required/);
  assert.throws(() => keepLeaseAlive({ ...base, manager: {} }), /manager\.renew/);
  assert.throws(() => keepLeaseAlive({ ...base, renewEveryMs: 950, safetyMarginMs: 100 }), RangeError);
  assert.throws(() => keepLeaseAlive({ ...base, renewEveryMs: 0 }), RangeError);
  assert.throws(() => keepLeaseAlive({ ...base, onLost: 'call me' }), TypeError);
  const expired = keepLeaseAlive({ ...base, lease: { ...lease, expiresAt: new Date(Date.now() - 1).toISOString() } });
  assert.equal(expired.signal.reason.code, 'LEASE_DEADLINE_PASSED'); // already past: lost at once
});
