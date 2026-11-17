import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { hostname } from 'node:os';
import path from 'node:path';
import { FencingGuard, FileResourceLeaseManager } from '../src/index.mjs';
import { DirectoryLock } from '../src/directory-lock.mjs';
import { processIdentity } from '../src/process-identity.mjs';
import { failFsPromises, temporaryDirectory, until, workerThread } from './helpers.mjs';

const nodeRegistry = () => ({ require: () => ({ trust: 'VERIFIED', status: 'AVAILABLE' }) });
const fileManager = (directory, options = {}) => new FileResourceLeaseManager({ directory, nodeRegistry: nodeRegistry(), ...options });

// Child processes import the module directly; each gets its own manager instance.
const moduleUrl = new URL('../src/index.mjs', import.meta.url).href;
const childPrelude = `
  import { FileResourceLeaseManager, keepLeaseAlive } from ${JSON.stringify(moduleUrl)};
  const [directory, ...args] = process.argv.slice(1);
  const manager = new FileResourceLeaseManager({ directory, lockTimeoutMs: 20000,
    nodeRegistry: { require: () => ({ trust: 'VERIFIED', status: 'AVAILABLE' }) } });
  const emit = value => process.stdout.write(JSON.stringify(value));
`;

function runChild(source, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--input-type=module', '-e', childPrelude + source, '--', ...args],
      { stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    child.on('error', reject);
    child.on('close', code => code === 0 ? resolve(JSON.parse(stdout)) : reject(new Error(`child exited ${code}: ${stderr}`)));
  });
}


test('concurrent processes cannot both acquire the same exclusive resource', async t => {
  const directory = await temporaryDirectory(t);
  const source = `emit((await manager.acquire({ ownerId: 'proc:' + process.pid, nodeId: 'local', resources: [args[0]], explicitAuthorization: true })).status);`;
  const statuses = await Promise.all(Array.from({ length: 8 }, (_, i) => runChild(source, [directory, i % 2 ? 'GPU:0' : 'gpu:0'])));
  assert.equal(statuses.filter(status => status === 'LEASE_ACQUIRED').length, 1);
  assert.equal(statuses.filter(status => status === 'LEASE_UNAVAILABLE').length, 7);
  assert.equal((await fileManager(directory).list()).length, 1);
  assert.deepEqual(await readdir(directory), ['leases.json']);
});

test('lease holders in different processes never overlap while cycling acquire and release', async t => {
  const directory = await temporaryDirectory(t);
  const marker = path.join(directory, 'critical-section');
  const source = `
    import { rm, writeFile } from 'node:fs/promises';
    const owner = 'proc:' + process.pid;
    let acquired = 0, violations = 0;
    for (let i = 0; i < 12; i++) {
      const result = await manager.acquire({ ownerId: owner, nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true });
      if (result.status !== 'LEASE_ACQUIRED') { await new Promise(r => setTimeout(r, 3)); continue; }
      acquired++;
      try { await writeFile(args[0], owner, { flag: 'wx' }); } catch { violations++; }
      await new Promise(r => setTimeout(r, 2));
      await rm(args[0], { force: true });
      await manager.release({ leaseId: result.lease.leaseId, ownerId: owner, explicitAuthorization: true });
    }
    emit({ acquired, violations });`;
  const results = await Promise.all(Array.from({ length: 4 }, () => runChild(source, [directory, marker])));
  assert.equal(results.reduce((sum, r) => sum + r.violations, 0), 0);
  assert.ok(results.reduce((sum, r) => sum + r.acquired, 0) > 0);
  assert.deepEqual(await fileManager(directory).list(), []);
});

test('a lease acquired in one process is enforced and releasable across processes', async t => {
  const directory = await temporaryDirectory(t);
  const manager = fileManager(directory);
  const { lease } = await manager.acquire({ ownerId: 'task:parent', nodeId: 'local', resources: ['gpu:1'], explicitAuthorization: true });
  const source = `
    const [mode, leaseId] = args;
    if (mode === 'inspect') emit({
      validated: (await manager.validateExecutionLease({ leaseId, ownerId: 'task:parent', nodeId: 'local', resource: 'gpu:1' })).leaseId,
      competing: (await manager.acquire({ ownerId: 'task:child', nodeId: 'local', resources: ['gpu:1'], explicitAuthorization: true })).status,
      wrongOwner: await manager.release({ leaseId, ownerId: 'task:child', explicitAuthorization: true }).then(() => 'released', e => e.message) });
    else emit((await manager.acquire({ ownerId: 'task:child', nodeId: 'local', resources: ['gpu:1'], explicitAuthorization: true })).status);`;
  assert.deepEqual(await runChild(source, [directory, 'inspect', lease.leaseId]),
    { validated: lease.leaseId, competing: 'LEASE_UNAVAILABLE', wrongOwner: 'Lease owner mismatch' });
  assert.equal((await manager.release({ leaseId: lease.leaseId, ownerId: 'task:parent', explicitAuthorization: true })).status, 'LEASE_RELEASED');
  assert.equal(await runChild(source, [directory, 'acquire']), 'LEASE_ACQUIRED');
});


test('a lock left by a dead process is recovered, but a possibly live holder is never broken', async t => {
  const directory = await temporaryDirectory(t);
  const lockFile = path.join(directory, 'leases.lock');
  const request = { ownerId: 'task:a', nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true };
  const lockRecord = fields => JSON.stringify({ token: randomUUID(), lockedAt: new Date().toISOString(), ...fields });

  const exited = spawn(process.execPath, ['-e', '']);
  const deadPid = exited.pid;
  await new Promise(resolve => exited.on('close', resolve));
  await writeFile(lockFile, lockRecord({ pid: deadPid, host: hostname() }));
  assert.equal((await fileManager(directory, { lockTimeoutMs: 2000 }).acquire(request)).status, 'LEASE_ACQUIRED');
  assert.deepEqual((await readdir(directory)).sort(), ['leases.json']);

  // The timeout error says who holds the lock and what to do next, as inspectLock() would.
  const expected = [
    [{ pid: deadPid, host: 'other-host.invalid' }, 'OTHER_HOST', /PID \d+ on other-host\.invalid .*confirm it is gone, then use forceUnlock\(\)/],
    [{ pid: process.pid, host: hostname() }, 'ALIVE', /still running \(possibly paused\): wait, or stop that process/],
    [{ unreadable: true }, 'UNREADABLE', /lock record is unreadable .*confirm no operation is running/]
  ];
  for (const [holder, holderState, message] of expected) {
    const text = holder.unreadable ? '{"truncated' : lockRecord(holder);
    await writeFile(lockFile, text);
    const manager = fileManager(directory, { lockTimeoutMs: 50 });
    const error = await manager.list().catch(error => error);
    assert.equal(error.code, 'LEASE_LOCK_TIMEOUT');
    assert.equal(error.holderState, holderState);
    assert.match(error.message, message);
    assert.equal(error.lockId, (await manager.inspectLock()).lockId); // usable directly with forceUnlock after confirmation
    assert.equal(error.holder?.pid, holder.pid);
    assert.equal(await readFile(lockFile, 'utf8'), text);
  }
});

test('corrupt lease state fails closed instead of granting held resources again', async t => {
  const directory = await temporaryDirectory(t);
  const manager = fileManager(directory);
  await manager.acquire({ ownerId: 'task:a', nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true });
  const stateFile = path.join(directory, 'leases.json');
  const valid = JSON.parse(await readFile(stateFile, 'utf8'));
  for (const corrupt of ['{"schema":', JSON.stringify({ ...valid, schema: 'other' }),
    JSON.stringify({ ...valid, leases: [{ ...valid.leases[0], resources: ['GPU:0'] }] }),
    JSON.stringify({ ...valid, leases: [valid.leases[0], valid.leases[0]] }),
    JSON.stringify({ ...valid, lastFencingToken: undefined }),
    JSON.stringify({ ...valid, lastFencingToken: 0 }),
    JSON.stringify({ ...valid, leases: [{ ...valid.leases[0], fencingToken: 1.5 }] }),
    JSON.stringify({ ...valid, lastFencingToken: 3, leases: [valid.leases[0], { ...valid.leases[0], leaseId: 'lease:copy', resources: ['gpu:9'] }] })]) {
    await writeFile(stateFile, corrupt);
    await assert.rejects(manager.acquire({ ownerId: 'task:b', nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true }),
      error => error.code === 'LEASE_STATE_INVALID');
    assert.equal(await readFile(stateFile, 'utf8'), corrupt);
  }
  assert.deepEqual((await readdir(directory)).sort(), ['leases.json']);
});

// SIGKILL cannot be caught, so these children die without any cleanup.
function startChild(source, args) {
  const child = spawn(process.execPath, ['--input-type=module', '-e', childPrelude + source, '--', ...args],
    { stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = '';
  child.stderr.on('data', chunk => { stderr += chunk; });
  child.exited = new Promise(resolve => child.on('close', code => resolve({ code, stderr })));
  return child;
}

async function waitForFile(file, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try { return await readFile(file, 'utf8'); }
    catch (error) {
      if (error.code !== 'ENOENT') throw error;
      if (Date.now() > deadline) throw new Error(`Timed out waiting for ${file}`);
      await new Promise(resolve => setTimeout(resolve, 10));
    }
  }
}

test('a process killed while holding the directory lock does not strand the resource', async t => {
  const [directory, signals] = [await temporaryDirectory(t), await temporaryDirectory(t)];
  const holding = path.join(signals, 'inside-critical-section');
  // The node registry is consulted inside the transaction: mark the file and hang.
  const source = `
    import { writeFileSync } from 'node:fs';
    const spinning = new FileResourceLeaseManager({ directory, lockTimeoutMs: 20000,
      nodeRegistry: { require: () => { writeFileSync(args[0], String(process.pid)); for (;;); } } });
    await spinning.acquire({ ownerId: 'task:doomed', nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true });
    emit('unreachable');`;
  const child = startChild(source, [directory, holding]);
  t.after(() => child.kill('SIGKILL'));
  await waitForFile(holding);
  await waitForFile(path.join(directory, 'leases.lock'));
  child.kill('SIGKILL');
  assert.equal((await child.exited).code, null);

  const manager = fileManager(directory, { lockTimeoutMs: 5000 });
  assert.equal((await manager.acquire({ ownerId: 'task:next', nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true })).status, 'LEASE_ACQUIRED');
  assert.deepEqual(await readdir(directory), ['leases.json']);
});

test('a lease held by a killed process is honoured until its TTL expires', async t => {
  const [directory, signals] = [await temporaryDirectory(t), await temporaryDirectory(t)];
  const acquired = path.join(signals, 'acquired.json');
  const source = `
    import { renameSync, writeFileSync } from 'node:fs';
    const result = await manager.acquire({ ownerId: 'task:doomed', nodeId: 'local', resources: ['gpu:0'], ttlMs: 60000, explicitAuthorization: true });
    writeFileSync(args[0] + '.tmp', JSON.stringify(result.lease));
    renameSync(args[0] + '.tmp', args[0]); // publish atomically
    for (;;);`;
  const child = startChild(source, [directory, acquired]);
  t.after(() => child.kill('SIGKILL'));
  const lease = JSON.parse(await waitForFile(acquired));
  child.kill('SIGKILL');
  await child.exited;

  // A crash is not a release: the lease stands until it expires on the shared clock.
  const manager = fileManager(directory);
  assert.equal((await manager.acquire({ ownerId: 'task:next', nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true })).status, 'LEASE_UNAVAILABLE');
  assert.equal((await manager.require(lease.leaseId)).ownerId, 'task:doomed');
  const later = fileManager(directory, { clock: () => new Date(Date.parse(lease.expiresAt) + 1) });
  assert.equal((await later.acquire({ ownerId: 'task:next', nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true })).status, 'LEASE_ACQUIRED');
});

test('temporary files from crashed writers are swept without touching live waiters', async t => {
  const directory = await temporaryDirectory(t);
  const manager = fileManager(directory);
  const first = await manager.acquire({ ownerId: 'task:a', nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true });
  const state = await readFile(path.join(directory, 'leases.json'), 'utf8');

  const exited = spawn(process.execPath, ['-e', '']);
  const deadPid = exited.pid;
  await new Promise(resolve => exited.on('close', resolve));
  const orphans = {
    [`${randomUUID()}.state.tmp`]: '{"half written',
    [`${randomUUID()}.lock.tmp`]: JSON.stringify({ token: randomUUID(), pid: deadPid, host: hostname() }),
    [`${randomUUID()}.${deadPid}.lock.tmp`]: '', // killed between creating and writing its record
    [`${randomUUID()}.lock.tmp`]: JSON.stringify({ token: randomUUID(), pid: process.pid, host: hostname() })
  };
  for (const [name, text] of Object.entries(orphans)) await writeFile(path.join(directory, name), text);
  const live = Object.keys(orphans).at(-1);
  const liveEmpty = `${randomUUID()}.${process.pid}.lock.tmp`; // a live waiter mid-write is kept
  await writeFile(path.join(directory, liveEmpty), '');

  assert.equal((await manager.list()).length, 1);
  assert.deepEqual((await readdir(directory)).sort(), ['leases.json', live, liveEmpty].sort());
  assert.equal(await readFile(path.join(directory, 'leases.json'), 'utf8'), state);
  assert.equal((await manager.release({ leaseId: first.lease.leaseId, ownerId: 'task:a', explicitAuthorization: true })).status, 'LEASE_RELEASED');
});

test('a crash while breaking a stale lock fails closed until an operator force-unlocks it', async t => {
  const directory = await temporaryDirectory(t);
  const lockFile = path.join(directory, 'leases.lock');
  const request = { ownerId: 'task:a', nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true };
  const exited = spawn(process.execPath, ['-e', '']);
  const deadPid = exited.pid;
  await new Promise(resolve => exited.on('close', resolve));

  const staleToken = randomUUID();
  await writeFile(lockFile, JSON.stringify({ token: staleToken, pid: deadPid, host: hostname() }));
  // A second process died after claiming the break and before removing the lock.
  const claim = path.join(directory, `leases.lock.${staleToken}.break`);
  await writeFile(claim, String(deadPid));
  await assert.rejects(fileManager(directory, { lockTimeoutMs: 100 }).acquire(request),
    error => error.code === 'LEASE_LOCK_TIMEOUT' && error.lockFile === lockFile && error.holderState === 'DEAD' &&
      error.pendingBreaks.length === 1 && /recovery attempt is pending: if it does not clear, use forceUnlock\(\)/.test(error.message));
  assert.equal(JSON.parse(await readFile(lockFile, 'utf8')).token, staleToken);

  // Operator recovery: inspect, then force-unlock exactly the inspected lock.
  const manager = fileManager(directory, { lockTimeoutMs: 5000 });
  const inspected = await manager.inspectLock();
  assert.equal(inspected.holderState, 'DEAD');
  assert.deepEqual(inspected.pendingBreaks.map(entry => entry.token), [staleToken]);
  assert.equal((await manager.forceUnlock({ expectedLockId: inspected.lockId, explicitAuthorization: true })).status, 'LOCK_FORCE_RELEASED');
  assert.deepEqual(await readdir(directory), []);
  assert.equal((await manager.acquire(request)).status, 'LEASE_ACQUIRED');
});

test('racing processes receive unique, gap-free fencing tokens', async t => {
  const directory = await temporaryDirectory(t);
  const source = `emit((await manager.acquire({ ownerId: 'proc:' + process.pid, nodeId: 'local', resources: [args[0]], explicitAuthorization: true })).lease.fencingToken);`;
  const tokens = await Promise.all(Array.from({ length: 8 }, (_, i) => runChild(source, [directory, `gpu:${i}`])));
  assert.deepEqual(tokens.sort((a, b) => a - b), [1, 2, 3, 4, 5, 6, 7, 8]);
  assert.equal((await fileManager(directory).snapshot()).lastFencingToken, 8);
});


test('a paused holder whose lease expired is fenced out by the protected resource',
  { skip: process.platform === 'win32' && 'SIGSTOP/SIGCONT are POSIX-only' }, async t => {
  const directory = await temporaryDirectory(t);
  // A well-behaved worker: renews its lease on a heartbeat and keeps writing.
  // It never rechecks the lease before a write, as a real worker could not do so atomically.
  const source = `
    const owner = 'task:slow';
    const { lease } = await manager.acquire({ ownerId: owner, nodeId: 'local', resources: ['gpu:0'], ttlMs: 1000, explicitAuthorization: true });
    process.send({ type: 'acquired', lease });
    let renewing = false;
    setInterval(async () => {
      if (renewing) return;
      renewing = true;
      try {
        await manager.renew({ leaseId: lease.leaseId, ownerId: owner, ttlMs: 1000, explicitAuthorization: true });
        process.send({ type: 'renewed' });
      } catch (error) { process.send({ type: 'renew-failed', message: error.message }); }
      finally { renewing = false; }
    }, 250);
    setInterval(() => process.send({ type: 'write', fencingToken: lease.fencingToken }), 25);`;
  const child = spawn(process.execPath, ['--input-type=module', '-e', childPrelude + source, '--', directory],
    { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  t.after(() => { child.kill('SIGCONT'); child.kill('SIGKILL'); });

  // The protected resource: it owns the guard and applies admitted writes.
  const guard = new FencingGuard();
  const key = 'local/gpu:0';
  const log = { renewed: 0, renewFailures: [], writes: [], unexpected: [] };
  let phase = 'running';
  let slowLease = null;
  child.on('message', message => {
    if (message.type === 'acquired') slowLease = message.lease;
    if (message.type === 'renewed') log.renewed++;
    if (message.type === 'renew-failed') log.renewFailures.push({ phase, message: message.message });
    if (message.type !== 'write') return;
    try { guard.admit(key, message.fencingToken); log.writes.push({ phase, token: message.fencingToken, admitted: true }); }
    catch (error) {
      if (error.code === 'FENCE_STALE') log.writes.push({ phase, token: message.fencingToken, admitted: false });
      else log.unexpected.push(error.message);
    }
  });

  // 1. While running, the heartbeat keeps the lease alive past its 1 s TTL.
  await until(() => log.renewed >= 5 && log.writes.length >= 3, 'the worker to renew and write');
  assert.ok(log.writes.every(write => write.admitted));

  // 2. Freeze the worker between transactions (a frozen lock holder would block everyone).
  const lockFile = path.join(directory, 'leases.lock');
  const holdsLock = async () => readFile(lockFile, 'utf8').then(text => JSON.parse(text).pid === child.pid, () => false);
  for (;;) {
    child.kill('SIGSTOP');
    await new Promise(resolve => setTimeout(resolve, 20));
    if (!(await holdsLock())) break;
    child.kill('SIGCONT');
    await new Promise(resolve => setTimeout(resolve, 30));
  }
  phase = 'paused';

  // 3. The lease expires without the heartbeat and another process takes over.
  const manager = fileManager(directory);
  let fastLease = null;
  await until(async () => {
    const result = await manager.acquire({ ownerId: 'task:fast', nodeId: 'local', resources: ['gpu:0'], ttlMs: 60_000, explicitAuthorization: true });
    fastLease = result.lease ?? null;
    return result.status === 'LEASE_ACQUIRED';
  }, 'the paused lease to expire');
  assert.ok(fastLease.fencingToken > slowLease.fencingToken);
  assert.equal(guard.admit(key, fastLease.fencingToken).status, 'FENCE_ACCEPTED');

  // 4. The worker resumes and, unaware, keeps writing with its old token.
  phase = 'resumed';
  child.kill('SIGCONT');
  await until(() => log.writes.filter(w => w.phase === 'resumed').length >= 3 &&
    log.renewFailures.some(f => f.phase === 'resumed'), 'late writes and a failed renewal');
  child.kill('SIGKILL');

  const late = log.writes.filter(write => write.phase === 'resumed');
  assert.deepEqual(log.unexpected, []);
  assert.ok(late.length >= 3);
  assert.ok(late.every(write => !write.admitted && write.token === slowLease.fencingToken), JSON.stringify(late));
  assert.match(log.renewFailures.find(f => f.phase === 'resumed').message, /expired/);
  assert.equal(guard.highest(key), fastLease.fencingToken);
  // The stale holder could neither renew nor disturb the new lease.
  assert.equal((await manager.require(fastLease.leaseId)).ownerId, 'task:fast');
});

async function deadPid() {
  const exited = spawn(process.execPath, ['-e', '']);
  await new Promise(resolve => exited.on('close', resolve));
  return exited.pid;
}

test('operators can inspect the lock, and a live holder can never be force-unlocked', async t => {
  const [directory, signals] = [await temporaryDirectory(t), await temporaryDirectory(t)];
  const manager = fileManager(directory);
  assert.deepEqual(await fileManager(path.join(directory, 'absent')).inspectLock(),
    { status: 'UNLOCKED', lockFile: path.join(directory, 'absent', 'leases.lock'), pendingBreaks: [] });

  const holding = path.join(signals, 'inside-critical-section');
  const source = `
    import { writeFileSync } from 'node:fs';
    const spinning = new FileResourceLeaseManager({ directory, lockTimeoutMs: 20000,
      nodeRegistry: { require: () => { writeFileSync(args[0], ''); for (;;); } } });
    await spinning.acquire({ ownerId: 'task:busy', nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true });`;
  const child = startChild(source, [directory, holding]);
  t.after(() => child.kill('SIGKILL'));
  await waitForFile(holding);

  const live = await manager.inspectLock();
  assert.equal(live.status, 'LOCKED');
  assert.equal(live.holderState, 'ALIVE');
  assert.equal(live.holder.pid, child.pid);
  assert.match(live.lockId, /^[0-9a-f]{64}$/);
  await assert.rejects(manager.forceUnlock({ expectedLockId: live.lockId, explicitAuthorization: true }),
    error => error.code === 'LOCK_HOLDER_ALIVE');
  assert.equal((await manager.inspectLock()).lockId, live.lockId);

  child.kill('SIGKILL');
  await child.exited;
  const dead = await manager.inspectLock();
  assert.equal(dead.holderState, 'DEAD');
  assert.equal(dead.lockId, live.lockId);
});

test('force-unlock removes only the inspected lock of a holder that cannot be checked', async t => {
  const directory = await temporaryDirectory(t);
  const lockFile = path.join(directory, 'leases.lock');
  const manager = fileManager(directory, { lockTimeoutMs: 50 });
  const request = { ownerId: 'task:a', nodeId: 'local', resources: ['gpu:0'], explicitAuthorization: true };
  assert.deepEqual(await manager.forceUnlock({ expectedLockId: '0'.repeat(64), explicitAuthorization: true }), { status: 'NOT_LOCKED' });

  const foreign = JSON.stringify({ token: randomUUID(), pid: 4242, host: 'build-agent.invalid', lockedAt: new Date().toISOString() });
  for (const [text, holderState] of [[foreign, 'OTHER_HOST'], ['{"truncated', 'UNREADABLE']]) {
    await writeFile(lockFile, text);
    await assert.rejects(manager.acquire(request), error => error.code === 'LEASE_LOCK_TIMEOUT');
    const inspected = await manager.inspectLock();
    assert.equal(inspected.holderState, holderState);
    await assert.rejects(manager.forceUnlock({ expectedLockId: inspected.lockId }), /authorization/);
    await assert.rejects(manager.forceUnlock({ expectedLockId: 'not-a-hash', explicitAuthorization: true }), TypeError);

    // The lock is replaced after inspection: the stale decision must not apply to it.
    const replacement = text + ' ';
    await writeFile(lockFile, replacement);
    await assert.rejects(manager.forceUnlock({ expectedLockId: inspected.lockId, explicitAuthorization: true }),
      error => error.code === 'LOCK_CHANGED');
    assert.equal(await readFile(lockFile, 'utf8'), replacement);

    const current = await manager.inspectLock();
    const released = await manager.forceUnlock({ expectedLockId: current.lockId, explicitAuthorization: true });
    assert.equal(released.status, 'LOCK_FORCE_RELEASED');
    assert.deepEqual((await readdir(directory)).filter(name => name !== 'leases.json'), []);
  }
  assert.equal((await manager.acquire(request)).status, 'LEASE_ACQUIRED');
});

test('force-unlock defers to a live breaker, and claims for vanished locks are swept', async t => {
  const directory = await temporaryDirectory(t);
  const manager = fileManager(directory, { lockTimeoutMs: 2000 });
  const staleToken = randomUUID();
  const lockFile = path.join(directory, 'leases.lock');
  const claim = path.join(directory, `leases.lock.${staleToken}.break`);
  await writeFile(lockFile, JSON.stringify({ token: staleToken, pid: await deadPid(), host: hostname() }));
  await writeFile(claim, JSON.stringify({ pid: process.pid, host: hostname() }));
  const inspected = await manager.inspectLock();
  assert.equal(inspected.pendingBreaks[0].claimantState, 'ALIVE');
  await assert.rejects(manager.forceUnlock({ expectedLockId: inspected.lockId, explicitAuthorization: true }),
    error => error.code === 'LOCK_BREAK_IN_PROGRESS');
  assert.ok((await readdir(directory)).includes('leases.lock'));

  // The breaker finishes its work; its claim is left behind for a lock that is gone.
  await rm(lockFile);
  const orphanClaim = path.join(directory, `leases.lock.${randomUUID()}.break`);
  await writeFile(orphanClaim, JSON.stringify({ pid: await deadPid(), host: hostname() }));
  assert.equal((await manager.list()).length, 0);
  assert.deepEqual(await readdir(directory), []);
});

test('a lease keeper in one process holds the resource against other processes until it releases', async t => {
  const directory = await temporaryDirectory(t);
  // Production TTLs are seconds to minutes. Renewing every 400 ms against a 2 s TTL
  // tolerates about 1.6 s without CPU, which a shared, oversubscribed host can cause.
  const ttlMs = 2000;
  const renewEveryMs = 400;
  const source = `
    const ttlMs = Number(args[0]), renewEveryMs = Number(args[1]);
    const { lease } = await manager.acquire({ ownerId: 'task:kept', nodeId: 'local', resources: ['gpu:0'], ttlMs, explicitAuthorization: true });
    let slowestRenewalMs = 0;
    const timed = { release: request => manager.release(request), renew: async request => {
      const started = Date.now();
      try { return await manager.renew(request); } finally { slowestRenewalMs = Math.max(slowestRenewalMs, Date.now() - started); }
    } };
    const keeper = keepLeaseAlive({ manager: timed, lease, ownerId: 'task:kept', ttlMs, renewEveryMs, explicitAuthorization: true });
    await new Promise(resolve => setTimeout(resolve, 3 * ttlMs));
    const report = { aborted: keeper.signal.aborted, reason: keeper.signal.reason?.code ?? null, fencingToken: keeper.lease.fencingToken };
    emit({ ...report, stop: (await keeper.stop({ release: true })).status, slowestRenewalMs });`;
  const child = startChild(source, [directory, String(ttlMs), String(renewEveryMs)]);
  let stdout = '';
  child.stdout.on('data', chunk => { stdout += chunk; });
  t.after(() => child.kill('SIGKILL'));

  const manager = fileManager(directory);
  const attempt = () => manager.acquire({ ownerId: 'task:other', nodeId: 'local', resources: ['gpu:0'], ttlMs: 60_000, explicitAuthorization: true });
  await until(async () => (await manager.list()).length === 1, 'the child to acquire');
  // Several TTLs pass (6 s against a 2 s TTL); every competing attempt should be refused while the keeper runs.
  const attempts = [];
  const started = Date.now();
  let exited = false;
  child.exited.then(() => { exited = true; });
  while (Date.now() - started < 2 * ttlMs && !exited) {
    attempts.push((await attempt()).status);
    await new Promise(resolve => setTimeout(resolve, 40));
  }
  const { code } = await child.exited;
  assert.equal(code, 0);
  const { slowestRenewalMs, ...report } = JSON.parse(stdout);
  t.diagnostic(`slowest renewal ${slowestRenewalMs} ms against a ${ttlMs} ms TTL`);
  const takenOver = attempts.includes('LEASE_ACQUIRED');
  // Safety, under any load: if another process took the lease, the keeper must have reported the loss.
  if (takenOver) assert.equal(report.aborted, true, `lease taken over but keeper reported ${JSON.stringify(report)}`);
  // Liveness: with this much renewal headroom the keeper holds the lease throughout.
  assert.ok(attempts.length >= 3, `only ${attempts.length} attempts`);
  assert.equal(takenOver, false, 'another process acquired the lease while the keeper ran: either renewals stopped, ' +
    `or this process got no CPU for about ${ttlMs - renewEveryMs} ms (slowest renewal ${slowestRenewalMs} ms; keeper ${JSON.stringify(report)})`);
  assert.deepEqual(report, { aborted: false, reason: null, fencingToken: 1, stop: 'LEASE_RELEASED' });
  const next = await attempt();
  assert.equal(next.status, 'LEASE_ACQUIRED');
  assert.equal(next.lease.fencingToken, 2);
});

// Guard for the B3 fix: a lock this process holds right now stays alive.
test('a lock held by this process right now is reported alive and is never broken', async t => {
  const directory = await temporaryDirectory(t);
  const holder = new DirectoryLock({ directory, name: 'leases.lock', timeoutMs: 5_000 });
  const manager = fileManager(directory, { lockTimeoutMs: 200 });
  await holder.run(async () => {
    const view = await manager.inspectLock();
    assert.equal(view.holderState, 'ALIVE');
    await assert.rejects(manager.list(), error => error.code === 'LEASE_LOCK_TIMEOUT');
    await assert.rejects(manager.forceUnlock({ expectedLockId: view.lockId, explicitAuthorization: true }),
      error => error.code === 'LOCK_HOLDER_ALIVE');
  });
  assert.deepEqual(await manager.list(), []);
});

// A holder is identified by PID, boot ID and start time (Linux), so a PID that
// now belongs to another process (B3) no longer keeps a dead holder's lock.
const linux = process.platform === 'linux';
const staleRecord = (pid, identity, startTicks) => JSON.stringify({ token: randomUUID(), pid, host: hostname(),
  lockedAt: new Date(Date.now() - 60_000).toISOString(), bootId: identity.bootId, startTicks });

test('a lock left by an earlier process that had this PID is recovered', { skip: !linux }, async t => {
  const directory = await temporaryDirectory(t);
  // An earlier run with the same PID (PID 1 in a restarted container that kept
  // its volume and hostname) crashed while holding the lock.
  const own = processIdentity(process.pid);
  await writeFile(path.join(directory, 'leases.lock'), staleRecord(process.pid, own, String(BigInt(own.startTicks) - 1n)));
  const manager = fileManager(directory, { lockTimeoutMs: 500 });
  assert.equal((await manager.inspectLock()).holderState, 'DEAD');
  assert.deepEqual(await manager.list(), []);
});

test('a dead holder whose PID now belongs to another running process is recognized', { skip: !linux }, async t => {
  const directory = await temporaryDirectory(t);
  const other = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });
  t.after(() => other.kill('SIGKILL'));
  await new Promise(resolve => other.once('spawn', resolve));
  const identity = processIdentity(other.pid);
  const lockFile = path.join(directory, 'leases.lock');
  const manager = fileManager(directory, { lockTimeoutMs: 300 });
  // Guard: the same process (same start time) is alive and its lock is never broken.
  await writeFile(lockFile, staleRecord(other.pid, identity, identity.startTicks));
  assert.equal((await manager.inspectLock()).holderState, 'ALIVE');
  await assert.rejects(manager.list(), error => error.code === 'LEASE_LOCK_TIMEOUT');
  // Another start time: the holder died and its PID was reused.
  await writeFile(lockFile, staleRecord(other.pid, identity, String(BigInt(identity.startTicks) - 1n)));
  assert.equal((await manager.inspectLock()).holderState, 'DEAD');
  assert.deepEqual(await manager.list(), []);
});

test('a lock held by another thread of this process is alive and is never broken', async t => {
  const directory = await temporaryDirectory(t);
  const worker = workerThread(t, `
    const { DirectoryLock } = await import(workerData.lockUrl);
    const lock = new DirectoryLock({ directory: workerData.directory, name: 'leases.lock' });
    await lock.run(() => new Promise(resolve => { parentPort.postMessage('held'); parentPort.once('message', resolve); }));
    parentPort.postMessage('released');`, { directory, lockUrl: new URL('../src/directory-lock.mjs', import.meta.url).href });
  assert.equal(await worker.next(), 'held');
  const manager = fileManager(directory, { lockTimeoutMs: 300 });
  assert.equal((await manager.inspectLock()).holderState, 'ALIVE');
  await assert.rejects(manager.list(), error => error.code === 'LEASE_LOCK_TIMEOUT');
  const released = worker.next();
  worker.send('release');
  assert.equal(await released, 'released');
  assert.deepEqual(await manager.list(), []);
});

test('a lock that could not be removed neither fails the committed operation nor blocks the next one', async t => {
  const directory = await temporaryDirectory(t);
  const lock = new DirectoryLock({ directory, name: 'leases.lock', timeoutMs: 500 });
  failFsPromises(t, 'rm', target => target === lock.lockFile);
  assert.equal(await lock.run(async () => 'committed'), 'committed');
  assert.equal(await lock.run(async () => 'next'), 'next');
});
