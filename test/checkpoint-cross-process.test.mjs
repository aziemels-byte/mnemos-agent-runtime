import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import { hostname } from 'node:os';
import path from 'node:path';
import { FileCheckpointStore, FileIdempotencyRegistry, FilePhaseJobRegistry, FileResourceLeaseManager, GoalRecoveryCoordinator } from '../src/index.mjs';
import { temporaryDirectory, until, zombieProcess } from './helpers.mjs';

// Child processes import the module directly; each gets its own store instance.
const moduleUrl = new URL('../src/index.mjs', import.meta.url).href;
const childPrelude = `
  import { FileCheckpointStore, FileResourceLeaseManager, GoalRecoveryCoordinator } from ${JSON.stringify(moduleUrl)};
  const [directory, ...args] = process.argv.slice(1);
  const store = new FileCheckpointStore(directory, { lockTimeoutMs: 20000 });
  const emit = value => process.stdout.write(JSON.stringify(value));
`;

function startChild(source, args) {
  const child = spawn(process.execPath, ['--input-type=module', '-e', childPrelude + source, '--', ...args],
    { stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', chunk => { stdout += chunk; });
  child.stderr.on('data', chunk => { stderr += chunk; });
  child.exited = new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('close', code => resolve({ code, stdout, stderr }));
  });
  return child;
}

async function runChild(source, args) {
  const { code, stdout, stderr } = await startChild(source, args).exited;
  if (code !== 0) throw new Error(`child exited ${code}: ${stderr}`);
  return JSON.parse(stdout);
}

async function runChildren(count, source, args) {
  const results = await Promise.allSettled(Array.from({ length: count }, () => runChild(source, args)));
  const failure = results.find(result => result.status === 'rejected');
  if (failure) throw failure.reason;
  return results.map(result => result.value);
}


async function deadPid() {
  const exited = spawn(process.execPath, ['-e', '']);
  await new Promise(resolve => exited.on('close', resolve));
  return exited.pid;
}

test('concurrent read-modify-write cycles from several processes lose no update', async t => {
  const directory = await temporaryDirectory(t);
  // Each child increments the counter 25 times, retrying when another process won the race.
  const source = `
    let conflicts = 0;
    for (let i = 0; i < 25; i++) {
      for (;;) {
        const current = await store.read();
        const count = current?.value.count ?? 0;
        try {
          await store.save({ count: count + 1 }, { expectedRevision: current?.revision ?? null });
          break;
        } catch (error) {
          if (error.code !== 'CHECKPOINT_CONFLICT') throw error;
          conflicts++;
        }
      }
    }
    emit({ conflicts });`;
  const reader = new FileCheckpointStore(directory);
  let reads = 0;
  let readErrors = 0;
  let done = false;
  const polling = (async () => {
    while (!done) {
      try { await reader.read(); reads++; } catch { readErrors++; }
      await new Promise(resolve => setTimeout(resolve, 2));
    }
  })();
  const results = await runChildren(4, source, [directory]).finally(async () => {
    done = true;
    await polling;
  });
  assert.equal((await reader.load()).count, 100);
  assert.ok(results.reduce((sum, r) => sum + r.conflicts, 0) > 0, 'the processes really raced');
  assert.ok(reads > 0);
  assert.equal(readErrors, 0, 'lock-free readers never see a partial checkpoint');
  assert.deepEqual(await readdir(directory), ['checkpoint.json']);
});

test('a superseded coordinator in another process cannot overwrite the new checkpoint', async t => {
  const [directory, signals] = [await temporaryDirectory(t), await temporaryDirectory(t)];
  const first = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) });
  await first.checkpoint({ goal: { goalId: 'report' }, activeJobs: [{ id: 'attempt_1' }] });
  const go = path.join(signals, 'go');
  const seen = path.join(signals, 'seen');
  // The old owner recovers, stalls, and later tries to record its own view.
  const source = `
    import { existsSync, renameSync, writeFileSync } from 'node:fs';
    const coordinator = new GoalRecoveryCoordinator({ store });
    const recovered = await coordinator.recover();
    writeFileSync(args[1] + '.tmp', recovered.checkpointRevision);
    renameSync(args[1] + '.tmp', args[1]); // publish atomically
    const giveUp = Date.now() + 60000; // never outlive the test by long, even if it died
    while (!existsSync(args[0])) { if (Date.now() > giveUp) process.exit(3); await new Promise(resolve => setTimeout(resolve, 10)); }
    // No revision passed: the coordinator requires the one it recovered by default.
    const outcome = await coordinator.checkpoint({ goal: recovered.goal, activeJobs: [{ id: 'stale_attempt' }] })
      .then(() => 'SAVED', error => error.code);
    emit(outcome);`;
  const child = startChild(source, [directory, go, seen]);
  t.after(() => child.kill('SIGKILL'));
  await until(() => readFile(seen, 'utf8').then(() => true, () => false), 'the old owner to recover');

  const successor = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) });
  const recovered = await successor.recover();
  assert.equal(recovered.checkpointRevision, await readFile(seen, 'utf8'));
  await successor.checkpoint({ goal: recovered.goal, activeJobs: [{ id: 'attempt_2' }], expectedRevision: recovered.checkpointRevision });
  await writeFile(go, '');
  const { code, stdout } = await child.exited;
  assert.equal(code, 0);
  assert.equal(JSON.parse(stdout), 'CHECKPOINT_CONFLICT');
  assert.deepEqual((await successor.recover()).activeJobs, [{ id: 'attempt_2' }]);
});

test('a process killed mid-save never leaves a torn checkpoint or a stuck lock', async t => {
  const directory = await temporaryDirectory(t);
  const source = `
    for (let i = 1; ; i++) await store.save({ i, padding: 'x'.repeat(20000) });`;
  for (let round = 0; round < 3; round++) {
    const child = startChild(source, [directory]);
    t.after(() => child.kill('SIGKILL'));
    await until(async () => ((await new FileCheckpointStore(directory).load().catch(() => null))?.i ?? 0) > 10 * (round + 1),
      'the child to save repeatedly');
    await new Promise(resolve => setTimeout(resolve, Math.random() * 20));
    child.kill('SIGKILL');
    await child.exited;
    const store = new FileCheckpointStore(directory, { lockTimeoutMs: 5000 });
    const survived = await store.read(); // whatever the kill point, the file is a complete checkpoint
    assert.equal(typeof survived.value.i, 'number');
    await store.save({ round }, { expectedRevision: survived.revision });
    assert.deepEqual(await readdir(directory), ['checkpoint.json']);
  }
});

test('a stuck checkpoint lock blocks writers but not recovery reads, and operators can clear it', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileCheckpointStore(directory, { lockTimeoutMs: 50 });
  const coordinator = new GoalRecoveryCoordinator({ store });
  await coordinator.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [{ id: 'job_1' }] });
  const foreign = JSON.stringify({ token: randomUUID(), pid: 4242, host: 'other-host.invalid', lockedAt: new Date().toISOString() });
  await writeFile(store.lockFile, foreign);

  await assert.rejects(store.save({}), error => error.code === 'CHECKPOINT_LOCK_TIMEOUT' && error.lockFile === store.lockFile &&
    error.holderState === 'OTHER_HOST' && error.holder.pid === 4242 && /checkpoint\.lock: held by PID 4242/.test(error.message));
  assert.equal((await coordinator.recover()).goal.goalId, 'goal'); // reads never wait for the lock

  const inspected = await store.inspectLock();
  assert.equal(inspected.holderState, 'OTHER_HOST');
  assert.equal((await store.forceUnlock({ expectedLockId: inspected.lockId, explicitAuthorization: true })).status, 'LOCK_FORCE_RELEASED');
  await coordinator.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [] });

  // A lock from a dead holder on this host is recovered automatically.
  await writeFile(store.lockFile, JSON.stringify({ token: randomUUID(), pid: await deadPid(), host: hostname() }));
  await new FileCheckpointStore(directory, { lockTimeoutMs: 2000 }).save({ recovered: true });
  assert.deepEqual(await readdir(directory), ['checkpoint.json']);
});

test('lease and checkpoint locks can share a directory without disturbing each other', async t => {
  const directory = await temporaryDirectory(t);
  const source = `
    const leases = new FileResourceLeaseManager({ directory, lockTimeoutMs: 20000,
      nodeRegistry: { require: () => ({ trust: 'VERIFIED', status: 'AVAILABLE' }) } });
    let saved = 0, leased = 0;
    for (let i = 0; i < 20; i++) {
      const [result] = await Promise.all([
        leases.acquire({ ownerId: 'proc:' + process.pid, nodeId: 'local', resources: ['cpu:' + process.pid], explicitAuthorization: true }),
        store.save({ pid: process.pid, i }).then(() => { saved++; })
      ]);
      await leases.release({ leaseId: result.lease.leaseId, ownerId: 'proc:' + process.pid, explicitAuthorization: true });
      leased++;
    }
    emit({ saved, leased });`;
  const results = await runChildren(3, source, [directory]);
  assert.deepEqual(results, Array.from({ length: 3 }, () => ({ saved: 20, leased: 20 })));
  assert.deepEqual((await readdir(directory)).sort(), ['checkpoint.json', 'leases.json']);
  const leases = new FileResourceLeaseManager({ directory, nodeRegistry: { require: () => ({ trust: 'VERIFIED', status: 'AVAILABLE' }) } });
  assert.equal((await leases.snapshot()).lastFencingToken, 60);

  // Each lock sweeps only its own break claims: removing another lock's claim while
  // its breaker works would let a second breaker remove that lock's next holder.
  const leaseClaim = `leases.lock.${randomUUID()}.break`;
  const checkpointClaim = `checkpoint.lock.${randomUUID()}.break`;
  for (const name of [leaseClaim, checkpointClaim]) await writeFile(path.join(directory, name), JSON.stringify({ pid: process.pid, host: hostname() }));
  await new FileCheckpointStore(directory).save({ swept: true });
  assert.deepEqual((await readdir(directory)).sort(), ['checkpoint.json', leaseClaim, 'leases.json'].sort());
  await leases.list();
  assert.deepEqual((await readdir(directory)).sort(), ['checkpoint.json', 'leases.json']);
});

test('a lock held by a process that exited but was not reaped is recovered', { skip: process.platform !== 'linux' }, async t => {
  const directory = await temporaryDirectory(t);
  const { pid, parentPid } = await zombieProcess(t);
  const store = new FileCheckpointStore(directory, { lockTimeoutMs: 1000 });
  const lockedBy = holder => writeFile(store.lockFile,
    JSON.stringify({ token: randomUUID(), pid: holder, host: hostname(), lockedAt: new Date().toISOString() }));

  await lockedBy(parentPid); // alive: never broken
  assert.equal((await store.inspectLock()).holderState, 'ALIVE');
  await assert.rejects(store.save({ blocked: true }), { code: 'CHECKPOINT_LOCK_TIMEOUT' });

  await lockedBy(pid); // exited, only awaiting reaping: provably gone
  assert.equal((await store.inspectLock()).holderState, 'DEAD');
  await store.save({ recovered: true });
  assert.deepEqual(await store.load(), { recovered: true });
  assert.equal((await store.inspectLock()).status, 'UNLOCKED');
});

test('a state path that is not a regular file fails closed in every store, never read as absent', async t => {
  const directory = await temporaryDirectory(t);
  const sha256 = text => createHash('sha256').update(text).digest('hex');
  for (const name of ['checkpoint.json', 'leases.json', `${sha256('report:weekly')}.record.json`, `${sha256('job_1')}.job.json`]) {
    await mkdir(path.join(directory, name));
  }
  const must = () => { throw new Error('must not run'); };
  assert.equal((await new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) }).recover()).recoveryError,
    'CHECKPOINT_UNREADABLE');
  await assert.rejects(new FileResourceLeaseManager({ directory, nodeRegistry: { require: () => ({ trust: 'VERIFIED', status: 'AVAILABLE' }) } })
    .list(), { code: 'LEASE_STATE_INVALID' });
  await assert.rejects(new FileIdempotencyRegistry({ directory }).execute({ key: 'report:weekly', operation: must }),
    { code: 'IDEMPOTENCY_STATE_INVALID' });
  await assert.rejects(new FilePhaseJobRegistry({ directory, execute: must }).start({ jobId: 'job_1', objective: 'Never starts' }),
    { code: 'JOB_STATE_INVALID' });
});
