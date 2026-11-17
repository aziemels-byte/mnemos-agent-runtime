import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { hostname } from 'node:os';
import path from 'node:path';
import { FileIdempotencyRegistry } from '../src/index.mjs';
import { temporaryDirectory, until, withTimeout } from './helpers.mjs';

// Child processes import the module directly; each gets its own registry instance.
const moduleUrl = new URL('../src/index.mjs', import.meta.url).href;
const childPrelude = `
  import { appendFile, writeFile } from 'node:fs/promises';
  import { FileIdempotencyRegistry } from ${JSON.stringify(moduleUrl)};
  const [directory, effects, ...args] = process.argv.slice(1);
  const registry = new FileIdempotencyRegistry({ directory, lockTimeoutMs: 20000 });
  const emit = value => process.stdout.write(JSON.stringify(value));
  const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
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


const lines = async file => (await readFile(file, 'utf8').catch(() => '')).split('\n').filter(Boolean);

test('processes racing on one key perform the effect exactly once', async t => {
  const [directory, scratch] = [await temporaryDirectory(t), await temporaryDirectory(t)];
  const effects = path.join(scratch, 'effects.log');
  const go = path.join(scratch, 'go');
  // Start barrier: every child is loaded and waiting before any of them races.
  const source = `
    import { existsSync } from 'node:fs';
    await appendFile(args[0], 'ready\\n');
    const giveUp = Date.now() + 60000; // never outlive the test by long, even if it died
    while (!existsSync(args[1])) { if (Date.now() > giveUp) process.exit(3); await sleep(2); }
    const result = await registry.execute({ key: 'payment:42', request: { amount: 10 }, operation: async () => {
      await appendFile(effects, process.pid + '\\n');
      await sleep(300);
      return { chargedBy: process.pid };
    } });
    emit(result);`;
  const children = Array.from({ length: 6 }, () => startChild(source, [directory, effects, path.join(scratch, 'ready.log'), go]));
  t.after(() => { for (const child of children) child.kill('SIGKILL'); }); // a failed wait must not leave children waiting
  // Starting six Node processes on a loaded machine can take many seconds.
  await until(async () => (await lines(path.join(scratch, 'ready.log'))).length === 6, 'every child to be ready', 60_000);
  await writeFile(go, '');
  const results = await Promise.all(children.map(async child => {
    const { code, stdout, stderr } = await child.exited;
    if (code !== 0) throw new Error(`child exited ${code}: ${stderr}`);
    return JSON.parse(stdout);
  }));
  const executed = results.filter(result => result.status === 'IDEMPOTENCY_EXECUTED');
  assert.equal((await lines(effects)).length, 1);
  assert.equal(executed.length, 1);
  assert.ok(results.every(result => ['IDEMPOTENCY_EXECUTED', 'IDEMPOTENCY_IN_PROGRESS', 'IDEMPOTENT_REPLAY'].includes(result.status)));
  assert.ok(results.some(result => result.status === 'IDEMPOTENCY_IN_PROGRESS'), 'the processes really overlapped');
  for (const replay of results.filter(result => result.status === 'IDEMPOTENT_REPLAY')) {
    assert.deepEqual(replay.result, executed[0].result);
  }
  const later = await new FileIdempotencyRegistry({ directory }).execute({ key: 'payment:42', request: { amount: 10 },
    operation: () => { throw Error('must not execute'); } });
  assert.deepEqual(later.result, executed[0].result);
});

test('many keys contended by many processes each run exactly once', async t => {
  const [directory, scratch] = [await temporaryDirectory(t), await temporaryDirectory(t)];
  // Every process attempts every key in its own order. A second effect for a key
  // would fail its exclusive create and be counted as a duplicate.
  const source = `
    const keys = Array.from({ length: 15 }, (_, i) => 'task:' + i).sort(() => Math.random() - 0.5);
    const statuses = {};
    let duplicates = 0;
    for (const key of keys) {
      const result = await registry.execute({ key, operation: async () => {
        try { await writeFile(effects + '/' + key.replace(':', '_'), String(process.pid), { flag: 'wx' }); }
        catch { duplicates++; }
        await sleep(5);
        return key;
      } });
      statuses[result.status] = (statuses[result.status] ?? 0) + 1;
    }
    emit({ statuses, duplicates });`;
  const results = await Promise.all(Array.from({ length: 4 }, () => runChild(source, [directory, scratch])));
  assert.equal(results.reduce((sum, r) => sum + r.duplicates, 0), 0);
  assert.equal(results.reduce((sum, r) => sum + (r.statuses.IDEMPOTENCY_EXECUTED ?? 0), 0), 15);
  assert.equal((await readdir(scratch)).length, 15);
  assert.deepEqual((await new FileIdempotencyRegistry({ directory }).snapshot()).states, { COMPLETED: 15 });
});

test('a process killed mid-effect leaves the key UNKNOWN for everyone, never re-run', async t => {
  const [directory, scratch] = [await temporaryDirectory(t), await temporaryDirectory(t)];
  const effects = path.join(scratch, 'effects.log');
  const source = `
    await registry.execute({ key: 'deploy:7', operation: async () => {
      await appendFile(effects, process.pid + '\\n');
      await sleep(60000);
    } });`;
  const child = startChild(source, [directory, effects]);
  t.after(() => child.kill('SIGKILL'));
  await until(async () => (await lines(effects)).length === 1, 'the effect to start');

  const observer = new FileIdempotencyRegistry({ directory });
  const mustNotRun = () => { throw Error('must not execute'); };
  assert.equal((await observer.execute({ key: 'deploy:7', operation: mustNotRun })).status, 'IDEMPOTENCY_IN_PROGRESS');
  assert.equal((await observer.inspect('deploy:7')).ownerState, 'ALIVE');

  child.kill('SIGKILL');
  await child.exited;
  assert.equal((await observer.inspect('deploy:7')).ownerState, 'DEAD');
  const after = await observer.execute({ key: 'deploy:7', operation: mustNotRun });
  assert.equal(after.status, 'IDEMPOTENCY_UNKNOWN');
  assert.equal((await observer.inspect('deploy:7')).reason, 'INTERRUPTED_AFTER_EFFECT_START');
  assert.equal((await lines(effects)).length, 1);

  // Only authoritative evidence resolves it; afterwards the outcome replays.
  assert.equal((await observer.reconcile({ key: 'deploy:7', disposition: 'COMPLETED', result: { version: 7 },
    evidence: { authoritative: true, source: 'deployment API' } })).status, 'IDEMPOTENCY_RECONCILED_COMPLETED');
  assert.deepEqual((await observer.execute({ key: 'deploy:7', operation: mustNotRun })).result, { version: 7 });
});

test('another process can neither run nor reconcile a key while its owner is alive', async t => {
  const directory = await temporaryDirectory(t);
  const owner = new FileIdempotencyRegistry({ directory });
  let release;
  const released = new Promise(resolve => { release = resolve; });
  let calls = 0;
  const running = owner.execute({ key: 'report:q3', operation: async () => { calls++; await released; return 'q3.pdf'; } });
  t.after(async () => { release(); await running; });
  await until(async () => (await owner.inspect('report:q3'))?.state === 'STARTED', 'the reservation');

  const source = `
    const attempt = await registry.execute({ key: 'report:q3', operation: () => { throw Error('must not execute'); } });
    const reconcile = await registry.reconcile({ key: 'report:q3', disposition: 'FAILED_NO_EFFECT',
      evidence: { authoritative: true, effectAbsent: true } }).then(r => r.status, e => e.message);
    emit({ attempt: attempt.status, reconcile });`;
  assert.deepEqual(await runChild(source, [directory, '']),
    { attempt: 'IDEMPOTENCY_IN_PROGRESS', reconcile: 'Cannot reconcile while the operation is running' });
  release();
  assert.equal((await running).status, 'IDEMPOTENCY_EXECUTED');
  const replay = `emit(await registry.execute({ key: 'report:q3', operation: () => { throw Error('must not execute'); } }));`;
  assert.deepEqual(await runChild(replay, [directory, '']), { status: 'IDEMPOTENT_REPLAY', key: 'report:q3', executed: false, result: 'q3.pdf' });
  assert.equal(calls, 1);
});

test('an outcome that could not be recorded at first is recorded while its owner keeps running', async t => {
  const directory = await temporaryDirectory(t);
  const lockFile = path.join(directory, 'idempotency.lock');
  // A holder that cannot be checked (another host) blocks the outcome write until it goes away.
  const foreignLock = JSON.stringify({ token: randomUUID(), pid: 4242, host: 'other-host.invalid', lockedAt: new Date().toISOString() });
  const registry = new FileIdempotencyRegistry({ directory, lockTimeoutMs: 100 });
  let calls = 0;
  const outcome = await registry.execute({ key: 'export:9', operation: async () => {
    calls++;
    await writeFile(lockFile, foreignLock);
    return 'exported';
  } }).catch(error => error);
  assert.equal(outcome.idempotency?.status, 'IDEMPOTENCY_UNKNOWN');
  assert.equal((await registry.inspect('export:9')).state, 'STARTED'); // nothing could be written yet

  await rm(lockFile); // the blocking holder is gone; this owner process is still running
  const observe = `emit(await registry.execute({ key: 'export:9', operation: () => { throw Error('must not execute'); } }));`;
  // The outcome this owner knows is written in the background, not downgraded to UNKNOWN.
  await until(async () => (await registry.inspect('export:9')).state !== 'STARTED', 'the outcome to be recorded', 5_000);
  const recorded = await registry.inspect('export:9');
  assert.deepEqual([recorded.state, recorded.result], ['COMPLETED', 'exported']);
  // Another process sees the result, not a key stuck IN_PROGRESS for this process's lifetime.
  const seen = await runChild(observe, [directory, '']);
  assert.deepEqual([seen.status, seen.result], ['IDEMPOTENT_REPLAY', 'exported']);
  assert.equal(calls, 1);
});

test('the background UNKNOWN write never replaces a record resolved by someone else', async t => {
  const directory = await temporaryDirectory(t);
  const lockFile = path.join(directory, 'idempotency.lock');
  const foreignLock = JSON.stringify({ token: randomUUID(), pid: 4242, host: 'other-host.invalid', lockedAt: new Date().toISOString() });
  const registry = new FileIdempotencyRegistry({ directory, lockTimeoutMs: 100 });
  await registry.execute({ key: 'export:10', operation: async () => { await writeFile(lockFile, foreignLock); return 'x'; } }).catch(() => {});
  const started = await registry.inspect('export:10');
  assert.equal(started.state, 'STARTED');

  // While the lock is still blocked, the record is resolved elsewhere (for example by an operator).
  const file = path.join(directory, (await readdir(directory)).find(name => name.endsWith('.record.json')));
  const resolved = { ...started, state: 'COMPLETED', result: 'confirmed by the export service', reconciliationRequired: false,
    owner: { ...started.owner, executionId: 'operator-resolution' } };
  delete resolved.ownerState;
  await writeFile(file, JSON.stringify({ schema: 'mnemos.idempotency-record.v1', record: resolved }));
  await rm(lockFile);
  await new Promise(resolve => setTimeout(resolve, 1_000)); // several retry intervals (25, 50, 100, 200, 400 ms ...)
  const after = await registry.inspect('export:10');
  assert.deepEqual([after.state, after.result], ['COMPLETED', 'confirmed by the export service']);
});

test('pending background writes never keep a finished process alive', async t => {
  const directory = await temporaryDirectory(t);
  // The lock stays blocked forever; the child finishes its work and must still exit.
  const source = `
    const blocked = new FileIdempotencyRegistry({ directory, lockTimeoutMs: 100 });
    const lock = JSON.stringify({ token: '${randomUUID()}', pid: 4242, host: 'other-host.invalid' });
    const outcome = await blocked.execute({ key: 'export:11', operation: async () => { await writeFile(directory + '/idempotency.lock', lock); return 1; } })
      .then(r => r.status, e => e.idempotency?.status);
    emit(outcome);`;
  const started = Date.now();
  const child = startChild(source, [directory, '']);
  t.after(() => child.kill('SIGKILL'));
  const exit = await withTimeout(child.exited, 5_000, 'still running').catch(error => error.message);
  assert.notEqual(exit, 'still running', 'the child was kept alive by retry timers');
  assert.equal(exit.code, 0);
  assert.equal(JSON.parse(exit.stdout), 'IDEMPOTENCY_UNKNOWN');
  assert.ok(Date.now() - started < 5_000);
});

const fingerprintOf = request => createHash('sha256').update(JSON.stringify(request)).digest('hex');

test('an operator can resolve a key whose dead owner\'s PID now belongs to another process', async t => {
  const directory = await temporaryDirectory(t);
  // An unrelated live process holds the PID recorded for an owner that crashed long ago.
  const bystander = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });
  t.after(() => bystander.kill('SIGKILL'));
  const key = 'refund:7';
  const orphan = { key, fingerprint: fingerprintOf({}), state: 'STARTED', startedAt: new Date().toISOString(), attempt: 1,
    owner: { pid: bystander.pid, host: hostname(), executionId: 'execution-that-crashed' } };
  const file = path.join(directory, `${createHash('sha256').update(key).digest('hex')}.record.json`);
  await writeFile(file, JSON.stringify({ schema: 'mnemos.idempotency-record.v1', record: orphan }));

  // Without an override the key is stuck: it looks like a running execution.
  const registry = new FileIdempotencyRegistry({ directory });
  const mustNotRun = () => { throw Error('must not execute'); };
  assert.equal((await registry.execute({ key, operation: mustNotRun })).status, 'IDEMPOTENCY_IN_PROGRESS');
  const evidence = { authoritative: true, effectAbsent: true, source: 'payment provider: no refund exists' };
  await assert.rejects(registry.reconcile({ key, disposition: 'FAILED_NO_EFFECT', evidence }), /operation is running/);
  const inspected = await registry.inspect(key);
  assert.deepEqual([inspected.ownerState, inspected.owner.executionId], ['ALIVE', 'execution-that-crashed']);

  // The override needs authorization, the exact inspected execution and authoritative evidence.
  const override = { key, disposition: 'FAILED_NO_EFFECT', evidence, expectedExecutionId: inspected.owner.executionId };
  await assert.rejects(registry.reconcile(override), /Explicit authorization/);
  await assert.rejects(registry.reconcile({ ...override, explicitAuthorization: true, expectedExecutionId: 'another-execution' }),
    error => error.code === 'IDEMPOTENCY_RECORD_CHANGED');
  assert.equal((await registry.reconcile({ ...override, explicitAuthorization: true, evidence: { effectAbsent: true } })).status,
    'IDEMPOTENCY_EVIDENCE_REQUIRED');
  assert.equal((await registry.inspect(key)).state, 'STARTED'); // refusals change nothing

  assert.equal((await registry.reconcile({ ...override, explicitAuthorization: true })).status, 'IDEMPOTENCY_RECONCILED_FAILED');
  const resolved = await registry.inspect(key);
  assert.equal(resolved.state, 'FAILED');
  assert.deepEqual([resolved.liveOwnerOverride.executionId, resolved.liveOwnerOverride.owner.pid], ['execution-that-crashed', bystander.pid]);
  let calls = 0;
  assert.equal((await registry.execute({ key, retryFailed: true, operation: () => { calls++; return 'refunded'; } })).status, 'IDEMPOTENCY_EXECUTED');
  assert.equal(calls, 1);
});

test('the override is refused for an execution running in this process', async t => {
  const directory = await temporaryDirectory(t);
  const [owner, operator] = [new FileIdempotencyRegistry({ directory }), new FileIdempotencyRegistry({ directory })];
  let release;
  const released = new Promise(resolve => { release = resolve; });
  const running = owner.execute({ key: 'sync:1', operation: () => released.then(() => 'synced') });
  t.after(async () => { release(); await running; });
  await until(async () => (await operator.inspect('sync:1'))?.state === 'STARTED', 'the reservation');
  const { owner: { executionId } } = await operator.inspect('sync:1');
  const override = { key: 'sync:1', disposition: 'COMPLETED', evidence: { authoritative: true }, expectedExecutionId: executionId, explicitAuthorization: true };
  await assert.rejects(operator.reconcile(override), /operation is running/);
  await assert.rejects(owner.reconcile(override), /operation is running/);
  release();
  assert.equal((await running).status, 'IDEMPOTENCY_EXECUTED');
});

test('if an overridden owner was in fact alive, its late outcome is refused and it reports UNKNOWN', async t => {
  const [directory, scratch] = [await temporaryDirectory(t), await temporaryDirectory(t)];
  const go = path.join(scratch, 'go');
  const source = `
    import { existsSync } from 'node:fs';
    const outcome = await registry.execute({ key: 'ship:3', operation: async () => {
      const giveUp = Date.now() + 60000;
      while (!existsSync(args[0])) { if (Date.now() > giveUp) process.exit(3); await sleep(10); }
      return 'shipped by the owner';
    } }).then(r => r.status, e => e.idempotency?.status);
    emit(outcome);`;
  const child = startChild(source, [directory, '', go]);
  t.after(() => child.kill('SIGKILL'));
  const operator = new FileIdempotencyRegistry({ directory });
  await until(async () => (await operator.inspect('ship:3'))?.state === 'STARTED', 'the reservation');
  const inspected = await operator.inspect('ship:3');
  assert.equal(inspected.ownerState, 'ALIVE'); // the operator overrides anyway (wrongly)
  assert.equal((await operator.reconcile({ key: 'ship:3', disposition: 'COMPLETED', result: 'confirmed by carrier',
    evidence: { authoritative: true }, expectedExecutionId: inspected.owner.executionId, explicitAuthorization: true })).status,
    'IDEMPOTENCY_RECONCILED_COMPLETED');

  await writeFile(go, '');
  const { code, stdout } = await child.exited;
  assert.equal(code, 0);
  assert.equal(JSON.parse(stdout), 'IDEMPOTENCY_UNKNOWN'); // the owner learns its outcome was not recorded
  const final = await operator.inspect('ship:3');
  assert.deepEqual([final.state, final.result], ['COMPLETED', 'confirmed by carrier']); // never overwritten
});

test('a lock timeout before reservation names the holder and never runs the effect', async t => {
  const directory = await temporaryDirectory(t);
  await writeFile(path.join(directory, 'idempotency.lock'),
    JSON.stringify({ token: randomUUID(), pid: 4242, host: 'other-host.invalid', lockedAt: new Date().toISOString() }));
  let calls = 0;
  const error = await new FileIdempotencyRegistry({ directory, lockTimeoutMs: 50 })
    .execute({ key: 'email:1', operation: () => { calls++; } }).catch(error => error);
  assert.equal(error.code, 'IDEMPOTENCY_LOCK_TIMEOUT');
  assert.equal(error.holderState, 'OTHER_HOST');
  assert.match(error.message, /idempotency\.lock: held by PID 4242 on other-host\.invalid/);
  assert.equal(calls, 0);
  assert.equal(await new FileIdempotencyRegistry({ directory }).inspect('email:1'), null);
});
