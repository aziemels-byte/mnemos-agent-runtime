import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { hostname } from 'node:os';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { FilePhaseJobRegistry } from '../src/index.mjs';
import { identityState, processIdentity } from '../src/directory-lock.mjs';
import { FileJobStore } from '../src/job-store.mjs';
import { temporaryDirectory, withTimeout } from './helpers.mjs';

const linux = process.platform === 'linux';
const forever = 'setInterval(() => {}, 1000);';
const noExecute = async () => { throw new Error('recovery must never run a job'); };
const evidence = { authoritative: true, source: 'deploy-api:release/7' };
const kill = pid => { try { process.kill(pid, 'SIGKILL'); } catch {} };
const killGroup = pgid => { try { process.kill(-pgid, 'SIGKILL'); } catch {} };
const gone = pid => identityState({ pid, host: hostname() }) === 'DEAD';

async function until(predicate, label, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}`);
    await sleep(20);
  }
}

// The job's command: a leader with a grandchild in its group and an escapee in its
// own session (keeping the environment), each recording its PID once running.
const deployTree = file => {
  const record = name => `require('node:fs').appendFileSync(${JSON.stringify(file)}, JSON.stringify({ ${name}: process.pid }) + '\\n');`;
  const grandchild = `require('node:child_process').spawn(process.execPath, ['-e', ${JSON.stringify(record('grandchild') + forever)}], { stdio: 'ignore' });`;
  const escapee = `require('node:child_process').spawn(process.execPath, ['-e', ${JSON.stringify(record('escapee') + forever)}], { stdio: 'ignore', detached: true });`;
  return grandchild + escapee + record('leader') + forever;
};

async function pidsOf(file) {
  let pids = {};
  await until(async () => {
    try { pids = Object.assign({}, ...(await readFile(file, 'utf8')).trim().split('\n').map(line => JSON.parse(line))); }
    catch { return false; }
    return ['leader', 'grandchild', 'escapee'].every(name => pids[name]);
  }, 'the job\'s processes to run');
  return pids;
}

/**
 * Run a job in an owner process, then kill its supervisor and the owner (SIGKILL,
 * supervisor first): a double crash that leaves the job's processes running.
 */
async function orphanedJob(t, directory, jobId = 'deploy') {
  const file = path.join(directory, `${jobId}.pids`);
  const registryUrl = new URL('../src/index.mjs', import.meta.url).href;
  const owner = spawn(process.execPath, ['--input-type=module', '-e', `
    import { FilePhaseJobRegistry } from ${JSON.stringify(registryUrl)};
    const jobs = new FilePhaseJobRegistry({ directory: ${JSON.stringify(directory)}, execute: ({ runProcess }) =>
      runProcess({ command: process.execPath, args: ['-e', ${JSON.stringify(deployTree(file))}] }) });
    await jobs.start({ jobId: ${JSON.stringify(jobId)}, objective: 'Deploy' });`], { stdio: 'ignore' });
  const ownerExited = new Promise(resolve => owner.once('exit', resolve));
  t.after(() => owner.kill('SIGKILL'));
  const pids = await pidsOf(file);
  const store = new FileJobStore({ directory });
  await until(async () => (await store.read(jobId))?.processes?.[0]?.leader != null, 'the leader to be recorded');
  const { processes } = await store.read(jobId);
  const supervisor = processes[0].supervisor.pid;
  t.after(() => { for (const pid of [...Object.values(pids), supervisor]) kill(pid); killGroup(pids.leader); });
  kill(supervisor);
  await until(() => gone(supervisor), 'the supervisor to die');
  owner.kill('SIGKILL');
  await withTimeout(ownerExited, 10_000, 'the owner did not exit');
  return { pids, supervisor, marker: processes[0].marker };
}

test('after the owner and its supervisor both die, the job stays RUNNING while its processes run', { skip: !linux }, async t => {
  const directory = await temporaryDirectory(t);
  const { pids } = await orphanedJob(t, directory);
  assert.ok(!gone(pids.leader) && !gone(pids.grandchild) && !gone(pids.escapee), 'nobody stopped the tree');

  const jobs = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const view = await jobs.get('deploy');
  assert.deepEqual([view.status, view.ownerState, view.orphaned, view.processesState], ['RUNNING', 'DEAD', true, 'RUNNING']);
  assert.deepEqual((await jobs.recover()).orphaned, ['deploy']);
  assert.equal((await jobs.wait('deploy', { timeoutMs: 300 })).status, 'RUNNING', 'the work is still running');
  await assert.rejects(jobs.reconcile({ jobId: 'deploy', expectedSequence: view.sequence, disposition: 'FAILED', evidence,
    explicitAuthorization: true }), error => error.code === 'JOB_PROCESSES_RUNNING' && error.processesState === 'RUNNING');
  await assert.rejects(jobs.requestCancellation('deploy'), error => error.code === 'JOB_NOT_OWNED' && error.orphaned === true);
  assert.equal((await new FileJobStore({ directory }).read('deploy')).status, 'RUNNING', 'nothing was recorded');
});

test('terminateOrphans stops the orphaned tree, including descendants that left its group, and records it', { skip: !linux }, async t => {
  const directory = await temporaryDirectory(t);
  const { pids, marker } = await orphanedJob(t, directory);
  const jobs = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const { sequence } = await jobs.get('deploy');

  const { confirmed, receipts, job } = await jobs.terminateOrphans({ jobId: 'deploy', expectedSequence: sequence,
    explicitAuthorization: true, graceMs: 2000 });
  assert.equal(confirmed, true);
  assert.deepEqual([receipts.length, receipts[0].marker, receipts[0].pgid, receipts[0].frozen], [1, marker, pids.leader, true]);
  assert.ok(receipts[0].stopped.includes(pids.escapee), 'the escapee was found by its marker');
  assert.ok(gone(pids.leader) && gone(pids.grandchild) && gone(pids.escapee));
  assert.deepEqual([job.status, job.reason, job.reconciliationRequired], ['UNKNOWN', 'ORPHANS_TERMINATED', true]);
  assert.deepEqual(job.orphanTermination.receipts, receipts);

  // Durable; and now the job can be reconciled as usual. The audit stays.
  const restarted = new FilePhaseJobRegistry({ directory, execute: noExecute });
  assert.deepEqual(await restarted.get('deploy'), job);
  const reconciled = await restarted.reconcile({ jobId: 'deploy', expectedSequence: job.sequence, disposition: 'FAILED',
    evidence: { ...evidence, effectAbsent: true }, explicitAuthorization: true });
  assert.deepEqual([reconciled.status, reconciled.orphanTermination.receipts], ['FAILED', receipts]);
});

test('a tree whose leader was never recorded is found by its marker', { skip: !linux }, async t => {
  const directory = await temporaryDirectory(t);
  const marker = `MNEMOS_TREE_${randomUUID().replaceAll('-', '')}`;
  // As if the owner crashed right after the go-ahead: the tree is recorded, its leader is not.
  const orphan = spawn(process.execPath, ['-e', forever], { stdio: 'ignore', detached: true, env: { ...process.env, [marker]: '1' } });
  t.after(() => kill(orphan.pid));
  await new Promise(resolve => orphan.once('spawn', resolve));
  const dead = spawn(process.execPath, ['-e', '']);
  await new Promise(resolve => dead.once('close', resolve));
  const store = new FileJobStore({ directory });
  let { record } = await store.create({ jobId: 'leaderless', objective: 'Crashed early', fingerprint: 'c'.repeat(64),
    owner: { pid: dead.pid, host: hostname(), executionId: 'gone' } });
  record = await store.update('leaderless', { expectedSequence: record.sequence, changes: { dispatched: true,
    dispatchedAt: new Date().toISOString(), processes: [{ marker, supervisor: { pid: dead.pid, host: hostname() }, leader: null,
      recordedAt: new Date().toISOString() }] } });

  const jobs = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const view = await jobs.get('leaderless');
  assert.deepEqual([view.status, view.orphaned], ['RUNNING', true]);
  const { confirmed, receipts, job } = await jobs.terminateOrphans({ jobId: 'leaderless', expectedSequence: view.sequence,
    explicitAuthorization: true, graceMs: 2000 });
  assert.deepEqual([confirmed, receipts[0].pgid, receipts[0].stopped], [true, null, [orphan.pid]]);
  assert.ok(gone(orphan.pid));
  assert.equal(job.reason, 'ORPHANS_TERMINATED');
});

test('a stop that cannot be verified is never recorded as one', { skip: !linux }, async t => {
  const directory = await temporaryDirectory(t);
  const marker = `MNEMOS_TREE_${randomUUID().replaceAll('-', '')}`;
  // Many processes that ignore SIGTERM: with no time to wait, their exit cannot be verified yet.
  const orphans = Array.from({ length: 20 }, () => spawn(process.execPath, ['-e', `process.on('SIGTERM', () => {}); ${forever}`],
    { stdio: 'ignore', detached: true, env: { ...process.env, [marker]: '1' } }));
  t.after(() => { for (const orphan of orphans) orphan.kill('SIGKILL'); });
  await Promise.all(orphans.map(orphan => new Promise(resolve => orphan.once('spawn', resolve))));
  await sleep(300); // the handlers are installed
  const dead = spawn(process.execPath, ['-e', '']);
  await new Promise(resolve => dead.once('close', resolve));
  const store = new FileJobStore({ directory });
  let { record } = await store.create({ jobId: 'stubborn', objective: 'Hard to stop', fingerprint: 'f'.repeat(64),
    owner: { pid: dead.pid, host: hostname(), executionId: 'gone' } });
  record = await store.update('stubborn', { expectedSequence: record.sequence, changes: { dispatched: true,
    dispatchedAt: new Date().toISOString(), processes: [{ marker, supervisor: { pid: dead.pid, host: hostname() }, leader: null,
      recordedAt: new Date().toISOString() }] } });

  const jobs = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const hurried = await jobs.terminateOrphans({ jobId: 'stubborn', expectedSequence: record.sequence, explicitAuthorization: true,
    graceMs: 0, killWaitMs: 0 });
  if (hurried.confirmed) return t.skip('the processes happened to exit at once');
  // Once they are gone, the job is reclassified as usual; until then it stays RUNNING.
  // Either way, the unverified stop is not recorded as a termination.
  assert.notEqual(hurried.job.reason, 'ORPHANS_TERMINATED');
  assert.equal((await store.read('stubborn')).orphanTermination, undefined);
  const final = await jobs.wait('stubborn', { timeoutMs: 10_000 });
  assert.deepEqual([final.status, final.reason], ['UNKNOWN', 'OWNER_DIED_AFTER_DISPATCH']);
});

test('a recorded leader whose PID now belongs to another process is neither counted nor signalled', { skip: !linux }, async t => {
  const directory = await temporaryDirectory(t);
  const unrelated = spawn(process.execPath, ['-e', forever], { stdio: 'ignore' });
  t.after(() => unrelated.kill('SIGKILL'));
  await new Promise(resolve => unrelated.once('spawn', resolve));
  const identity = processIdentity(unrelated.pid);
  const dead = spawn(process.execPath, ['-e', '']);
  await new Promise(resolve => dead.once('close', resolve));
  const store = new FileJobStore({ directory });
  let { record } = await store.create({ jobId: 'reused', objective: 'Long ago', fingerprint: 'd'.repeat(64),
    owner: { pid: dead.pid, host: hostname(), executionId: 'gone' } });
  await store.update('reused', { expectedSequence: record.sequence, changes: { dispatched: true, dispatchedAt: new Date().toISOString(),
    processes: [{ marker: `MNEMOS_TREE_${'e'.repeat(32)}`, supervisor: { pid: dead.pid, host: hostname() },
      leader: { ...identity, startTicks: String(BigInt(identity.startTicks) - 1n) }, recordedAt: new Date().toISOString() }] } });

  const view = await new FilePhaseJobRegistry({ directory, execute: noExecute }).get('reused');
  assert.deepEqual([view.status, view.reason, view.orphaned], ['UNKNOWN', 'OWNER_DIED_AFTER_DISPATCH', undefined]);
  assert.ok(!gone(unrelated.pid), 'the unrelated process that reused the PID runs on');
});

test('terminateOrphans refuses without authorization, on stale or unsuitable jobs, and while the owner lives', { skip: !linux }, async t => {
  const directory = await temporaryDirectory(t);
  const { pids } = await orphanedJob(t, directory);
  const jobs = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const { sequence } = await jobs.get('deploy');
  const base = { jobId: 'deploy', expectedSequence: sequence, explicitAuthorization: true };
  await assert.rejects(jobs.terminateOrphans({ ...base, explicitAuthorization: false }), { code: 'JOB_AUTHORIZATION_REQUIRED' });
  await assert.rejects(jobs.terminateOrphans({ ...base, expectedSequence: sequence - 1 }), { code: 'JOB_RECORD_CHANGED' });
  await assert.rejects(jobs.terminateOrphans({ ...base, expectedSequence: undefined }), /expectedSequence/);
  await assert.rejects(jobs.terminateOrphans({ ...base, graceMs: -1 }), /graceMs/);
  await assert.rejects(jobs.terminateOrphans({ ...base, jobId: 'missing' }), { code: 'JOB_NOT_FOUND' });
  assert.ok(!gone(pids.leader), 'nothing was signalled');

  // A job whose owner runs: its own supervisor is in charge.
  const owner = new FilePhaseJobRegistry({ directory, execute: ({ runProcess }) =>
    runProcess({ command: process.execPath, args: ['-e', forever] }) });
  await owner.start({ jobId: 'live', objective: 'Live' });
  await until(async () => (await owner.get('live')).processes[0]?.leader != null, 'the live leader');
  const live = await new FilePhaseJobRegistry({ directory, execute: noExecute }).get('live');
  await assert.rejects(new FilePhaseJobRegistry({ directory, execute: noExecute }).terminateOrphans({ jobId: 'live',
    expectedSequence: live.sequence, explicitAuthorization: true }), { code: 'JOB_NOT_OWNED' });
  await owner.requestCancellation('live', { timeoutMs: 10_000 });

  // A job without recorded processes, and a job that is no longer RUNNING.
  const plain = new FilePhaseJobRegistry({ directory, execute: async () => ({ status: 'COMPLETED' }) });
  await plain.start({ jobId: 'plain', objective: 'No processes' });
  const done = await plain.wait('plain');
  await assert.rejects(plain.terminateOrphans({ jobId: 'plain', expectedSequence: done.sequence, explicitAuthorization: true }),
    { code: 'JOB_NO_ORPHANS' });
});

test('an operator can confirm that orphaned processes stopped, and reconcile with an audited override', { skip: !linux }, async t => {
  const directory = await temporaryDirectory(t);
  await orphanedJob(t, directory);
  const jobs = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const view = await jobs.get('deploy');
  const reconciled = await jobs.reconcile({ jobId: 'deploy', expectedSequence: view.sequence, disposition: 'CANCELLED',
    evidence, explicitAuthorization: true, overrideRunningOwner: true });
  assert.equal(reconciled.status, 'CANCELLED');
  assert.deepEqual([reconciled.reconciliation.override.ownerState, reconciled.reconciliation.override.processesState], ['DEAD', 'RUNNING']);
});

test('on other platforms orphans cannot be verified; terminateOrphans says so', { skip: linux }, async t => {
  const directory = await temporaryDirectory(t);
  const jobs = new FilePhaseJobRegistry({ directory, execute: noExecute });
  await assert.rejects(jobs.terminateOrphans({ jobId: 'any', expectedSequence: 1, explicitAuthorization: true }),
    { code: 'JOB_ORPHANS_UNSUPPORTED' });
});
