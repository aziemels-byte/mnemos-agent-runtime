import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFile, readdir, writeFile } from 'node:fs/promises';
import { hostname } from 'node:os';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { FilePhaseJobRegistry, GoalRecoveryCoordinator, FileCheckpointStore } from '../src/index.mjs';
import { FileJobStore } from '../src/job-store.mjs';
import { identityState, processIdentity } from '../src/process-identity.mjs';
import { deferred, temporaryDirectory, until, withTimeout, workerThread, zombieProcess } from './helpers.mjs';

const recordText = (directory, jobId) =>
  readFile(path.join(directory, `${createHash('sha256').update(jobId).digest('hex')}.job.json`), 'utf8');
const untilAborted = signal => new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }));


test('a completed job survives a restart and its ID never runs again', async t => {
  const directory = await temporaryDirectory(t);
  let calls = 0;
  const execute = async ({ input, setPhase }) => { calls++; setPhase('report'); return { status: 'COMPLETED', value: input.n * 2 }; };
  const job = { jobId: 'job_1', objective: 'Double it', input: { n: 21 } };
  const first = new FilePhaseJobRegistry({ directory, execute });
  const started = await first.start(job);
  assert.equal(started.status, 'RUNNING');
  const done = await first.wait('job_1');
  assert.deepEqual([done.status, done.phase, done.result.value, done.dispatched, done.effectAbsent, done.reason],
    ['COMPLETED', 'report', 42, true, false, null]);

  const restarted = new FilePhaseJobRegistry({ directory, execute }); // a new process
  assert.deepEqual(await restarted.get('job_1'), done);
  assert.deepEqual(await restarted.start(job), done);
  assert.deepEqual(await restarted.wait('job_1'), done);
  assert.equal(calls, 1);
  await assert.rejects(restarted.start({ ...job, input: { n: 22 } }), error =>
    error.code === 'JOB_IDENTITY_CONFLICT' && /identity/.test(error.message));
  await assert.rejects(restarted.start({ ...job, objective: 'Other' }), { code: 'JOB_IDENTITY_CONFLICT' });
  assert.equal(await restarted.get('never_started'), null);
});

test('the job is recorded before start resolves and dispatched before the executor runs', async t => {
  const directory = await temporaryDirectory(t);
  const observer = new FileJobStore({ directory });
  const seenByExecutor = deferred();
  const registry = new FilePhaseJobRegistry({ directory, execute: async () => {
    seenByExecutor.resolve(await observer.read('job_1'));
    return { status: 'COMPLETED' };
  } });
  const started = await registry.start({ jobId: 'job_1', objective: 'Write ahead' });
  const stored = await observer.read('job_1');
  assert.deepEqual([stored.status, stored.phase, stored.dispatched], ['RUNNING', 'queued', false]);
  assert.equal(started.sequence, stored.sequence);
  const atDispatch = await seenByExecutor.promise;
  assert.deepEqual([atDispatch.status, atDispatch.dispatched], ['RUNNING', true]);
  await registry.wait('job_1');
});

test('a job cancelled before dispatch never runs and is recorded as effect-free', async t => {
  const directory = await temporaryDirectory(t);
  let calls = 0;
  const registry = new FilePhaseJobRegistry({ directory, execute: async () => { calls++; return { status: 'COMPLETED' }; } });
  await registry.start({ jobId: 'job_1', objective: 'Cancel early' });
  const cancelled = await registry.requestCancellation('job_1');
  assert.equal(calls, 0);
  const stored = await new FilePhaseJobRegistry({ directory, execute: async () => {} }).get('job_1');
  assert.deepEqual(stored, cancelled);
  assert.deepEqual([stored.status, stored.cancellationRequested, stored.cancellationConfirmed, stored.effectAbsent, stored.reason],
    ['CANCELLED', true, true, true, 'CANCELLED_BEFORE_DISPATCH']);
});

test('a confirmed cancellation after dispatch is recorded with its request and phase', async t => {
  const directory = await temporaryDirectory(t);
  const entered = deferred();
  const registry = new FilePhaseJobRegistry({ directory, execute: async ({ signal, setPhase }) => {
    setPhase('full_regression');
    entered.resolve();
    await untilAborted(signal);
    return { status: 'CANCELLED', termination: { confirmed: true } };
  } });
  await registry.start({ jobId: 'job_1', objective: 'Cancel a running job' });
  await entered.promise;
  const cancelled = await registry.requestCancellation('job_1');
  assert.deepEqual([cancelled.status, cancelled.phase, cancelled.cancellationRequested, cancelled.cancellationConfirmed,
    cancelled.effectAbsent, cancelled.reason], ['CANCELLED', 'full_regression', true, true, false, 'CANCELLED_BY_REQUEST']);
  const stored = JSON.parse(await recordText(directory, 'job_1')).record;
  assert.ok(stored.cancellationRequestedAt <= stored.updatedAt);
  assert.deepEqual(await new FilePhaseJobRegistry({ directory, execute: async () => {} }).get('job_1'), cancelled);
});

test('an unconfirmed stop is recorded as UNKNOWN and is never started again', async t => {
  const directory = await temporaryDirectory(t);
  let calls = 0;
  const entered = deferred();
  const execute = async ({ signal }) => { calls++; entered.resolve(); await untilAborted(signal); return { status: 'CANCELLED' }; };
  const registry = new FilePhaseJobRegistry({ directory, execute });
  await registry.start({ jobId: 'job_1', objective: 'Uncertain stop' });
  await entered.promise;
  const result = await registry.requestCancellation('job_1');
  assert.deepEqual([result.status, result.reconciliationRequired, result.reason], ['UNKNOWN', true, 'CANCELLATION_UNCONFIRMED']);
  const restarted = new FilePhaseJobRegistry({ directory, execute });
  assert.equal((await restarted.start({ jobId: 'job_1', objective: 'Uncertain stop' })).status, 'UNKNOWN');
  assert.equal(calls, 1);
});

test('a non-cooperative job stays RUNNING, and its cancellation request is visible to other processes', async t => {
  const directory = await temporaryDirectory(t);
  const entered = deferred();
  const release = deferred();
  const registry = new FilePhaseJobRegistry({ directory, execute: async () => {
    entered.resolve(); await release.promise; return { status: 'COMPLETED' };
  } });
  await registry.start({ jobId: 'job_1', objective: 'Ignore abort' });
  await entered.promise;
  const pending = await registry.requestCancellation('job_1', { timeoutMs: 5 });
  assert.deepEqual([pending.status, pending.cancellationRequested, pending.cancellationConfirmed], ['RUNNING', true, false]);
  const other = new FilePhaseJobRegistry({ directory, execute: async () => {} });
  await until(async () => (await other.get('job_1')).cancellationRequested, 'the recorded request');
  assert.equal((await other.get('job_1')).status, 'RUNNING');
  release.resolve();
  assert.equal((await registry.wait('job_1')).status, 'COMPLETED'); // it completed despite the request
});

test('retry safety (effectAbsent) comes only from an explicit executor report', async t => {
  const directory = await temporaryDirectory(t);
  const reports = {
    failed_clean: async () => ({ status: 'FAILED', effectAbsent: true }),
    failed_plain: async () => ({ status: 'FAILED' }),
    completed_claim: async () => ({ status: 'COMPLETED', effectAbsent: true }),
    threw: async () => { throw new Error('sensitive diagnostic 9f3a'); },
    odd_status: async () => ({ status: 'CANCELLED' }), // cancelled without a request
    unknown_report: async () => ({ status: 'UNKNOWN', effectAbsent: true }), // cannot tell; never effect-free
    hostile: async () => ({ get status() { throw new Error('hostile'); } })
  };
  const registry = new FilePhaseJobRegistry({ directory, execute: ({ jobId }) => reports[jobId]() });
  const outcomes = {};
  for (const jobId of Object.keys(reports)) {
    await registry.start({ jobId, objective: 'Report an outcome' });
    const { status, effectAbsent, reason } = await registry.wait(jobId);
    outcomes[jobId] = [status, effectAbsent, reason];
  }
  assert.deepEqual(outcomes, {
    failed_clean: ['FAILED', true, 'EXECUTOR_REPORTED_FAILURE'],
    failed_plain: ['FAILED', false, 'EXECUTOR_REPORTED_FAILURE'],
    completed_claim: ['COMPLETED', false, null],
    threw: ['FAILED', false, 'EXECUTOR_THREW'],
    odd_status: ['FAILED', false, 'EXECUTOR_STATUS_UNEXPECTED'],
    unknown_report: ['UNKNOWN', false, 'EXECUTOR_REPORTED_UNKNOWN'],
    hostile: ['FAILED', false, 'EXECUTOR_THREW']
  });
  assert.doesNotMatch(await recordText(directory, 'threw'), /sensitive|9f3a/);
});

test('phase changes are recorded in the background, coalesced, ending with the last phase', async t => {
  const directory = await temporaryDirectory(t);
  const observer = new FileJobStore({ directory });
  const proceed = deferred();
  const phases = Array.from({ length: 50 }, (_, index) => `step_${index}`);
  const registry = new FilePhaseJobRegistry({ directory, execute: async ({ setPhase }) => {
    for (const phase of phases) assert.equal(setPhase(phase), true); // never blocks the executor
    await proceed.promise;
    return { status: 'COMPLETED' };
  } });
  await registry.start({ jobId: 'job_1', objective: 'Many phases' });
  assert.equal((await registry.get('job_1')).phase, 'queued');
  const running = await until(async () => {
    const record = await observer.read('job_1');
    return record.phase === 'step_49' && record;
  }, 'the last phase on disk');
  assert.equal(running.status, 'RUNNING');
  assert.ok(running.sequence < 2 + phases.length, `coalesced: ${running.sequence - 2} phase writes for ${phases.length} changes`);
  proceed.resolve();
  const done = await registry.wait('job_1');
  assert.equal(done.phase, 'step_49');
});

test('setPhase after the outcome is decided is refused', async t => {
  const directory = await temporaryDirectory(t);
  let late;
  const registry = new FilePhaseJobRegistry({ directory, execute: async ({ setPhase }) => {
    late = setPhase;
    return { status: 'COMPLETED' };
  } });
  await registry.start({ jobId: 'job_1', objective: 'Late phase' });
  await registry.wait('job_1');
  assert.equal(late('too_late'), false);
  assert.throws(() => late('bad phase'), /Invalid job phase/);
  assert.equal((await registry.get('job_1')).phase, 'queued');
});

test('results are kept only as plain JSON of at most 64 KiB, never changing the status', async t => {
  const directory = await temporaryDirectory(t);
  const results = {
    small: { status: 'COMPLETED', rows: [1, 2] },
    large: { status: 'COMPLETED', blob: 'x'.repeat(70 * 1024) },
    handle: { status: 'COMPLETED', close: () => {} }
  };
  const registry = new FilePhaseJobRegistry({ directory, execute: async ({ jobId }) => results[jobId] });
  for (const jobId of Object.keys(results)) await registry.start({ jobId, objective: 'Return data' });
  const view = {};
  for (const jobId of Object.keys(results)) {
    const { status, result, resultDiscarded } = await registry.wait(jobId);
    view[jobId] = [status, result, resultDiscarded];
  }
  assert.deepEqual(view, {
    small: ['COMPLETED', { rows: [1, 2], status: 'COMPLETED' }, false],
    large: ['COMPLETED', null, true],
    handle: ['COMPLETED', null, true]
  });
});

test('an outcome that cannot be recorded is UNKNOWN until a background retry records it', async t => {
  // The clock fails for the dispatch write and the first outcome write, then recovers.
  const directory = await temporaryDirectory(t);
  let calls = 0;
  let healed = false;
  const clock = () => (++calls >= 2 && !healed ? new Date(NaN) : new Date());
  let executed = 0;
  const registry = new FilePhaseJobRegistry({ directory, clock, execute: async () => { executed++; return { status: 'COMPLETED' }; } });
  await registry.start({ jobId: 'job_1', objective: 'Storage trouble' });
  const uncertain = await registry.wait('job_1');
  assert.equal(executed, 0, 'an executor is never called without a recorded dispatch');
  assert.deepEqual([uncertain.status, uncertain.reconciliationRequired, uncertain.reason], ['UNKNOWN', true, 'OUTCOME_NOT_RECORDED']);
  const before = await new FileJobStore({ directory }).read('job_1');
  assert.deepEqual([before.status, before.dispatched], ['RUNNING', false]);

  healed = true;
  const recorded = await until(async () => {
    const view = await registry.get('job_1');
    return view.status !== 'UNKNOWN' && view;
  }, 'the retried outcome');
  assert.deepEqual([recorded.status, recorded.effectAbsent, recorded.reason, recorded.dispatched],
    ['FAILED', true, 'DISPATCH_NOT_RECORDED', false]);
  assert.deepEqual(await new FilePhaseJobRegistry({ directory, execute: async () => {} }).get('job_1'), recorded);
  assert.equal(executed, 0);
});

test('a cancellation is delivered even when recording it fails', async t => {
  const directory = await temporaryDirectory(t);
  let broken = false;
  const clock = () => (broken ? new Date(NaN) : new Date());
  const entered = deferred();
  const registry = new FilePhaseJobRegistry({ directory, clock, execute: async ({ signal }) => {
    entered.resolve();
    await untilAborted(signal);
    return { status: 'CANCELLED', termination: { confirmed: true } };
  } });
  await registry.start({ jobId: 'job_1', objective: 'Cancel with a broken clock' });
  await entered.promise;
  broken = true;
  const cancelled = await registry.requestCancellation('job_1');
  assert.deepEqual([cancelled.status, cancelled.reason, cancelled.cancellationRequested], ['UNKNOWN', 'OUTCOME_NOT_RECORDED', true]);
  broken = false;
  const recorded = await until(async () => {
    const view = await registry.get('job_1');
    return view.status !== 'UNKNOWN' && view;
  }, 'the retried outcome');
  assert.deepEqual([recorded.status, recorded.cancellationConfirmed], ['CANCELLED', true]);
});

test('capacity counts jobs executing here; existing jobs can still be looked up through start', async t => {
  const directory = await temporaryDirectory(t);
  const release = deferred();
  const registry = new FilePhaseJobRegistry({ directory, maxRunning: 1, execute: async ({ jobId }) => {
    if (jobId === 'slow') await release.promise;
    return { status: 'COMPLETED' };
  } });
  await registry.start({ jobId: 'done', objective: 'Quick' });
  await registry.wait('done');
  await registry.start({ jobId: 'slow', objective: 'Slow' });
  await assert.rejects(registry.start({ jobId: 'extra', objective: 'Extra' }), { code: 'JOB_CAPACITY_EXHAUSTED' });
  assert.equal((await registry.start({ jobId: 'done', objective: 'Quick' })).status, 'COMPLETED');
  assert.equal(await registry.get('extra'), null, 'a refused job is not recorded');
  release.resolve();
  await registry.wait('slow');
  await registry.start({ jobId: 'extra', objective: 'Extra' });
  assert.equal((await registry.wait('extra')).status, 'COMPLETED');
});

test('list shows live and recorded jobs', async t => {
  const directory = await temporaryDirectory(t);
  const release = deferred();
  const registry = new FilePhaseJobRegistry({ directory, execute: async ({ jobId, setPhase }) => {
    if (jobId === 'live') { setPhase('working'); await release.promise; }
    return { status: 'COMPLETED' };
  } });
  await registry.start({ jobId: 'done', objective: 'Quick' });
  await registry.wait('done');
  await registry.start({ jobId: 'live', objective: 'Slow' });
  await until(async () => (await registry.get('live')).phase === 'working', 'the phase');
  const { jobs, invalid } = await registry.list();
  assert.deepEqual(jobs.map(job => [job.jobId, job.status]), [['done', 'COMPLETED'], ['live', 'RUNNING']]);
  assert.equal(jobs[1].phase, 'working');
  assert.deepEqual(invalid, []);
  release.resolve();
  await registry.wait('live');
});

test('a GoalRecoveryCoordinator can inspect recorded jobs after a restart', async t => {
  const directory = await temporaryDirectory(t);
  const registry = new FilePhaseJobRegistry({ directory, execute: async ({ jobId }) =>
    ({ status: jobId === 'good' ? 'COMPLETED' : 'FAILED', effectAbsent: true }) });
  for (const jobId of ['good', 'bad']) await registry.start({ jobId, objective: 'Part of a goal' });
  await Promise.all(['good', 'bad'].map(jobId => registry.wait(jobId)));
  await new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) })
    .checkpoint({ goal: { goalId: 'goal' }, activeJobs: [{ id: 'good' }, { id: 'bad' }, { id: 'never' }] });

  const restarted = new FilePhaseJobRegistry({ directory, execute: async () => {} });
  const coordinator = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory),
    jobInspector: async job => (await restarted.get(job.id)) ?? { status: 'UNKNOWN' } });
  const { jobs } = await coordinator.recover();
  assert.deepEqual(jobs.map(job => [job.id, job.recoveryStatus]), [['good', 'COMPLETED'], ['bad', 'FAILED'], ['never', 'UNKNOWN']]);
});

test('inputs and options are validated', async t => {
  const directory = await temporaryDirectory(t);
  const execute = async () => ({ status: 'COMPLETED' });
  assert.throws(() => new FilePhaseJobRegistry({ directory }), /execute/);
  assert.throws(() => new FilePhaseJobRegistry({ execute }), /directory/);
  for (const maxRunning of [0, 513, 1.5]) assert.throws(() => new FilePhaseJobRegistry({ directory, execute, maxRunning }), /maxRunning/);
  const registry = new FilePhaseJobRegistry({ directory, execute });
  for (const jobId of ['', '_x', 'x'.repeat(129)]) await assert.rejects(registry.start({ jobId, objective: 'x' }), /Invalid job ID/);
  await assert.rejects(registry.start({ jobId: 'job_1', objective: ' ' }), /objective/);
  await assert.rejects(registry.start({ jobId: 'job_1', objective: 'x', input: new Map() }), /JSON/);
  await assert.rejects(registry.start({ jobId: 'job_1', objective: 'x', input: NaN }), /JSON/);
  await assert.rejects(registry.start({ jobId: 'job_1', objective: 'x', input: 'x'.repeat(65536) }), /65536/);
  await assert.rejects(registry.requestCancellation('job_1', { timeoutMs: -1 }), /timeoutMs/);
  assert.equal(await registry.requestCancellation('job_1'), null);
  assert.deepEqual((await readdir(directory)).filter(name => name.endsWith('.job.json')), []);
  // Equivalent inputs (key order) are the same identity.
  await registry.start({ jobId: 'job_2', objective: 'x', input: { a: 1, b: 2 } });
  await registry.start({ jobId: 'job_2', objective: 'x', input: { b: 2, a: 1 } });
  await registry.wait('job_2');
});

// Cross-process behavior.
const moduleUrl = new URL('../src/index.mjs', import.meta.url).href;
const childPrelude = `
  import { FilePhaseJobRegistry } from ${JSON.stringify(moduleUrl)};
  import { appendFile } from 'node:fs/promises';
  import path from 'node:path';
  const [directory, ...args] = process.argv.slice(1);
  const emit = value => process.stdout.write(JSON.stringify(value) + '\\n');
`;

function startChild(source, args) {
  const child = spawn(process.execPath, ['--input-type=module', '-e', childPrelude + source, '--', ...args],
    { stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', chunk => { stdout += chunk; child.emit('output'); });
  child.stderr.on('data', chunk => { stderr += chunk; });
  child.exited = new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('close', code => resolve({ code, stdout, stderr }));
  });
  child.line = () => new Promise((resolve, reject) => {
    const check = () => { if (stdout.includes('\n')) resolve(JSON.parse(stdout.split('\n')[0])); };
    child.on('output', check);
    child.exited.then(({ stderr: error }) => reject(new Error(`child exited before reporting: ${error}`)));
    check();
  });
  return child;
}

test('processes starting the same job at once run it exactly once', async t => {
  const directory = await temporaryDirectory(t);
  const source = `
    const registry = new FilePhaseJobRegistry({ directory, lockTimeoutMs: 20000, execute: async () => {
      await appendFile(path.join(directory, 'runs.log'), process.pid + '\\n');
      return { status: 'COMPLETED', by: process.pid };
    } });
    const started = await registry.start({ jobId: 'shared_job', objective: 'Run once', input: { n: 1 } });
    const settled = await registry.wait('shared_job');
    emit({ started: started.status, settled: settled.status });`;
  const children = Array.from({ length: 6 }, () => startChild(source, [directory]));
  const results = await Promise.all(children.map(child => child.exited));
  for (const { code, stderr } of results) assert.equal(code, 0, stderr);
  const runs = (await readFile(path.join(directory, 'runs.log'), 'utf8')).trim().split('\n');
  assert.equal(runs.length, 1);
  const final = await new FilePhaseJobRegistry({ directory, execute: async () => {} }).get('shared_job');
  assert.deepEqual([final.status, final.result.by], ['COMPLETED', Number(runs[0])]);
});

test('a job whose process was killed after dispatch is recorded UNKNOWN on restart and never replayed', async t => {
  const directory = await temporaryDirectory(t);
  const source = `
    const registry = new FilePhaseJobRegistry({ directory, execute: ({ setPhase }) => {
      setPhase('execution');
      emit({ pid: process.pid });
      return new Promise(() => setInterval(() => {}, 1000)); // runs until killed
    } });
    await registry.start({ jobId: 'killed_job', objective: 'Long task' });`;
  const child = startChild(source, [directory]);
  t.after(() => child.kill('SIGKILL'));
  const { pid } = await child.line();
  const observer = new FilePhaseJobRegistry({ directory, execute: async () => { throw new Error('must not replay'); } });
  await until(async () => (await observer.get('killed_job')).phase === 'execution', 'the recorded phase');
  child.kill('SIGKILL');
  await child.exited;

  let calls = 0;
  const restarted = new FilePhaseJobRegistry({ directory, execute: async () => { calls++; return { status: 'COMPLETED' }; } });
  const view = await restarted.start({ jobId: 'killed_job', objective: 'Long task' });
  assert.deepEqual([view.status, view.reason, view.reconciliationRequired, view.phase, view.dispatched, view.owner.pid],
    ['UNKNOWN', 'OWNER_DIED_AFTER_DISPATCH', true, 'execution', true, pid]);
  const stored = await new FileJobStore({ directory }).read('killed_job');
  assert.deepEqual([stored.status, stored.reason], ['UNKNOWN', 'OWNER_DIED_AFTER_DISPATCH'], 'the reclassification is recorded');
  assert.deepEqual(await restarted.requestCancellation('killed_job'), view);
  assert.equal(calls, 0);
});

test('an outcome blocked by a live lock holder is UNKNOWN until the lock is free', async t => {
  const directory = await temporaryDirectory(t);
  const entered = deferred();
  const release = deferred();
  const registry = new FilePhaseJobRegistry({ directory, lockTimeoutMs: 100, execute: async () => {
    entered.resolve(); await release.promise; return { status: 'COMPLETED', value: 1 };
  } });
  await registry.start({ jobId: 'job_1', objective: 'Blocked outcome' });
  await entered.promise;

  const lockUrl = new URL('../src/directory-lock.mjs', import.meta.url).href;
  const holder = startChild(`
    const { DirectoryLock } = await import(${JSON.stringify(lockUrl)});
    setInterval(() => {}, 1000);
    new DirectoryLock({ directory, name: 'jobs.lock' }).run(() => { emit({ locked: true }); return new Promise(() => {}); });`,
  [directory]);
  t.after(() => holder.kill('SIGKILL'));
  await holder.line();
  release.resolve();
  const blocked = await registry.wait('job_1');
  assert.deepEqual([blocked.status, blocked.reason, blocked.result], ['UNKNOWN', 'OUTCOME_NOT_RECORDED', null]);
  assert.equal((await new FileJobStore({ directory }).read('job_1')).status, 'RUNNING');

  holder.kill('SIGKILL');
  await holder.exited;
  const recorded = await until(async () => {
    const view = await registry.get('job_1');
    return view.status === 'COMPLETED' && view;
  }, 'the retried outcome');
  assert.equal(recorded.result.value, 1);
  assert.equal((await new FileJobStore({ directory }).read('job_1')).status, 'COMPLETED');
});

// Restart classification: what a restarted process reports for jobs it does not own.
async function deadPid() {
  const exited = spawn(process.execPath, ['-e', '']);
  await new Promise(resolve => exited.on('close', resolve));
  return exited.pid;
}

// A RUNNING record left by `owner`, optionally dispatched, written as the owner would have.
async function leftRunning(directory, jobId, owner, { dispatched = true } = {}) {
  const store = new FileJobStore({ directory });
  const { record } = await store.create({ jobId, objective: 'Left running', owner,
    fingerprint: createHash('sha256').update('null').digest('hex') });
  if (!dispatched) return record;
  return store.update(jobId, { expectedSequence: record.sequence, changes: { dispatched: true, dispatchedAt: new Date().toISOString() } });
}
const noExecute = async () => { throw new Error('must not run'); };

test('a dead owner\'s job is UNKNOWN if dispatched and CANCELLED if not; both are recorded and never run', async t => {
  const directory = await temporaryDirectory(t);
  const pid = await deadPid();
  const owner = executionId => ({ pid, host: hostname(), executionId });
  const dispatched = await leftRunning(directory, 'after_dispatch', owner('a'));
  await leftRunning(directory, 'before_dispatch', owner('b'), { dispatched: false });
  let calls = 0;
  const registry = new FilePhaseJobRegistry({ directory, execute: async () => { calls++; return { status: 'COMPLETED' }; } });

  const after = await registry.get('after_dispatch');
  assert.deepEqual([after.status, after.reason, after.reconciliationRequired, after.effectAbsent, after.sequence],
    ['UNKNOWN', 'OWNER_DIED_AFTER_DISPATCH', true, false, dispatched.sequence + 1]);
  const before = await registry.start({ jobId: 'before_dispatch', objective: 'Left running' });
  assert.deepEqual([before.status, before.reason, before.cancellationConfirmed, before.effectAbsent],
    ['CANCELLED', 'OWNER_DIED_BEFORE_DISPATCH', true, true]);
  const store = new FileJobStore({ directory });
  assert.equal((await store.read('after_dispatch')).status, 'UNKNOWN');
  assert.equal((await store.read('before_dispatch')).status, 'CANCELLED');
  assert.deepEqual(await registry.get('after_dispatch'), after, 'reading again changes nothing');
  assert.equal(calls, 0);
});

test('a record with this process\'s PID but an execution it does not run was left by an earlier process', async t => {
  const directory = await temporaryDirectory(t);
  await leftRunning(directory, 'reused_pid', { pid: process.pid, host: hostname(), executionId: 'earlier-process' });
  const view = await new FilePhaseJobRegistry({ directory, execute: noExecute }).get('reused_pid');
  assert.deepEqual([view.status, view.reason], ['UNKNOWN', 'OWNER_DIED_AFTER_DISPATCH']);
});

test('a job running in another instance of this process is ALIVE, not reclassified', async t => {
  const directory = await temporaryDirectory(t);
  const entered = deferred();
  const release = deferred();
  const owner = new FilePhaseJobRegistry({ directory, execute: async () => {
    entered.resolve(); await release.promise; return { status: 'COMPLETED' };
  } });
  await owner.start({ jobId: 'job_1', objective: 'Shared process' });
  await entered.promise;
  const other = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const view = await other.get('job_1');
  assert.deepEqual([view.status, view.ownerState], ['RUNNING', 'ALIVE']);
  assert.deepEqual((await other.recover()).reclassified, []);
  await assert.rejects(other.requestCancellation('job_1'), error => error.code === 'JOB_NOT_OWNED' && error.ownerState === 'ALIVE');
  const waiting = other.wait('job_1', { timeoutMs: 10_000 }); // a regression fails instead of hanging
  release.resolve();
  assert.equal((await waiting).status, 'COMPLETED', 'wait follows a job owned by another instance');
});

test('a job owned by another host cannot be verified and stays RUNNING for an operator', async t => {
  const directory = await temporaryDirectory(t);
  await leftRunning(directory, 'remote_job', { pid: 4242, host: 'other-host.invalid', executionId: 'remote' });
  const registry = new FilePhaseJobRegistry({ directory, execute: noExecute, pollIntervalMs: 5 });
  const view = await registry.get('remote_job');
  assert.deepEqual([view.status, view.ownerState], ['RUNNING', 'OTHER_HOST']);
  const started = performance.now();
  assert.equal((await registry.wait('remote_job', { timeoutMs: 60 })).status, 'RUNNING');
  assert.ok(performance.now() - started >= 55, 'wait honours its timeout');
  assert.deepEqual((await registry.recover()).unverified, ['remote_job']);
  await assert.rejects(registry.requestCancellation('remote_job'), { code: 'JOB_NOT_OWNED' });
  assert.equal((await new FileJobStore({ directory }).read('remote_job')).status, 'RUNNING');
});

test('wait follows a job in another process until it completes, or until its owner dies', async t => {
  const directory = await temporaryDirectory(t);
  const source = `
    const registry = new FilePhaseJobRegistry({ directory, execute: async ({ jobId }) => {
      emit({ running: jobId });
      if (jobId === 'completes') { await new Promise(resolve => setTimeout(resolve, 200)); return { status: 'COMPLETED', by: process.pid }; }
      return new Promise(() => setInterval(() => {}, 1000)); // killed while running
    } });
    await registry.start({ jobId: args[0], objective: 'Remote work' });`;
  const registry = new FilePhaseJobRegistry({ directory, execute: noExecute, pollIntervalMs: 10 });

  const completes = startChild(source, [directory, 'completes']);
  t.after(() => completes.kill('SIGKILL'));
  await completes.line();
  const completed = await registry.wait('completes', { timeoutMs: 10_000 });
  assert.deepEqual([completed.status, completed.result.by], ['COMPLETED', completes.pid]);

  const dies = startChild(source, [directory, 'dies']);
  t.after(() => dies.kill('SIGKILL'));
  await dies.line();
  const waiting = registry.wait('dies', { timeoutMs: 10_000 });
  await sleep(50);
  dies.kill('SIGKILL');
  const died = await waiting;
  assert.deepEqual([died.status, died.reason], ['UNKNOWN', 'OWNER_DIED_AFTER_DISPATCH']);
});

test('processes reclassifying the same dead owner\'s job at once record it exactly once', async t => {
  const directory = await temporaryDirectory(t);
  const left = await leftRunning(directory, 'orphan', { pid: await deadPid(), host: hostname(), executionId: 'gone' });
  const source = `
    const registry = new FilePhaseJobRegistry({ directory, lockTimeoutMs: 20000, execute: async () => { throw new Error('must not run'); } });
    const view = await registry.get('orphan');
    emit({ status: view.status, sequence: view.sequence });`;
  const children = Array.from({ length: 6 }, () => startChild(source, [directory]));
  const views = await Promise.all(children.map(child => child.line()));
  await Promise.all(children.map(child => child.exited));
  assert.ok(views.every(view => view.status === 'UNKNOWN' && view.sequence === left.sequence + 1), JSON.stringify(views));
  assert.equal((await new FileJobStore({ directory }).read('orphan')).sequence, left.sequence + 1);
});

test('a reclassification that cannot be recorded yet is reported UNKNOWN and recorded later', async t => {
  const directory = await temporaryDirectory(t);
  await leftRunning(directory, 'orphan', { pid: await deadPid(), host: hostname(), executionId: 'gone' }, { dispatched: false });
  const lockUrl = new URL('../src/directory-lock.mjs', import.meta.url).href;
  const holder = startChild(`
    const { DirectoryLock } = await import(${JSON.stringify(lockUrl)});
    setInterval(() => {}, 1000);
    new DirectoryLock({ directory, name: 'jobs.lock' }).run(() => { emit({ locked: true }); return new Promise(() => {}); });`,
  [directory]);
  t.after(() => holder.kill('SIGKILL'));
  await holder.line();
  const registry = new FilePhaseJobRegistry({ directory, execute: noExecute, lockTimeoutMs: 50 });
  const pending = await registry.get('orphan');
  assert.deepEqual([pending.status, pending.reason, pending.ownerState], ['UNKNOWN', 'OWNER_DIED_NOT_RECORDED', 'DEAD']);
  assert.deepEqual((await registry.recover()).unrecorded, ['orphan']);
  assert.equal((await new FileJobStore({ directory }).read('orphan')).status, 'RUNNING');

  holder.kill('SIGKILL');
  await holder.exited;
  const recorded = await registry.get('orphan');
  assert.deepEqual([recorded.status, recorded.reason], ['CANCELLED', 'OWNER_DIED_BEFORE_DISPATCH']);
});

test('recover resolves every job after a restart and reports what needs an operator', async t => {
  const directory = await temporaryDirectory(t);
  const first = new FilePhaseJobRegistry({ directory, execute: async () => ({ status: 'COMPLETED' }) });
  await first.start({ jobId: 'finished', objective: 'Done before the restart' });
  await first.wait('finished');
  const pid = await deadPid();
  await leftRunning(directory, 'dispatched', { pid, host: hostname(), executionId: 'a' });
  await leftRunning(directory, 'queued', { pid, host: hostname(), executionId: 'b' }, { dispatched: false });
  await leftRunning(directory, 'remote', { pid, host: 'other-host.invalid', executionId: 'c' });
  await writeFile(path.join(directory, `${createHash('sha256').update('torn').digest('hex')}.job.json`), '{"torn');

  const summary = await new FilePhaseJobRegistry({ directory, execute: noExecute }).recover();
  const statuses = Object.fromEntries(summary.jobs.map(job => [job.jobId, job.status]));
  assert.deepEqual(statuses, { finished: 'COMPLETED', dispatched: 'UNKNOWN', queued: 'CANCELLED', remote: 'RUNNING' });
  assert.deepEqual(summary.reclassified.sort(), ['dispatched', 'queued']);
  assert.deepEqual([summary.unverified, summary.unrecorded], [['remote'], []]);
  assert.equal(summary.invalid.length, 1);
  // Recovering again changes nothing.
  const again = await new FilePhaseJobRegistry({ directory, execute: noExecute }).recover();
  assert.deepEqual(again.jobs, summary.jobs);
});

test('wait and poll options are validated', async t => {
  const directory = await temporaryDirectory(t);
  for (const pollIntervalMs of [0, 1.5, 60_001]) {
    assert.throws(() => new FilePhaseJobRegistry({ directory, execute: noExecute, pollIntervalMs }), /pollIntervalMs/);
  }
  const registry = new FilePhaseJobRegistry({ directory, execute: noExecute });
  for (const timeoutMs of [-1, 1.5, '5']) await assert.rejects(registry.wait('job_1', { timeoutMs }), /timeoutMs/);
  assert.equal(await registry.wait('job_1', { timeoutMs: 10 }), null);
});

// Reconciliation: resolving UNKNOWN and unverifiable jobs with authoritative evidence.
const evidence = { authoritative: true, source: 'billing-api:invoice/42' };

async function unknownJob(directory, jobId) {
  await leftRunning(directory, jobId, { pid: await deadPid(), host: hostname(), executionId: 'gone' });
  return new FilePhaseJobRegistry({ directory, execute: noExecute }).get(jobId); // recorded UNKNOWN
}

test('reconciling an UNKNOWN job records its outcome and audit; it survives a restart and is final', async t => {
  const directory = await temporaryDirectory(t);
  const unknown = await unknownJob(directory, 'job_1');
  assert.deepEqual([unknown.status, unknown.reconciliation], ['UNKNOWN', null]);
  const registry = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const reconciled = await registry.reconcile({ jobId: 'job_1', expectedSequence: unknown.sequence, disposition: 'COMPLETED',
    evidence: { ...evidence, effectAbsent: true, note: 'invoice exists' }, result: { invoice: 42 }, explicitAuthorization: true });
  assert.deepEqual([reconciled.status, reconciled.reason, reconciled.reconciliationRequired, reconciled.effectAbsent, reconciled.result],
    ['COMPLETED', 'RECONCILED', false, false, { invoice: 42 }]);
  const audit = reconciled.reconciliation;
  assert.deepEqual([audit.disposition, audit.override, audit.previous], ['COMPLETED', null, { status: 'UNKNOWN', reason: 'OWNER_DIED_AFTER_DISPATCH' }]);
  assert.deepEqual(audit.evidence, { authoritative: true, effectAbsent: true, note: 'invoice exists', source: 'billing-api:invoice/42' });
  assert.ok(Number.isFinite(Date.parse(audit.at)));

  const restarted = new FilePhaseJobRegistry({ directory, execute: noExecute });
  assert.deepEqual(await restarted.get('job_1'), reconciled);
  assert.deepEqual(await restarted.start({ jobId: 'job_1', objective: 'Left running' }), reconciled, 'never run');
  await assert.rejects(restarted.reconcile({ jobId: 'job_1', expectedSequence: reconciled.sequence, disposition: 'FAILED',
    evidence, explicitAuthorization: true }), { code: 'JOB_RECONCILIATION_NOT_REQUIRED' });
});

test('refused reconciliations write nothing', async t => {
  const directory = await temporaryDirectory(t);
  const unknown = await unknownJob(directory, 'job_1');
  await leftRunning(directory, 'queued', { pid: await deadPid(), host: hostname(), executionId: 'q' }, { dispatched: false });
  const queued = await new FileJobStore({ directory }).read('queued');
  const registry = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const before = await readdir(directory).then(names => Promise.all(names.sort().map(name => readFile(path.join(directory, name), 'utf8').catch(() => ''))));
  const base = { jobId: 'job_1', expectedSequence: unknown.sequence, disposition: 'FAILED', evidence, explicitAuthorization: true };
  const refusals = [
    [{ explicitAuthorization: false }, { code: 'JOB_AUTHORIZATION_REQUIRED' }],
    [{ evidence: undefined }, { code: 'JOB_EVIDENCE_REQUIRED' }],
    [{ evidence: { ...evidence, authoritative: 'yes' } }, { code: 'JOB_EVIDENCE_REQUIRED' }],
    [{ evidence: { authoritative: true } }, /evidence.source/],
    [{ evidence: { ...evidence, effectAbsent: 'yes' } }, /effectAbsent/],
    [{ evidence: { ...evidence, when: new Date() } }, /plain JSON/],
    [{ evidence: { ...evidence, note: 'x'.repeat(8 * 1024) } }, /8 KiB/],
    [{ disposition: 'DONE' }, /disposition/],
    [{ disposition: 'UNKNOWN' }, /disposition/],
    [{ result: { close: () => {} } }, /plain JSON/],
    [{ result: 'x'.repeat(70 * 1024) }, /64 KiB/],
    [{ expectedSequence: undefined }, /expectedSequence/],
    [{ expectedSequence: unknown.sequence - 1 }, { code: 'JOB_RECORD_CHANGED' }],
    [{ overrideRunningOwner: 'yes' }, /overrideRunningOwner/],
    [{ jobId: 'missing' }, { code: 'JOB_NOT_FOUND' }],
    [{ jobId: 'bad id' }, /Invalid job ID/],
    // The executor of a job never dispatched was never called: it cannot have completed.
    [{ jobId: 'queued', expectedSequence: queued.sequence, disposition: 'COMPLETED' }, { code: 'JOB_TRANSITION_INVALID' }]
  ];
  for (const [change, expected] of refusals) {
    await assert.rejects(registry.reconcile({ ...base, ...change }), expected, JSON.stringify(Object.keys(change)));
  }
  const after = await readdir(directory).then(names => Promise.all(names.sort().map(name => readFile(path.join(directory, name), 'utf8').catch(() => ''))));
  assert.deepEqual(after, before);
});

test('effectAbsent evidence marks a failed job safe to retry under a new job ID', async t => {
  const directory = await temporaryDirectory(t);
  const unknown = await unknownJob(directory, 'charge_attempt_1');
  let runs = 0;
  const registry = new FilePhaseJobRegistry({ directory, execute: async () => { runs++; return { status: 'COMPLETED' }; } });
  const failed = await registry.reconcile({ jobId: 'charge_attempt_1', expectedSequence: unknown.sequence, disposition: 'FAILED',
    evidence: { ...evidence, effectAbsent: true }, explicitAuthorization: true });
  assert.deepEqual([failed.status, failed.effectAbsent], ['FAILED', true]);
  // The caller's resume policy decides on a retry; it is a new identity, never the same ID again.
  await registry.start({ jobId: 'charge_attempt_2', objective: 'Charge again' });
  assert.equal((await registry.wait('charge_attempt_2')).status, 'COMPLETED');
  assert.equal(runs, 1);

  const withoutProof = await unknownJob(directory, 'other_attempt_1');
  const plain = await registry.reconcile({ jobId: 'other_attempt_1', expectedSequence: withoutProof.sequence,
    disposition: 'CANCELLED', evidence, explicitAuthorization: true });
  assert.deepEqual([plain.status, plain.cancellationConfirmed, plain.effectAbsent], ['CANCELLED', true, false]);
});

test('a RUNNING job whose owner is provably gone can be reconciled directly', async t => {
  const directory = await temporaryDirectory(t);
  const left = await leftRunning(directory, 'job_1', { pid: await deadPid(), host: hostname(), executionId: 'gone' });
  const reconciled = await new FilePhaseJobRegistry({ directory, execute: noExecute }).reconcile({ jobId: 'job_1',
    expectedSequence: left.sequence, disposition: 'COMPLETED', evidence, explicitAuthorization: true });
  assert.equal(reconciled.status, 'COMPLETED');
  assert.deepEqual([reconciled.reconciliation.previous, reconciled.reconciliation.override],
    [{ status: 'RUNNING', reason: null, ownerState: 'DEAD' }, null]);
});

test('an unverifiable owner requires an explicit override, which is audited', async t => {
  const directory = await temporaryDirectory(t);
  const owner = { pid: 4242, host: 'other-host.invalid', executionId: 'remote' };
  const left = await leftRunning(directory, 'remote_job', owner);
  const registry = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const request = { jobId: 'remote_job', expectedSequence: left.sequence, disposition: 'CANCELLED', evidence, explicitAuthorization: true };
  await assert.rejects(registry.reconcile(request), error =>
    error.code === 'JOB_OWNER_UNVERIFIED' && error.ownerState === 'OTHER_HOST' && error.owner.host === owner.host);
  const reconciled = await registry.reconcile({ ...request, overrideRunningOwner: true });
  assert.deepEqual([reconciled.status, reconciled.cancellationConfirmed], ['CANCELLED', true]);
  assert.deepEqual(reconciled.reconciliation.override, { owner, ownerState: 'OTHER_HOST' });
  assert.deepEqual((await registry.recover()).unverified, []);
});

test('a job executing in this process is never reconciled, even with an override', async t => {
  const directory = await temporaryDirectory(t);
  const entered = deferred();
  const release = deferred();
  const owner = new FilePhaseJobRegistry({ directory, execute: async () => { entered.resolve(); await release.promise; return { status: 'COMPLETED' }; } });
  await owner.start({ jobId: 'job_1', objective: 'Busy' });
  await entered.promise;
  const { sequence } = await owner.get('job_1');
  const request = { jobId: 'job_1', expectedSequence: sequence, disposition: 'FAILED', evidence, explicitAuthorization: true, overrideRunningOwner: true };
  await assert.rejects(owner.reconcile(request), { code: 'JOB_RUNNING_HERE' });
  await assert.rejects(new FilePhaseJobRegistry({ directory, execute: noExecute }).reconcile(request), { code: 'JOB_RUNNING_HERE' });
  release.resolve();
  assert.equal((await owner.wait('job_1')).status, 'COMPLETED');
});

test('overriding a live owner in another process: the reconciliation stands, its late outcome is refused', async t => {
  const directory = await temporaryDirectory(t);
  const releaseFile = path.join(directory, 'release');
  const source = `
    const { access } = await import('node:fs/promises');
    const registry = new FilePhaseJobRegistry({ directory, execute: async () => {
      emit({ running: true });
      for (;;) { try { await access(args[0]); break; } catch { await new Promise(resolve => setTimeout(resolve, 10)); } }
      return { status: 'COMPLETED', late: true };
    } });
    await registry.start({ jobId: 'job_1', objective: 'Slow owner' });
    const settled = await registry.wait('job_1', { timeoutMs: 15000 });
    emit({ status: settled.status, reason: settled.reason, result: settled.result });`;
  const child = startChild(source, [directory, releaseFile]);
  t.after(() => child.kill('SIGKILL'));
  await child.line();
  const registry = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const running = await registry.get('job_1');
  assert.deepEqual([running.status, running.ownerState], ['RUNNING', 'ALIVE']);
  const request = { jobId: 'job_1', expectedSequence: running.sequence, disposition: 'CANCELLED', evidence, explicitAuthorization: true };
  await assert.rejects(registry.reconcile(request), error => error.code === 'JOB_OWNER_UNVERIFIED' && error.ownerState === 'ALIVE');
  const reconciled = await registry.reconcile({ ...request, overrideRunningOwner: true });
  assert.equal(reconciled.reconciliation.override.ownerState, 'ALIVE');

  await writeFile(releaseFile, '');
  const { code, stdout, stderr } = await child.exited;
  assert.equal(code, 0, stderr);
  const ownerView = JSON.parse(stdout.trim().split('\n').at(-1));
  assert.deepEqual(ownerView, { status: 'CANCELLED', reason: 'RECONCILED', result: null }, 'the owner reports the reconciled record');
  assert.deepEqual(await registry.get('job_1'), reconciled);
});

test('concurrent reconciliations of the same state: exactly one wins', async t => {
  const directory = await temporaryDirectory(t);
  const unknown = await unknownJob(directory, 'job_1');
  const attempts = ['COMPLETED', 'FAILED', 'CANCELLED'].map(disposition =>
    new FilePhaseJobRegistry({ directory, execute: noExecute }).reconcile({ jobId: 'job_1', expectedSequence: unknown.sequence,
      disposition, evidence, explicitAuthorization: true }));
  const results = await Promise.allSettled(attempts);
  const won = results.filter(result => result.status === 'fulfilled');
  assert.equal(won.length, 1);
  assert.ok(results.filter(result => result.status === 'rejected').every(result =>
    ['JOB_RECORD_CHANGED', 'JOB_RECONCILIATION_NOT_REQUIRED'].includes(result.reason.code)));
  assert.equal((await new FileJobStore({ directory }).read('job_1')).status, won[0].value.status);
});

test('a job whose owner exited but was not reaped is reclassified', { skip: process.platform !== 'linux' }, async t => {
  const directory = await temporaryDirectory(t);
  const { pid } = await zombieProcess(t);
  await leftRunning(directory, 'zombie_owner', { pid, host: hostname(), executionId: 'exited' });
  const view = await new FilePhaseJobRegistry({ directory, execute: noExecute }).get('zombie_owner');
  assert.deepEqual([view.status, view.reason], ['UNKNOWN', 'OWNER_DIED_AFTER_DISPATCH']);
});

// Owner identity: a dead owner's PID reused by an unrelated process.
async function unrelatedProcess(t) {
  const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });
  t.after(() => child.kill('SIGKILL'));
  await new Promise((resolve, reject) => { child.once('spawn', resolve); child.once('error', reject); });
  return processIdentity(child.pid);
}

test('new jobs record their owner\'s process identity', { skip: process.platform !== 'linux' }, async t => {
  const directory = await temporaryDirectory(t);
  const registry = new FilePhaseJobRegistry({ directory, execute: async () => ({ status: 'COMPLETED' }) });
  await registry.start({ jobId: 'job_1', objective: 'Identify the owner' });
  const { owner } = await registry.wait('job_1');
  const own = processIdentity();
  assert.deepEqual([owner.pid, owner.bootId, owner.startTicks], [process.pid, own.bootId, own.startTicks]);
});

test('an owner whose PID now belongs to another process is DEAD; the real owner is ALIVE', { skip: process.platform !== 'linux' }, async t => {
  const directory = await temporaryDirectory(t);
  const other = await unrelatedProcess(t);
  const owner = executionId => ({ pid: other.pid, host: hostname(), executionId, bootId: other.bootId });
  // Recorded by a process that had this PID but started earlier: it is gone, its PID was reused.
  await leftRunning(directory, 'reused', { ...owner('earlier'), startTicks: String(BigInt(other.startTicks) - 1n) });
  // Recorded by the process that runs under this PID now: still alive.
  await leftRunning(directory, 'current', { ...owner('current'), startTicks: other.startTicks });
  const registry = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const reused = await registry.get('reused');
  assert.deepEqual([reused.status, reused.reason], ['UNKNOWN', 'OWNER_DIED_AFTER_DISPATCH']);
  const current = await registry.get('current');
  assert.deepEqual([current.status, current.ownerState], ['RUNNING', 'ALIVE']);
});

test('an owner recorded before a reboot is DEAD, even if its PID runs again', { skip: process.platform !== 'linux' }, async t => {
  const directory = await temporaryDirectory(t);
  const other = await unrelatedProcess(t);
  await leftRunning(directory, 'before_reboot', { pid: other.pid, host: hostname(), executionId: 'old',
    bootId: '00000000-0000-4000-8000-000000000000', startTicks: other.startTicks });
  const view = await new FilePhaseJobRegistry({ directory, execute: noExecute }).get('before_reboot');
  assert.deepEqual([view.status, view.reason], ['UNKNOWN', 'OWNER_DIED_AFTER_DISPATCH']);
});

test('limit: an owner recorded without identity is judged by its PID alone', async t => {
  const directory = await temporaryDirectory(t);
  const other = await unrelatedProcess(t);
  await leftRunning(directory, 'legacy', { pid: other.pid, host: hostname(), executionId: 'old' });
  const view = await new FilePhaseJobRegistry({ directory, execute: noExecute }).get('legacy');
  assert.deepEqual([view.status, view.ownerState], ['RUNNING', 'ALIVE'], 'its PID runs, so it may be the owner');
});

// Recorded process trees (A6): a job's processes are on record before they start.

// A command that, once it runs, checks its job record: is its tree (its marker and
// its supervisor, which is its parent) already recorded? It writes the answer, then runs.
const selfChecking = (recordPath, out, then = 'setInterval(() => {}, 1000);') => `
  const fs = require('node:fs');
  const marker = Object.keys(process.env).find(name => name.startsWith('MNEMOS_TREE_'));
  const { record } = JSON.parse(fs.readFileSync(${JSON.stringify(recordPath)}, 'utf8'));
  const entry = (record.processes || []).find(candidate => candidate.marker === marker);
  fs.writeFileSync(${JSON.stringify(out)}, JSON.stringify({ marker, recorded: Boolean(entry),
    supervisorIsParent: entry?.supervisor.pid === process.ppid, leaderYet: entry ? entry.leader : 'missing', pid: process.pid }));
  ${then}`;
const recordPathOf = (directory, jobId) => path.join(directory, `${createHash('sha256').update(jobId).digest('hex')}.job.json`);
const isGone = pid => identityState({ pid, host: hostname() }) === 'DEAD';

async function readWhenWritten(file) {
  let value;
  await until(async () => { try { value = JSON.parse(await readFile(file, 'utf8')); return true; } catch { return false; } }, file);
  return value;
}

test('runProcess records the process tree before the command starts, and its leader once it runs', { skip: process.platform === 'win32' }, async t => {
  const directory = await temporaryDirectory(t);
  const out = path.join(directory, 'seen.json');
  const jobs = new FilePhaseJobRegistry({ directory, execute: ({ runProcess }) =>
    runProcess({ command: process.execPath, args: ['-e', selfChecking(recordPathOf(directory, 'build'), out, '')] }) });
  await jobs.start({ jobId: 'build', objective: 'Build' });
  const done = await jobs.wait('build', { timeoutMs: 20_000 });
  const seen = await readWhenWritten(out);
  // The leader is recorded a few milliseconds after the start, usually before the command reads its record.
  assert.deepEqual([seen.recorded, seen.supervisorIsParent], [true, true],
    'the marker and the supervisor were durable before the command ran');
  assert.equal(done.status, 'COMPLETED');
  assert.equal(done.processes.length, 1);
  const [entry] = done.processes;
  assert.deepEqual([entry.marker, entry.leader.pid], [seen.marker, seen.pid], 'the leader is recorded once it runs');
  if (process.platform === 'linux') assert.match(entry.leader.startTicks, /^\d+$/);
  assert.deepEqual((await new FileJobStore({ directory }).read('build')).processes, done.processes);
});

test('a cancelled job keeps its process records; the tree is stopped', { skip: process.platform === 'win32' }, async t => {
  const directory = await temporaryDirectory(t);
  const out = path.join(directory, 'seen.json');
  const jobs = new FilePhaseJobRegistry({ directory, execute: ({ runProcess }) =>
    runProcess({ command: process.execPath, args: ['-e', selfChecking(recordPathOf(directory, 'serve'), out)], graceMs: 2000 }) });
  await jobs.start({ jobId: 'serve', objective: 'Serve' });
  const seen = await readWhenWritten(out);
  t.after(() => { try { process.kill(seen.pid, 'SIGKILL'); } catch {} });
  const cancelled = await jobs.requestCancellation('serve', { timeoutMs: 10_000 });
  assert.deepEqual([cancelled.status, cancelled.cancellationConfirmed], ['CANCELLED', true]);
  assert.deepEqual(cancelled.processes.map(entry => [entry.marker, entry.leader.pid]), [[seen.marker, seen.pid]]);
  assert.ok(isGone(seen.pid));
});

test('if the process tree cannot be recorded, the command never starts', { skip: process.platform === 'win32' }, async t => {
  const directory = await temporaryDirectory(t);
  const started = path.join(directory, 'started');
  let broken = false;
  const clock = () => (broken ? new Date(NaN) : new Date());
  const jobs = new FilePhaseJobRegistry({ directory, clock, execute: async ({ runProcess }) => {
    broken = true; // the record of the tree cannot be written
    const result = await runProcess({ command: process.execPath, args: ['-e', `require('node:fs').writeFileSync(${JSON.stringify(started)}, '')`] });
    broken = false;
    return result;
  } });
  await jobs.start({ jobId: 'job_1', objective: 'Unrecordable' });
  const done = await jobs.wait('job_1', { timeoutMs: 20_000 });
  assert.deepEqual([done.status, done.effectAbsent, done.processes], ['FAILED', true, []]);
  await sleep(300);
  await assert.rejects(readFile(started), { code: 'ENOENT' }, 'the command never ran');
});

test('each command a job runs is recorded, in order', { skip: process.platform === 'win32' }, async t => {
  const directory = await temporaryDirectory(t);
  const jobs = new FilePhaseJobRegistry({ directory, execute: async ({ runProcess }) => {
    const first = await runProcess({ command: process.execPath, args: ['-e', ''] });
    const second = await runProcess({ command: process.execPath, args: ['-e', 'process.exitCode = 2'] });
    return { status: first.status === 'COMPLETED' && second.status === 'FAILED' ? 'COMPLETED' : 'FAILED' };
  } });
  await jobs.start({ jobId: 'steps', objective: 'Two steps' });
  const done = await jobs.wait('steps', { timeoutMs: 20_000 });
  assert.equal(done.status, 'COMPLETED');
  assert.equal(done.processes.length, 2);
  assert.notEqual(done.processes[0].marker, done.processes[1].marker);
  assert.ok(done.processes.every(entry => entry.leader !== null));
});

test('a job killed mid-command leaves its process tree on record for a restart', { skip: process.platform === 'win32' }, async t => {
  const directory = await temporaryDirectory(t);
  const out = path.join(directory, 'seen.json');
  const registryUrl = new URL('../src/index.mjs', import.meta.url).href;
  const owner = spawn(process.execPath, ['--input-type=module', '-e', `
    import { FilePhaseJobRegistry } from ${JSON.stringify(registryUrl)};
    const jobs = new FilePhaseJobRegistry({ directory: ${JSON.stringify(directory)}, execute: ({ runProcess }) =>
      runProcess({ command: process.execPath, args: ['-e', ${JSON.stringify(selfChecking(recordPathOf(directory, 'long'), out))}] }) });
    await jobs.start({ jobId: 'long', objective: 'Long' });`], { stdio: 'ignore' });
  const ownerExited = new Promise(resolve => owner.once('exit', resolve));
  t.after(() => owner.kill('SIGKILL'));
  const seen = await readWhenWritten(out);
  t.after(() => { try { process.kill(seen.pid, 'SIGKILL'); } catch {} });
  await until(async () => (await new FileJobStore({ directory }).read('long')).processes?.[0]?.leader !== null, 'the leader record');
  owner.kill('SIGKILL');
  await withTimeout(ownerExited, 10_000, 'the owner did not exit'); // a signal is not delivered instantly
  const restarted = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const first = await restarted.get('long');
  // While the supervisor is still stopping the tree, the work is still running.
  assert.ok(first.status === 'UNKNOWN' || (first.status === 'RUNNING' && first.orphaned === true), JSON.stringify(first));
  const view = await restarted.wait('long', { timeoutMs: 15_000 });
  assert.deepEqual([view.status, view.reason], ['UNKNOWN', 'OWNER_DIED_AFTER_DISPATCH']);
  assert.deepEqual(view.processes.map(entry => [entry.marker, entry.leader.pid]), [[seen.marker, seen.pid]],
    'what a restart needs to find the tree');
  await until(() => isGone(seen.pid), 'the supervisor to stop the orphaned tree');
});

// Threads of one process share its PID but not their lists of running executions (B13).

test('a job running in another thread of this process is alive there, never reclassified or resumed', async t => {
  const directory = await temporaryDirectory(t);
  const worker = workerThread(t, `
    const { FilePhaseJobRegistry } = await import(workerData.indexUrl);
    // Reports from inside the executor: its dispatch is recorded by then, so the record stays as inspected.
    const jobs = new FilePhaseJobRegistry({ directory: workerData.directory, execute: () => new Promise(resolve => {
      parentPort.postMessage('running');
      parentPort.once('message', () => resolve({ status: 'COMPLETED' }));
    }) });
    await jobs.start({ jobId: 'batch_import', objective: 'Import a batch', resumable: true });
    parentPort.postMessage((await jobs.wait('batch_import')).status);`,
  { directory, indexUrl: new URL('../src/index.mjs', import.meta.url).href });
  assert.equal(await worker.next(), 'running');
  const jobs = new FilePhaseJobRegistry({ directory, execute: async () => { throw new Error('must not run'); } });
  const seen = await jobs.get('batch_import');
  assert.deepEqual([seen.status, seen.ownerState], ['RUNNING', 'ALIVE']);
  assert.deepEqual((await jobs.recover()).resumable, []);
  await assert.rejects(jobs.resume({ jobId: 'batch_import', expectedSequence: seen.sequence, successorJobId: 'batch_import_2',
    explicitAuthorization: true }), error => error.code === 'JOB_STOP_UNVERIFIED');
  // An operator here may settle it only with the override, as for any live owner elsewhere.
  await assert.rejects(jobs.reconcile({ jobId: 'batch_import', expectedSequence: seen.sequence, disposition: 'FAILED',
    evidence: { authoritative: true, source: 'import-api:batch/9' }, explicitAuthorization: true }),
  error => error.code === 'JOB_OWNER_UNVERIFIED' && error.ownerState === 'ALIVE');
  const finished = worker.next();
  worker.send('finish');
  assert.equal(await finished, 'COMPLETED');
  assert.equal((await jobs.get('batch_import')).status, 'COMPLETED');
});
