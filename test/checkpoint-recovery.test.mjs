import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, writeFile, readdir } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import path from 'node:path';
import { FileCheckpointStore, GoalRecoveryCoordinator, PhaseJobRegistry } from '../src/index.mjs';
import { temporaryDirectory, deferred } from './helpers.mjs';

test('new store instance recovers a persisted checkpoint and reconciles each job', async t => {
  const directory = await temporaryDirectory(t);
  const first = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) });
  await first.checkpoint({ goal: { goalId: 'example_goal' }, dag: { nodes: ['a'] }, activeJobs: [{ id: 'done' }, { id: 'lost' }] });
  const second = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory), jobInspector: async job => ({ status: job.id === 'done' ? 'COMPLETED' : 'UNKNOWN' }) });
  const result = await second.recover();
  assert.deepEqual(result.jobs.map(job => job.recoveryStatus), ['COMPLETED', 'UNKNOWN']);
  assert.equal(result.goal.goalId, 'example_goal');
  assert.equal(result.recoveryRequired, true);
});

test('missing checkpoint is distinct from a corrupt checkpoint', async t => {
  const store = new FileCheckpointStore(await temporaryDirectory(t));
  const coordinator = new GoalRecoveryCoordinator({ store });
  assert.equal(await coordinator.recover(), null);
  await writeFile(store.file, '{not-json');
  const result = await coordinator.recover();
  assert.equal(result.recoveryError, 'CHECKPOINT_UNREADABLE');
  assert.equal(result.goal, null);
});

test('invalid shape and duplicate jobs are rejected without running an inspector', async t => {
  const store = new FileCheckpointStore(await temporaryDirectory(t));
  const coordinator = new GoalRecoveryCoordinator({ store, jobInspector: () => { throw new Error('must not inspect'); } });
  await store.save({ version: 1, goal: { goalId: 'bad' }, activeJobs: 'wrong' });
  assert.equal((await coordinator.recover()).recoveryError, 'CHECKPOINT_INVALID');
  await assert.rejects(coordinator.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [{ id: 'same' }, { id: 'same' }] }), /Invalid checkpoint/);
});

test('inspector failure and unsupported status remain UNKNOWN', async t => {
  const store = new FileCheckpointStore(await temporaryDirectory(t));
  const coordinator = new GoalRecoveryCoordinator({ store, jobInspector: async job => {
    if (job.id === 'throws') throw new Error('offline');
    return { status: 'probably_done' };
  } });
  await coordinator.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [{ id: 'throws' }, { id: 'odd' }] });
  assert.deepEqual((await coordinator.recover()).jobs.map(job => job.recoveryStatus), ['UNKNOWN', 'UNKNOWN']);
});

test('serial writes leave complete JSON and no temporary files', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileCheckpointStore(directory);
  await Promise.all(Array.from({ length: 12 }, (_, index) => store.save({ index, text: 'x'.repeat(100) })));
  assert.equal(JSON.parse(await readFile(store.file, 'utf8')).index, 11);
  assert.deepEqual(await readdir(directory), ['checkpoint.json']);
});

test('oversized writes preserve the last valid checkpoint', async t => {
  const store = new FileCheckpointStore(await temporaryDirectory(t));
  await store.save({ valid: true });
  assert.throws(() => store.save({ text: 'x'.repeat(1024 * 1024) }), RangeError);
  assert.deepEqual(await store.load(), { valid: true });
});

test('oversized checkpoint reads fail closed', async t => {
  const store = new FileCheckpointStore(await temporaryDirectory(t));
  await writeFile(store.file, 'x'.repeat(1024 * 1024 + 1));
  assert.equal((await new GoalRecoveryCoordinator({ store }).recover()).recoveryError, 'CHECKPOINT_UNREADABLE');
});

test('DurableJobRunner does not start or register a job when the initial checkpoint save fails', async () => {
  // Loaded dynamically so the missing export fails only this test, not the whole file.
  const { DurableJobRunner } = await import('../src/index.mjs');
  assert.equal(typeof DurableJobRunner, 'function', 'DurableJobRunner must be exported from src/index.mjs');

  const saveError = new Error('disk unavailable');
  let saves = 0, calls = 0;
  const store = { save: async () => { saves++; throw saveError; }, load: async () => null };
  const registry = new PhaseJobRegistry({ execute: async () => { calls++; return { status: 'COMPLETED' }; } });
  const runner = new DurableJobRunner({
    registry,
    coordinator: new GoalRecoveryCoordinator({ store }),
    goal: { goalId: 'durable_goal' }
  });

  await assert.rejects(runner.start({ jobId: 'durable_job_1', objective: 'Durable work' }),
    error => error === saveError || error?.cause === saveError);
  await new Promise(resolve => setImmediate(resolve)); // let any stray dispatch run
  assert.equal(saves, 1);
  assert.equal(calls, 0);
  assert.equal(registry.get('durable_job_1'), null);
});

test('DurableJobRunner preserves every active job in the checkpoint', async t => {
  const { DurableJobRunner } = await import('../src/index.mjs');
  const directory = await temporaryDirectory(t);
  const ids = ['multi_job_1', 'multi_job_2'];
  const entered = Object.fromEntries(ids.map(id => [id, deferred()]));
  const release = Object.fromEntries(ids.map(id => [id, deferred()]));
  const registry = new PhaseJobRegistry({ execute: async ({ jobId }) => {
    entered[jobId].resolve();
    await release[jobId].promise;
    return { status: 'COMPLETED' };
  } });
  // Registered before any assertion so pending executors are released even on failure.
  t.after(async () => {
    for (const id of ids) release[id].resolve();
    await Promise.all(ids.map(id => registry.wait(id)));
  });
  const runner = new DurableJobRunner({
    registry,
    coordinator: new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) }),
    goal: { goalId: 'multi_goal' }
  });

  await runner.start({ jobId: 'multi_job_1', objective: 'First job' });
  await runner.start({ jobId: 'multi_job_2', objective: 'Second job' });
  await Promise.all(ids.map(id => entered[id].promise)); // both executors dispatched and still pending

  const checkpoint = await new FileCheckpointStore(directory).load();
  assert.deepEqual(checkpoint.activeJobs.map(job => job.id).sort(), ids);
});

test('DurableJobRunner keeps both jobs when concurrent start() calls read the same checkpoint', async t => {
  const { DurableJobRunner } = await import('../src/index.mjs');
  const directory = await temporaryDirectory(t);
  const ids = ['race_job_1', 'race_job_2'];
  const release = Object.fromEntries(ids.map(id => [id, deferred()]));
  const readsDone = deferred(); // barrier: no read returns until every started read has its data
  const observed = [];
  class BarrierStore extends FileCheckpointStore {
    entered = 0;
    completed = 0;
    async load() {
      this.entered++;
      const value = await super.load();
      observed.push(value?.activeJobs?.map(job => job.id) ?? null);
      if (++this.completed === this.entered) readsDone.resolve();
      await readsDone.promise;
      return value;
    }
  }
  const store = new BarrierStore(directory);
  const coordinator = new GoalRecoveryCoordinator({ store });
  await coordinator.checkpoint({ goal: { goalId: 'race_goal' }, activeJobs: [] });
  const registry = new PhaseJobRegistry({ execute: async ({ jobId }) => {
    await release[jobId].promise;
    return { status: 'COMPLETED' };
  } });
  const runner = new DurableJobRunner({ registry, coordinator, goal: { goalId: 'race_goal' } });

  const starts = [
    runner.start({ jobId: 'race_job_1', objective: 'First concurrent job' }),
    runner.start({ jobId: 'race_job_2', objective: 'Second concurrent job' })
  ];
  try {
    await Promise.all(starts);
    const checkpoint = await new FileCheckpointStore(directory).load();
    assert.deepEqual(checkpoint.activeJobs.map(job => job.id).sort(), ids,
      `lost update; reads observed ${JSON.stringify(observed)}`);
  } finally {
    readsDone.resolve();
    for (const id of ids) release[id].resolve();
    await Promise.allSettled(starts);
    await Promise.all(ids.map(id => registry.wait(id)));
  }
});

test('DurableJobRunner does not re-execute a checkpointed job after a restart', async t => {
  const { DurableJobRunner } = await import('../src/index.mjs');
  const directory = await temporaryDirectory(t);
  const jobId = 'restart_job_1';

  // First process lifetime.
  let firstCalls = 0;
  const firstRegistry = new PhaseJobRegistry({ execute: async () => { firstCalls++; return { status: 'COMPLETED' }; } });
  const firstCoordinator = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) });
  await firstCoordinator.checkpoint({ goal: { goalId: 'restart_goal' }, dag: { nodes: ['report'] }, activeJobs: [] });
  const firstRunner = new DurableJobRunner({ registry: firstRegistry, coordinator: firstCoordinator, goal: { goalId: 'restart_goal' } });
  await firstRunner.start({ jobId, objective: 'Create report once' });
  assert.equal((await firstRegistry.wait(jobId)).status, 'COMPLETED');
  assert.equal(firstCalls, 1);
  const before = await new FileCheckpointStore(directory).load();

  // Reconstructed application state after a restart: nothing shared with the first lifetime.
  let secondCalls = 0;
  const secondRegistry = new PhaseJobRegistry({ execute: async () => { secondCalls++; return { status: 'COMPLETED' }; } });
  const secondRunner = new DurableJobRunner({
    registry: secondRegistry,
    coordinator: new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) }),
    goal: { goalId: 'restart_goal' }
  });
  // Only an explicit reconciliation rejection counts; unrelated errors do not.
  await assert.rejects(secondRunner.start({ jobId, objective: 'Create report once' }),
    error => error?.code === 'JOB_RECONCILIATION_REQUIRED' && error.reconciliationRequired === true);
  await secondRegistry.wait(jobId); // settles any execution that was dispatched

  assert.equal(secondCalls, 0, 'fresh registry executed an already checkpointed job again');
  assert.equal(secondRegistry.get(jobId), null);
  const after = await new FileCheckpointStore(directory).load();
  assert.deepEqual(after, before); // not rewritten, including checkpointedAt
  assert.ok(after.activeJobs.some(job => job.id === jobId));
});

test('DurableJobRunner durably records completion without permitting replay', async t => {
  const { DurableJobRunner } = await import('../src/index.mjs');
  const directory = await temporaryDirectory(t);
  const jobId = 'final_state_job_1';
  const goal = { goalId: 'final_state_goal' };
  let firstCalls = 0;
  const registry = new PhaseJobRegistry({ execute: async () => { firstCalls++; return { status: 'COMPLETED' }; } });
  const runner = new DurableJobRunner({
    registry, coordinator: new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) }), goal
  });
  assert.equal(typeof runner.wait, 'function', 'DurableJobRunner.wait(jobId) must be implemented');

  await runner.start({ jobId, objective: 'Complete once' });
  await runner.wait(jobId); // must resolve only after the terminal state is durably saved
  assert.equal(firstCalls, 1);

  const checkpoint = await new FileCheckpointStore(directory).load();
  const entry = checkpoint.activeJobs.find(job => job.id === jobId);
  assert.ok(entry, 'completed job must remain in activeJobs');
  assert.equal(entry.status, 'COMPLETED');

  // Recording completion must not permit replay from a reconstructed runner.
  let secondCalls = 0;
  const secondRegistry = new PhaseJobRegistry({ execute: async () => { secondCalls++; return { status: 'COMPLETED' }; } });
  const secondRunner = new DurableJobRunner({
    registry: secondRegistry, coordinator: new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) }), goal
  });
  await assert.rejects(secondRunner.start({ jobId, objective: 'Complete once' }),
    error => error?.code === 'JOB_RECONCILIATION_REQUIRED' && error.reconciliationRequired === true);
  assert.equal(secondCalls, 0);
  assert.equal(secondRegistry.get(jobId), null);
});

test('DurableJobRunner durably records failure without permitting replay', async t => {
  const { DurableJobRunner } = await import('../src/index.mjs');
  const directory = await temporaryDirectory(t);
  const jobId = 'failed_state_job_1';
  const goal = { goalId: 'failed_state_goal' };
  let firstCalls = 0;
  const registry = new PhaseJobRegistry({ execute: async () => { firstCalls++; throw new Error('synthetic executor failure'); } });
  const runner = new DurableJobRunner({
    registry, coordinator: new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) }), goal
  });

  await runner.start({ jobId, objective: 'Fail once' });
  const snapshot = await runner.wait(jobId);
  assert.equal(snapshot.status, 'FAILED');
  assert.equal(firstCalls, 1);

  const checkpoint = await new FileCheckpointStore(directory).load();
  const entry = checkpoint.activeJobs.find(job => job.id === jobId);
  assert.ok(entry, 'failed job must remain in activeJobs');
  assert.equal(entry.status, 'FAILED');

  // FAILED is not proof that no external effect occurred, so it must not permit replay.
  let secondCalls = 0;
  const secondRegistry = new PhaseJobRegistry({ execute: async () => { secondCalls++; return { status: 'COMPLETED' }; } });
  const secondRunner = new DurableJobRunner({
    registry: secondRegistry, coordinator: new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) }), goal
  });
  await assert.rejects(secondRunner.start({ jobId, objective: 'Fail once' }),
    error => error?.code === 'JOB_RECONCILIATION_REQUIRED' && error.reconciliationRequired === true);
  assert.equal(secondCalls, 0);
  assert.equal(secondRegistry.get(jobId), null);
});

// ---- Durable lifecycle: CANCELLED / UNKNOWN persistence, persistence failures, fresh-process recovery ----

const LIFECYCLE_GOAL = { goalId: 'lifecycle_goal' };

async function lifecycleRunner(directory, execute, store = new FileCheckpointStore(directory)) {
  const { DurableJobRunner } = await import('../src/index.mjs');
  const registry = new PhaseJobRegistry({ execute });
  const runner = new DurableJobRunner({ registry, coordinator: new GoalRecoveryCoordinator({ store }), goal: LIFECYCLE_GOAL });
  return { registry, runner };
}

// Executors that start, signal it, and settle only after abort with the given outcome.
function abortableJobs(ids, onAbort) {
  const started = Object.fromEntries(ids.map(id => [id, deferred()]));
  const execute = ({ jobId, signal }) => new Promise((resolve, reject) => {
    const settle = () => { try { resolve(onAbort(jobId)); } catch (error) { reject(error); } };
    if (signal.aborted) settle(); else signal.addEventListener('abort', settle, { once: true });
    started[jobId].resolve();
  });
  return { started, execute };
}

// Abort is the only way these executors settle, so this leaves nothing pending.
async function settleAll(registry, ids) {
  for (const id of ids) {
    if (registry.get(id)?.status === 'RUNNING') await registry.requestCancellation(id, { timeoutMs: 30000 });
    await registry.wait(id);
  }
}

async function assertReplayBlocked(directory, ids) {
  let calls = 0;
  const { registry, runner } = await lifecycleRunner(directory, async () => { calls++; return { status: 'COMPLETED' }; });
  for (const jobId of ids) {
    await assert.rejects(runner.start({ jobId, objective: 'Replay attempt' }),
      error => error?.code === 'JOB_RECONCILIATION_REQUIRED' && error.reconciliationRequired === true);
    assert.equal(registry.get(jobId), null);
  }
  assert.equal(calls, 0);
}

const savedStatuses = async directory =>
  Object.fromEntries((await new FileCheckpointStore(directory).load()).activeJobs.map(job => [job.id, job.status ?? null]));

test('DurableJobRunner durably records confirmed cancellation without permitting replay', async t => {
  const directory = await temporaryDirectory(t);
  const ids = ['confirmed_cancel_1'];
  const { started, execute } = abortableJobs(ids, () => ({ status: 'CANCELLED', termination: { confirmed: true } }));
  const { registry, runner } = await lifecycleRunner(directory, execute);
  try {
    await runner.start({ jobId: ids[0], objective: 'Cancel with confirmation' });
    await started[ids[0]].promise;
    await registry.requestCancellation(ids[0], { timeoutMs: 30000 });
    const snapshot = await runner.wait(ids[0]);
    assert.equal(snapshot.status, 'CANCELLED');
    assert.equal(snapshot.cancellationConfirmed, true);
    assert.deepEqual(await savedStatuses(directory), { confirmed_cancel_1: 'CANCELLED' });
    await assertReplayBlocked(directory, ids);
  } finally {
    await settleAll(registry, ids);
  }
});

test('DurableJobRunner records unconfirmed cancellation as UNKNOWN without permitting replay', async t => {
  const directory = await temporaryDirectory(t);
  const ids = ['unconfirmed_cancel_1', 'throws_after_abort_1'];
  const { started, execute } = abortableJobs(ids, jobId => {
    if (jobId === 'throws_after_abort_1') throw new Error('synthetic failure during cancellation');
    return { status: 'CANCELLED' }; // no termination evidence
  });
  const { registry, runner } = await lifecycleRunner(directory, execute);
  try {
    for (const jobId of ids) {
      await runner.start({ jobId, objective: 'Cancel without confirmation' });
      await started[jobId].promise;
      await registry.requestCancellation(jobId, { timeoutMs: 30000 });
      const snapshot = await runner.wait(jobId);
      assert.equal(snapshot.status, 'UNKNOWN');
      assert.equal(snapshot.cancellationConfirmed, false);
      assert.equal(snapshot.reconciliationRequired, true);
    }
    assert.deepEqual(await savedStatuses(directory), { unconfirmed_cancel_1: 'UNKNOWN', throws_after_abort_1: 'UNKNOWN' });
    const text = await readFile(new FileCheckpointStore(directory).file, 'utf8');
    assert.doesNotMatch(text, /synthetic failure/);
    await assertReplayBlocked(directory, ids);
  } finally {
    await settleAll(registry, ids);
  }
});

test('DurableJobRunner never saves CANCELLED without confirmed termination, whatever the registry reports', async t => {
  const { DurableJobRunner } = await import('../src/index.mjs');
  const directory = await temporaryDirectory(t);
  // A registry adapter reporting CANCELLED without confirmation must not be trusted.
  const registry = {
    start: ({ jobId }) => ({ jobId, status: 'RUNNING' }),
    wait: async jobId => ({ jobId, status: 'CANCELLED', cancellationRequested: true, cancellationConfirmed: false })
  };
  const runner = new DurableJobRunner({
    registry, coordinator: new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) }), goal: LIFECYCLE_GOAL
  });
  await runner.start({ jobId: 'adapter_cancel_1', objective: 'Unconfirmed adapter cancellation' });
  assert.equal((await runner.wait('adapter_cancel_1')).status, 'CANCELLED');
  assert.deepEqual(await savedStatuses(directory), { adapter_cancel_1: 'UNKNOWN' });
  await assertReplayBlocked(directory, ['adapter_cancel_1']);
});

test('DurableJobRunner.wait rejects without claiming a terminal state when persistence fails', async t => {
  const directory = await temporaryDirectory(t);
  const real = new FileCheckpointStore(directory);
  const saveError = new Error('synthetic disk failure');
  let failSaves = false;
  const store = { load: () => real.load(), save: value => failSaves ? Promise.reject(saveError) : real.save(value) };
  const expected = { persist_completed: 'COMPLETED', persist_failed: 'FAILED', persist_cancelled: 'CANCELLED', persist_unknown: 'UNKNOWN' };
  const abortable = ['persist_cancelled', 'persist_unknown'];
  const { started, execute: abortExecute } = abortableJobs(abortable, jobId =>
    jobId === 'persist_cancelled' ? { status: 'CANCELLED', termination: { confirmed: true } } : { status: 'CANCELLED' });
  const { registry, runner } = await lifecycleRunner(directory, args => {
    if (args.jobId === 'persist_completed') return { status: 'COMPLETED' };
    if (args.jobId === 'persist_failed') throw new Error('synthetic executor failure');
    return abortExecute(args);
  }, store);
  try {
    for (const jobId of Object.keys(expected)) await runner.start({ jobId, objective: 'Persistence failure' });
    for (const jobId of abortable) { await started[jobId].promise; await registry.requestCancellation(jobId, { timeoutMs: 30000 }); }
    failSaves = true;
    for (const [jobId, status] of Object.entries(expected)) {
      await assert.rejects(runner.wait(jobId), error =>
        error?.reconciliationRequired === true && error.cause === saveError && error.message.startsWith(`${status} state of job ${jobId}`));
    }
    failSaves = false;
    // No terminal state was claimed, and every job stays protected from replay.
    assert.deepEqual(await savedStatuses(directory),
      { persist_completed: null, persist_failed: null, persist_cancelled: null, persist_unknown: null });
    await assertReplayBlocked(directory, Object.keys(expected));
  } finally {
    failSaves = false;
    await settleAll(registry, Object.keys(expected));
  }
});

test('DurableJobRunner does not let a concurrent start() overwrite a recorded terminal state', async t => {
  const directory = await temporaryDirectory(t);
  const readsDone = deferred();
  let armed = false, entered = 0, completed = 0;
  class BarrierStore extends FileCheckpointStore {
    async load() {
      if (!armed) return super.load();
      entered++;
      const value = await super.load();
      if (++completed === entered) readsDone.resolve(); // holds reads only while another read is in flight
      await readsDone.promise;
      return value;
    }
  }
  const { registry, runner } = await lifecycleRunner(directory, async () => ({ status: 'COMPLETED' }), new BarrierStore(directory));
  try {
    await runner.start({ jobId: 'recorded_job', objective: 'Finish first' });
    await registry.wait('recorded_job');
    armed = true;
    await Promise.all([runner.wait('recorded_job'), runner.start({ jobId: 'late_job', objective: 'Start concurrently' })]);
    assert.deepEqual(await savedStatuses(directory), { recorded_job: 'COMPLETED', late_job: null });
  } finally {
    readsDone.resolve();
    await registry.wait('recorded_job');
    await registry.wait('late_job');
  }
});

const PROCESS_CHILD = `
const [moduleUrl, directory, effects, mode, jobId] = process.argv.slice(1);
const { DurableJobRunner, PhaseJobRegistry, GoalRecoveryCoordinator, FileCheckpointStore } = await import(moduleUrl);
const { appendFile } = await import('node:fs/promises');
const store = new FileCheckpointStore(directory);
const registry = new PhaseJobRegistry({ execute: async () => {
  await appendFile(effects, jobId + '\\n');                      // the external side effect
  if (mode === 'crash') {
    process.kill(process.pid, 'SIGKILL');  // die before any terminal state is saved
    await new Promise(() => {});           // never settle, even if termination is asynchronous (Windows)
  }
  return { status: 'COMPLETED' };
} });
const runner = new DurableJobRunner({ registry, coordinator: new GoalRecoveryCoordinator({ store }), goal: { goalId: 'process_goal' } });
if (mode === 'recover') {
  const recovery = await new GoalRecoveryCoordinator({ store, jobInspector: async job => ({ status: job.status ?? 'UNKNOWN' }) }).recover();
  const attempt = await runner.start({ jobId, objective: 'Write effect once' }).then(() => 'STARTED', error => error.code ?? 'ERROR');
  await registry.wait(jobId);
  console.log(JSON.stringify({ jobs: recovery.jobs.map(job => [job.id, job.recoveryStatus]), recoveryRequired: recovery.recoveryRequired, attempt }));
} else {
  await runner.start({ jobId, objective: 'Write effect once' });
  console.log(JSON.stringify({ status: (await runner.wait(jobId)).status }));
}
`;

function runChild(args) {
  return new Promise(resolve => execFile(process.execPath,
    ['--input-type=module', '-e', PROCESS_CHILD, new URL('../src/index.mjs', import.meta.url).href, ...args],
    { timeout: 15000, windowsHide: true },
    (error, stdout) => resolve({ code: error ? error.code ?? 1 : 0, signal: error?.signal ?? null, stdout })));
}

test('fresh Node.js processes recover durable state without replaying completed or interrupted jobs', { timeout: 60000 }, async t => {
  const directory = await temporaryDirectory(t);
  const effects = path.join(await temporaryDirectory(t), 'effects.log');

  const completed = await runChild([directory, effects, 'complete', 'process_done_job']);
  assert.equal(completed.code, 0);
  assert.deepEqual(JSON.parse(completed.stdout), { status: 'COMPLETED' });

  const crashed = await runChild([directory, effects, 'crash', 'process_crash_job']);
  assert.ok(crashed.code !== 0 || crashed.signal !== null, 'crash child must terminate abnormally');
  assert.equal(crashed.stdout, '');

  for (const jobId of ['process_done_job', 'process_crash_job']) {
    const recovered = await runChild([directory, effects, 'recover', jobId]);
    assert.equal(recovered.code, 0);
    assert.deepEqual(JSON.parse(recovered.stdout), {
      jobs: [['process_done_job', 'COMPLETED'], ['process_crash_job', 'UNKNOWN']],
      recoveryRequired: true,
      attempt: 'JOB_RECONCILIATION_REQUIRED'
    });
  }
  // Each external effect happened exactly once across all four processes.
  assert.deepEqual((await readFile(effects, 'utf8')).trim().split('\n'), ['process_done_job', 'process_crash_job']);
  assert.deepEqual(await savedStatuses(directory), { process_done_job: 'COMPLETED', process_crash_job: null });
});
