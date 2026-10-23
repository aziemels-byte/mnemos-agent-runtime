import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, writeFile, readdir } from 'node:fs/promises';
import { FileCheckpointStore, GoalRecoveryCoordinator } from '../src/index.mjs';
import { temporaryDirectory } from './helpers.mjs';

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
