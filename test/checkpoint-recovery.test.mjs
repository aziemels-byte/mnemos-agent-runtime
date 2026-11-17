import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, writeFile, readdir } from 'node:fs/promises';
import { createHash, randomUUID } from 'node:crypto';
import path from 'node:path';
import { FileCheckpointStore, GoalRecoveryCoordinator } from '../src/index.mjs';
import { deferred, temporaryDirectory } from './helpers.mjs';

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
  await assert.rejects(store.save({ text: 'x'.repeat(1024 * 1024) }), RangeError);
  assert.deepEqual(await store.load(), { valid: true });
});

test('oversized checkpoint reads fail closed', async t => {
  const store = new FileCheckpointStore(await temporaryDirectory(t));
  await writeFile(store.file, 'x'.repeat(1024 * 1024 + 1));
  assert.equal((await new GoalRecoveryCoordinator({ store }).recover()).recoveryError, 'CHECKPOINT_UNREADABLE');
});

const sha256 = text => createHash('sha256').update(text).digest('hex');

test('conditional saves replace only the revision the caller last read', async t => {
  const store = new FileCheckpointStore(await temporaryDirectory(t));
  assert.equal(await store.read(), null);
  const created = await store.save({ step: 1 }, { expectedRevision: null }); // create-only
  assert.equal(created.revision, sha256(await readFile(store.file, 'utf8')));
  assert.deepEqual(await store.read(), { value: { step: 1 }, revision: created.revision });
  await assert.rejects(store.save({ step: 'again' }, { expectedRevision: null }),
    error => error.code === 'CHECKPOINT_CONFLICT' && error.currentRevision === created.revision);

  const second = await store.save({ step: 2 }, { expectedRevision: created.revision });
  const stale = await store.save({ step: 'stale' }, { expectedRevision: created.revision }).catch(error => error);
  assert.equal(stale.code, 'CHECKPOINT_CONFLICT');
  assert.deepEqual([stale.expectedRevision, stale.currentRevision], [created.revision, second.revision]);
  assert.deepEqual(await store.load(), { step: 2 });

  // Unconditional saves keep their previous behaviour.
  assert.match((await store.save({ step: 3 })).revision, /^[0-9a-f]{64}$/);
  for (const invalid of ['abc', 5, {}]) await assert.rejects(store.save({}, { expectedRevision: invalid }), TypeError);
  assert.deepEqual(await readdir(store.directory), ['checkpoint.json']);
});

test('checkpoint revisions distinguish stored bytes even if they decode to identical text', async t => {
  const store = new FileCheckpointStore(await temporaryDirectory(t));
  const firstBytes = Buffer.from([0x22, 0x80, 0x22]);
  const secondBytes = Buffer.from([0x22, 0x81, 0x22]);
  await writeFile(store.file, firstBytes);
  const first = await store.read();
  assert.equal(first.revision, sha256(firstBytes));
  await writeFile(store.file, secondBytes);
  const second = await store.read();
  assert.equal(second.value, first.value);
  assert.equal(second.revision, sha256(secondBytes));
  await assert.rejects(store.save({ stale: true }, { expectedRevision: first.revision }),
    error => error.code === 'CHECKPOINT_CONFLICT' && error.currentRevision === second.revision);
});

test('a stale coordinator cannot overwrite the checkpoint of the one that took over', async t => {
  const directory = await temporaryDirectory(t);
  const original = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) });
  const saved = await original.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [{ id: 'attempt_1' }] });
  const seenByOld = (await original.recover()).checkpointRevision;
  assert.equal(seenByOld, saved.checkpointRevision);

  const successor = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) });
  const recovered = await successor.recover();
  const next = await successor.checkpoint({ goal: recovered.goal, activeJobs: [{ id: 'attempt_2' }],
    expectedRevision: recovered.checkpointRevision });

  await assert.rejects(original.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [], expectedRevision: seenByOld }),
    error => error.code === 'CHECKPOINT_CONFLICT' && error.currentRevision === next.checkpointRevision);
  const final = await successor.recover();
  assert.deepEqual(final.activeJobs, [{ id: 'attempt_2' }]);
  assert.equal(final.checkpointRevision, next.checkpointRevision);

  // An invalid checkpoint still reports its revision so it can be replaced conditionally.
  await writeFile(path.join(directory, 'checkpoint.json'), '{"version":2}');
  const invalid = await successor.recover();
  assert.equal(invalid.recoveryError, 'CHECKPOINT_INVALID');
  assert.equal(invalid.checkpointRevision, sha256('{"version":2}'));
  await successor.checkpoint({ goal: { goalId: 'goal' }, expectedRevision: invalid.checkpointRevision });
});

test('custom stores keep working, but cannot silently ignore a revision condition', async () => {
  let stored = null;
  const plain = { save: async value => { stored = structuredClone(value); }, load: async () => structuredClone(stored) };
  const coordinator = new GoalRecoveryCoordinator({ store: plain });
  const saved = await coordinator.checkpoint({ goal: { goalId: 'goal' } });
  assert.equal('checkpointRevision' in saved, false);
  assert.equal('checkpointRevision' in await coordinator.recover(), false);
  await assert.rejects(coordinator.checkpoint({ goal: { goalId: 'goal' }, expectedRevision: 'f'.repeat(64) }),
    /does not support conditional saves/);
});

test('checkpoints written by earlier versions are read and replaced conditionally', async t => {
  const directory = await temporaryDirectory(t);
  const legacy = JSON.stringify({ version: 1, goal: { goalId: 'old' }, dag: null, activeJobs: [], checkpointedAt: '2026-01-01T00:00:00.000Z' }, null, 2) + '\n';
  await writeFile(path.join(directory, 'checkpoint.json'), legacy);
  await writeFile(path.join(directory, `${'a'.repeat(32)}.tmp`), '{"crashed');
  await writeFile(path.join(directory, `${randomUUID()}.checkpoint.tmp`), '{"crashed');
  const unrelated = 'notes.checkpoint.tmp';
  await writeFile(path.join(directory, unrelated), 'keep me');
  const store = new FileCheckpointStore(directory);
  const { revision } = await store.read();
  assert.equal(revision, sha256(legacy));
  assert.equal((await new GoalRecoveryCoordinator({ store }).recover()).goal.goalId, 'old');
  await store.save({ replaced: true }, { expectedRevision: revision });
  assert.deepEqual((await readdir(directory)).sort(), ['checkpoint.json', unrelated]);
  assert.equal(await readFile(path.join(directory, unrelated), 'utf8'), 'keep me');
});

test('by default a coordinator only replaces the checkpoint it last saw', async t => {
  const directory = await temporaryDirectory(t);
  const stale = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) });
  await stale.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [{ id: 'attempt_1' }] });

  const successor = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) });
  const recovered = await successor.recover();
  await successor.checkpoint({ goal: recovered.goal, activeJobs: [{ id: 'attempt_2' }] }); // no revision passed

  // The stale coordinator passes nothing either, and is still refused.
  await assert.rejects(stale.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [] }), error => error.code === 'CHECKPOINT_CONFLICT');
  await assert.rejects(stale.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [] }), error => error.code === 'CHECKPOINT_CONFLICT');
  assert.deepEqual((await successor.recover()).activeJobs, [{ id: 'attempt_2' }]);

  // After recovering (and reconciling) it may save again.
  await stale.recover();
  await stale.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [{ id: 'attempt_3' }] });
  assert.deepEqual((await successor.recover()).activeJobs, [{ id: 'attempt_3' }]);
});

test('a coordinator that has seen no checkpoint may only create one', async t => {
  const directory = await temporaryDirectory(t);
  const first = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) });
  assert.equal(first.lastKnownRevision, undefined);
  assert.equal(await first.recover(), null);
  assert.equal(first.lastKnownRevision, null); // known to be absent
  const saved = await first.checkpoint({ goal: { goalId: 'goal' } });
  assert.equal(first.lastKnownRevision, saved.checkpointRevision);

  // A fresh coordinator must not silently replace a checkpoint it never read.
  const fresh = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) });
  await assert.rejects(fresh.checkpoint({ goal: { goalId: 'other' } }),
    error => error.code === 'CHECKPOINT_CONFLICT' && error.expectedRevision === null && error.currentRevision === saved.checkpointRevision);
  assert.equal((await fresh.recover()).goal.goalId, 'goal');
  await fresh.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [{ id: 'next' }] });
});

test('unconditional saves are an explicit opt-out, and cannot be combined with a revision', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileCheckpointStore(directory);
  await new GoalRecoveryCoordinator({ store }).checkpoint({ goal: { goalId: 'goal' } });
  // Repair path: the checkpoint is unreadable, so its revision cannot be learned by recovering.
  await writeFile(store.file, '{"corrupt');
  const repair = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) });
  assert.equal((await repair.recover()).recoveryError, 'CHECKPOINT_UNREADABLE');
  await assert.rejects(repair.checkpoint({ goal: { goalId: 'goal' } }), error => error.code === 'CHECKPOINT_CONFLICT');
  const replaced = await repair.checkpoint({ goal: { goalId: 'goal' }, unconditional: true });
  assert.equal((await repair.recover()).checkpointRevision, replaced.checkpointRevision);
  await assert.rejects(repair.checkpoint({ goal: { goalId: 'goal' }, unconditional: true, expectedRevision: null }), /either expectedRevision or unconditional/);
  await assert.rejects(repair.checkpoint({ goal: { goalId: 'goal' }, unconditional: 'yes' }), /must be a boolean/);
});

test('overlapping checkpoints from one coordinator are applied in order without false conflicts', async t => {
  const directory = await temporaryDirectory(t);
  const coordinator = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) });
  const saves = await Promise.all(Array.from({ length: 8 }, (_, index) =>
    coordinator.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [{ id: `job_${index}` }] })));
  assert.equal(new Set(saves.map(save => save.checkpointRevision)).size, 8);
  const final = await coordinator.recover();
  assert.deepEqual(final.activeJobs, [{ id: 'job_7' }]);
  assert.equal(final.checkpointRevision, saves.at(-1).checkpointRevision);
});

test('overlapping recoveries require the revision of the snapshot being saved', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileCheckpointStore(directory);
  const writer = new GoalRecoveryCoordinator({ store });
  const inspectionStarted = deferred();
  const finishInspection = deferred();
  const coordinator = new GoalRecoveryCoordinator({
    store: new FileCheckpointStore(directory),
    jobInspector: async () => {
      inspectionStarted.resolve();
      await finishInspection.promise;
      return { status: 'COMPLETED' };
    }
  });
  const old = await writer.checkpoint({ goal: { goalId: 'goal', state: 'old' }, activeJobs: [{ id: 'old_job' }] });
  const delayed = coordinator.recover();
  t.after(() => finishInspection.resolve());
  await inspectionStarted.promise;
  const current = await writer.checkpoint({ goal: { goalId: 'goal', state: 'current' } });
  const newest = await coordinator.recover();
  assert.equal(newest.checkpointRevision, current.checkpointRevision);
  await assert.rejects(coordinator.checkpoint({ goal: newest.goal }), { code: 'CHECKPOINT_REVISION_REQUIRED' });
  const explicit = await coordinator.checkpoint({ goal: newest.goal, expectedRevision: newest.checkpointRevision });

  finishInspection.resolve();
  const earlier = await delayed;
  assert.equal(earlier.checkpointRevision, old.checkpointRevision);
  assert.equal(coordinator.lastKnownRevision, explicit.checkpointRevision);
  const stale = { goal: earlier.goal, activeJobs: earlier.activeJobs };
  await assert.rejects(coordinator.checkpoint(stale), { code: 'CHECKPOINT_REVISION_REQUIRED' });
  await assert.rejects(coordinator.checkpoint({ ...stale, expectedRevision: earlier.checkpointRevision }), { code: 'CHECKPOINT_CONFLICT' });
  assert.equal((await store.load()).goal.state, 'current');

  const recovered = await coordinator.recover();
  await coordinator.checkpoint({ goal: { ...recovered.goal, state: 'next' } });
  assert.equal((await store.load()).goal.state, 'next');
});

test('a hung inspector times out as UNKNOWN and does not block later default saves', async t => {
  const directory = await temporaryDirectory(t);
  await new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) })
    .checkpoint({ goal: { goalId: 'goal' }, activeJobs: [{ id: 'hung' }, { id: 'done' }] });
  const signals = [];
  const coordinator = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory), inspectionTimeoutMs: 30,
    jobInspector: (job, { signal }) => {
      signals.push(signal);
      return job.id === 'done' ? { status: 'COMPLETED' } : new Promise(() => {});
    } });
  const recovered = await coordinator.recover();
  assert.deepEqual(recovered.jobs.map(job => [job.id, job.recoveryStatus, job.recoveryReason]),
    [['hung', 'UNKNOWN', 'INSPECTION_TIMEOUT'], ['done', 'COMPLETED', undefined]]);
  assert.equal(recovered.recoveryRequired, true);
  assert.deepEqual(signals.map(signal => signal.aborted), [true, false]);
  assert.equal(signals[0].reason.code, 'INSPECTION_TIMEOUT');
  // The recovery settled, so the coordinator's default conditional save works again.
  await coordinator.checkpoint({ goal: recovered.goal, activeJobs: [] });
  assert.deepEqual((await coordinator.recover()).activeJobs, []);
});

test('the recovery budget bounds all inspections; uninspected jobs are UNKNOWN', async t => {
  const store = new FileCheckpointStore(await temporaryDirectory(t));
  const ids = Array.from({ length: 6 }, (_, index) => `job_${index}`);
  await new GoalRecoveryCoordinator({ store }).checkpoint({ goal: { goalId: 'goal' }, activeJobs: ids.map(id => ({ id })) });
  let calls = 0;
  const coordinator = new GoalRecoveryCoordinator({ store, inspectionTimeoutMs: 10_000, recoveryTimeoutMs: 40,
    jobInspector: () => { calls++; return new Promise(() => {}); } });
  const started = performance.now();
  const { jobs } = await coordinator.recover();
  assert.ok(performance.now() - started < 5_000, 'recovery must not wait for the per-job timeout');
  assert.ok(jobs.every(job => job.recoveryStatus === 'UNKNOWN'));
  assert.equal(jobs[0].recoveryReason, 'INSPECTION_TIMEOUT');
  assert.equal(jobs.at(-1).recoveryReason, 'RECOVERY_TIMEOUT');
  assert.ok(calls < ids.length, 'jobs past the budget are not inspected');
  assert.equal(jobs.filter(job => job.recoveryReason === 'RECOVERY_TIMEOUT').length, ids.length - calls);
});

test('inspection failures are UNKNOWN with a reason; inspector answers carry none', async t => {
  const store = new FileCheckpointStore(await temporaryDirectory(t));
  const unhandled = [];
  const onUnhandled = reason => unhandled.push(reason);
  process.on('unhandledRejection', onUnhandled);
  t.after(() => process.off('unhandledRejection', onUnhandled));
  const answers = {
    throws: () => { throw new Error('offline'); },
    rejects: async () => { throw new Error('offline'); },
    getter: async () => ({ get status() { throw new Error('hostile'); } }),
    odd: async () => ({ status: 'probably_done' }),
    unknown: async () => ({ status: 'UNKNOWN' }),
    stale: async () => ({ status: 'FAILED' })
  };
  await new GoalRecoveryCoordinator({ store }).checkpoint({ goal: { goalId: 'goal' },
    activeJobs: Object.keys(answers).map(id => id === 'stale' ? { id, recoveryReason: 'INSPECTION_TIMEOUT' } : { id }) });
  const { jobs } = await new GoalRecoveryCoordinator({ store, jobInspector: job => answers[job.id]() }).recover();
  assert.deepEqual(jobs.map(job => [job.id, job.recoveryStatus, job.recoveryReason]), [
    ['throws', 'UNKNOWN', 'INSPECTION_FAILED'], ['rejects', 'UNKNOWN', 'INSPECTION_FAILED'],
    ['getter', 'UNKNOWN', 'INSPECTION_FAILED'], ['odd', 'UNKNOWN', 'INSPECTION_UNSUPPORTED_STATUS'],
    ['unknown', 'UNKNOWN', undefined], ['stale', 'FAILED', undefined]]);
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(unhandled, []);
});

test('inspection time limits are validated', () => {
  const store = { save() {}, load() {} };
  for (const invalid of [0, -1, 1.5, '10', Infinity, 300_001]) {
    assert.throws(() => new GoalRecoveryCoordinator({ store, inspectionTimeoutMs: invalid }), /inspectionTimeoutMs/);
  }
  for (const invalid of [0, 2.5, 3_600_001]) {
    assert.throws(() => new GoalRecoveryCoordinator({ store, recoveryTimeoutMs: invalid }), /recoveryTimeoutMs/);
  }
  assert.equal(new GoalRecoveryCoordinator({ store }).inspectionTimeoutMs, 10_000);
});

test('every job ID a checkpoint accepts is a valid registry job ID', async t => {
  const { PhaseJobRegistry } = await import('../src/index.mjs');
  const registry = new PhaseJobRegistry({ execute: async () => ({ status: 'COMPLETED' }) });
  const store = new FileCheckpointStore(await temporaryDirectory(t));
  const coordinator = new GoalRecoveryCoordinator({ store });
  for (const id of ['a', 'A_1', 'x'.repeat(128)]) {
    await coordinator.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [{ id }] });
    assert.equal(registry.get(id), null); // valid ID, no such job
  }
  for (const id of ['', '_a', 'x'.repeat(129)]) {
    await assert.rejects(coordinator.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [{ id }] }), /Invalid checkpoint/);
    assert.throws(() => registry.get(id), /Invalid job ID/);
  }
});

test('save() reports invalid arguments through its promise, like every other failure', async t => {
  const store = new FileCheckpointStore(await temporaryDirectory(t));
  await store.save({ valid: true });
  let saving;
  assert.doesNotThrow(() => { saving = store.save({ text: 'x'.repeat(1024 * 1024) }); });
  await assert.rejects(saving, RangeError);
  assert.doesNotThrow(() => { saving = store.save({}, { expectedRevision: 'not-a-revision' }); });
  await assert.rejects(saving, TypeError);
  for (const value of [undefined, () => {}]) await assert.rejects(store.save(value), TypeError); // would store the text "undefined"
  assert.deepEqual(await store.load(), { valid: true });
});
