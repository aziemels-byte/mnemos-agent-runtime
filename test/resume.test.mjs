import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { appendFile, readFile } from 'node:fs/promises';
import { hostname } from 'node:os';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { FileCheckpointStore, FileIdempotencyRegistry, FilePhaseJobRegistry, GoalRecoveryCoordinator } from '../src/index.mjs';
import { processIdentity } from '../src/process-identity.mjs';
import { FileJobStore } from '../src/job-store.mjs';
import { deferred, temporaryDirectory, until, withTimeout } from './helpers.mjs';

const noExecute = async () => { throw new Error('must not run'); };
const evidence = { authoritative: true, source: 'import-api:batch/9' };


/**
 * An import job: one effect per item, each keyed with effectKey() so that it
 * happens once however often the job is resumed, then saved progress. With
 * hangAfter, it stops after that item's effect, before saving its progress.
 * Its source also runs in an owner process (see ownerRun).
 */
function importExecutor(FileIdempotencyRegistry, appendFile, directory, hangAfter) {
  return async ({ input, saveProgress, effectKey, resume, setPhase }) => {
    const effects = new FileIdempotencyRegistry({ directory });
    setPhase('importing');
    for (let item = resume?.progress?.state.next ?? 0; item < input.items; item++) {
      await effects.execute({ key: effectKey(`item-${item}`), request: { item },
        operation: async () => { await appendFile(`${directory}/effects.log`, `item-${item}\n`); return item; } });
      if (item === hangAfter) { await appendFile(`${directory}/hung`, ''); await new Promise(() => setInterval(() => {}, 1000)); }
      await saveProgress({ next: item + 1 });
    }
    return { status: 'COMPLETED', imported: input.items, resumedFrom: resume?.progress?.state.next ?? 0 };
  };
}

// Run the import in an owner process that is killed once it hangs (a crash mid-job).
async function crashedImport(t, directory, { jobId = 'batch_import', items = 6, hangAfter = 3 } = {}) {
  const url = new URL('../src/index.mjs', import.meta.url).href;
  const owner = spawn(process.execPath, ['--input-type=module', '-e', `
    import { FilePhaseJobRegistry, FileIdempotencyRegistry } from ${JSON.stringify(url)};
    import { appendFile } from 'node:fs/promises';
    const importExecutor = ${importExecutor.toString()};
    const jobs = new FilePhaseJobRegistry({ directory: ${JSON.stringify(directory)},
      execute: importExecutor(FileIdempotencyRegistry, appendFile, ${JSON.stringify(directory)}, ${hangAfter}) });
    await jobs.start({ jobId: ${JSON.stringify(jobId)}, objective: 'Import a batch', input: { items: ${items} }, resumable: true });`],
  { stdio: 'ignore' });
  const exited = new Promise(resolve => owner.once('exit', resolve));
  t.after(() => owner.kill('SIGKILL'));
  await until(async () => { try { await readFile(path.join(directory, 'hung')); return true; } catch { return false; } }, 'the owner to hang');
  owner.kill('SIGKILL');
  await withTimeout(exited, 10_000, 'the owner did not exit');
}

const effectLog = async directory => (await readFile(path.join(directory, 'effects.log'), 'utf8')).trim().split('\n');

test('after a crash, a resumable job continues from its saved progress; each effect happens once', async t => {
  const directory = await temporaryDirectory(t);
  await crashedImport(t, directory);
  assert.deepEqual(await effectLog(directory), ['item-0', 'item-1', 'item-2', 'item-3'], 'item 3 took effect; its progress was not saved');

  const jobs = new FilePhaseJobRegistry({ directory, execute: importExecutor(FileIdempotencyRegistry, appendFile, directory, null) });
  const stopped = await jobs.get('batch_import');
  assert.deepEqual([stopped.status, stopped.reason, stopped.resumable, stopped.progress.state], ['UNKNOWN', 'OWNER_DIED_AFTER_DISPATCH', true, { next: 3 }]);
  assert.deepEqual((await jobs.recover()).resumable, ['batch_import']);

  const successor = await jobs.resume({ jobId: 'batch_import', expectedSequence: stopped.sequence, successorJobId: 'batch_import_2',
    input: { items: 6 }, explicitAuthorization: true });
  assert.deepEqual(successor.resumeOf, { jobId: 'batch_import', rootJobId: 'batch_import', attempt: 2 });
  const done = await jobs.wait('batch_import_2');
  assert.deepEqual([done.status, done.result.resumedFrom, done.progress.state], ['COMPLETED', 3, { next: 6 }]);
  // Item 3's effect was recorded before the crash: replayed, not repeated. Items 4 and 5 ran once.
  assert.deepEqual(await effectLog(directory), ['item-0', 'item-1', 'item-2', 'item-3', 'item-4', 'item-5']);

  const predecessor = await jobs.get('batch_import');
  assert.deepEqual([predecessor.status, predecessor.reason, predecessor.resumedBy, predecessor.cancellationConfirmed],
    ['CANCELLED', 'RESUMED', 'batch_import_2', true]);
  assert.deepEqual((await jobs.recover()).resumable, []);

  // A goal that lists the original job sees the latest attempt.
  const coordinator = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory), jobInspector: jobs.jobInspector() });
  await coordinator.checkpoint({ goal: { goalId: 'import_goal' }, activeJobs: [{ id: 'batch_import' }] });
  assert.equal((await coordinator.recover()).jobs[0].recoveryStatus, 'COMPLETED');
});

test('progress is saved durably, step by step, only by resumable jobs', async t => {
  const directory = await temporaryDirectory(t);
  const seen = {};
  const jobs = new FilePhaseJobRegistry({ directory, execute: async ({ jobId, saveProgress, setPhase }) => {
    if (jobId === 'plain') {
      seen.plain = await saveProgress({ n: 1 }).catch(error => error.message);
      return { status: 'COMPLETED' };
    }
    setPhase('download');
    seen.first = await saveProgress({ chunk: 1 });
    setPhase('verify');
    seen.second = await saveProgress({ b: 2, a: 1 });
    seen.invalid = await saveProgress({ handle: () => {} }).catch(error => error.message);
    seen.large = await saveProgress('x'.repeat(70 * 1024)).catch(error => error.message);
    return { status: 'COMPLETED' };
  } });
  await jobs.start({ jobId: 'steps', objective: 'Steps', resumable: true });
  await jobs.start({ jobId: 'plain', objective: 'Plain' });
  const done = await jobs.wait('steps');
  await jobs.wait('plain');
  assert.deepEqual([seen.first, seen.second], [1, 2]);
  assert.match(seen.invalid, /plain JSON/);
  assert.match(seen.large, /64 KiB/);
  assert.match(seen.plain, /resumable: true/);
  assert.deepEqual([done.progress.sequence, done.progress.phase, done.progress.state], [2, 'verify', { a: 1, b: 2 }]);
  assert.deepEqual((await new FilePhaseJobRegistry({ directory, execute: noExecute }).get('steps')).progress, done.progress);
  assert.equal((await jobs.get('plain')).progress, null);
  // resumable is part of the job's identity.
  await assert.rejects(jobs.start({ jobId: 'steps', objective: 'Steps' }), { code: 'JOB_IDENTITY_CONFLICT' });
});

test('resume is refused unless the job is resumable, stopped, verified and inspected', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const release = deferred();
  const busyRuns = deferred(); // its dispatch is recorded before the executor runs: its sequence is stable then
  const jobs = new FilePhaseJobRegistry({ directory, execute: async ({ jobId, saveProgress }) => {
    if (jobId === 'busy') { busyRuns.resolve(); await release.promise; return { status: 'COMPLETED' }; }
    if (jobId !== 'plain') await saveProgress({ step: 1 });
    return jobId === 'done' ? { status: 'COMPLETED' } : { status: 'FAILED' };
  } });
  for (const [jobId, resumable] of [['plain', false], ['done', true], ['failed', true], ['busy', true]]) {
    await jobs.start({ jobId, objective: jobId, input: { n: 1 }, resumable });
  }
  for (const jobId of ['plain', 'done', 'failed']) await jobs.wait(jobId);
  await busyRuns.promise;
  const other = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const resume = async (jobId, extra = {}) => other.resume({ jobId, expectedSequence: (await store.read(jobId)).sequence,
    successorJobId: `${jobId}_2`, input: { n: 1 }, explicitAuthorization: true, ...extra });

  await assert.rejects(resume('plain'), { code: 'JOB_NOT_RESUMABLE' });
  await assert.rejects(resume('done'), { code: 'JOB_NOT_RESUMABLE' });
  await assert.rejects(resume('busy'), { code: 'JOB_STOP_UNVERIFIED' });
  await assert.rejects(resume('failed', { explicitAuthorization: false }), { code: 'JOB_AUTHORIZATION_REQUIRED' });
  await assert.rejects(resume('failed', { expectedSequence: 1 }), { code: 'JOB_RECORD_CHANGED' });
  await assert.rejects(resume('failed', { input: { n: 2 } }), { code: 'JOB_IDENTITY_CONFLICT' });
  await assert.rejects(resume('failed', { successorJobId: 'failed' }), /new job ID/);
  await assert.rejects(resume('failed', { successorJobId: 'done' }), { code: 'JOB_IDENTITY_CONFLICT' }); // taken
  await assert.rejects(resume('missing'), error => error instanceof TypeError || error.code === 'JOB_NOT_FOUND');
  await assert.rejects(jobs.resume({ jobId: 'busy', expectedSequence: 1, successorJobId: 'busy_2', explicitAuthorization: true }),
    { code: 'JOB_RUNNING_HERE' });
  assert.equal((await store.read('failed')).resumedBy, undefined, 'nothing was written');
  release.resolve();
  await jobs.wait('busy');
});

test('a job whose recorded processes may still run cannot be resumed', { skip: process.platform === 'win32' }, async t => {
  const directory = await temporaryDirectory(t);
  const running = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });
  t.after(() => running.kill('SIGKILL'));
  await new Promise(resolve => running.once('spawn', resolve));
  const store = new FileJobStore({ directory });
  const owner = { pid: process.pid, host: hostname(), executionId: 'finished' };
  const fingerprint = createHash('sha256').update('null').digest('hex'); // of the input null
  let { record } = await store.create({ jobId: 'leaky', objective: 'Leaky', fingerprint, owner, resumable: true });
  record = await store.update('leaky', { expectedSequence: record.sequence, changes: { dispatched: true, dispatchedAt: new Date().toISOString(),
    processes: [{ marker: `MNEMOS_TREE_${'b'.repeat(32)}`, supervisor: { pid: running.pid, host: hostname() },
      leader: processIdentity(running.pid), recordedAt: new Date().toISOString() }] } });
  record = await store.update('leaky', { expectedSequence: record.sequence, changes: { status: 'FAILED', reason: 'EXECUTOR_THREW' } });
  const jobs = new FilePhaseJobRegistry({ directory, execute: noExecute });
  await assert.rejects(jobs.resume({ jobId: 'leaky', expectedSequence: record.sequence, successorJobId: 'leaky_2',
    input: null, explicitAuthorization: true }), error => error.code === 'JOB_STOP_UNVERIFIED' && error.processesState !== 'NONE');
  assert.deepEqual((await jobs.recover()).resumable, []);
});

// A resumable, dispatched job owned by a live process elsewhere, which an operator
// settled (CANCELLED) with overrideRunningOwner: its owner may still run it.
async function contestedJob(t, directory) {
  const owner = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });
  t.after(() => owner.kill('SIGKILL'));
  await new Promise(resolve => owner.once('spawn', resolve));
  const store = new FileJobStore({ directory });
  const fingerprint = createHash('sha256').update('null').digest('hex');
  let { record } = await store.create({ jobId: 'contested', objective: 'Contested', fingerprint, resumable: true,
    owner: { ...processIdentity(owner.pid), executionId: 'elsewhere' } });
  record = await store.update('contested', { expectedSequence: record.sequence, changes: { dispatched: true, dispatchedAt: new Date().toISOString() } });
  const jobs = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const settled = await jobs.reconcile({ jobId: 'contested', expectedSequence: record.sequence, disposition: 'CANCELLED', evidence,
    explicitAuthorization: true, overrideRunningOwner: true });
  assert.equal(settled.reconciliation.override.ownerState, 'ALIVE');
  return { owner, jobs, settled };
}

test('a job settled over a live owner cannot be resumed while that owner may still run it', { skip: process.platform === 'win32' }, async t => {
  const directory = await temporaryDirectory(t);
  const { owner, jobs, settled } = await contestedJob(t, directory);
  await assert.rejects(jobs.resume({ jobId: 'contested', expectedSequence: settled.sequence, successorJobId: 'contested_2',
    explicitAuthorization: true }), error => error.code === 'JOB_STOP_UNVERIFIED' && error.ownerState === 'ALIVE');
  owner.kill('SIGKILL');
  await until(() => processIdentity(owner.pid) === null, 'the owner to exit');
  await jobs.resume({ jobId: 'contested', expectedSequence: settled.sequence, successorJobId: 'contested_2', explicitAuthorization: true });
  await jobs.wait('contested_2'); // let the attempt settle before the directory is removed
});

test('CANCELLED and FAILED jobs keep their outcome when resumed and gain only the link', async t => {
  const directory = await temporaryDirectory(t);
  const entered = deferred();
  const jobs = new FilePhaseJobRegistry({ directory, execute: async ({ jobId, saveProgress, signal, resume }) => {
    if (resume) return { status: 'COMPLETED', from: resume.progress.state };
    await saveProgress({ at: jobId });
    if (jobId === 'cancelled') {
      entered.resolve();
      await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }));
      return { status: 'CANCELLED', termination: { confirmed: true } };
    }
    return { status: 'FAILED' };
  } });
  await jobs.start({ jobId: 'cancelled', objective: 'Cancel me', resumable: true });
  await entered.promise;
  const cancelled = await jobs.requestCancellation('cancelled');
  await jobs.start({ jobId: 'failed', objective: 'Fail', resumable: true });
  const failed = await jobs.wait('failed');
  for (const [before, successorJobId] of [[cancelled, 'cancelled_2'], [failed, 'failed_2']]) {
    await jobs.resume({ jobId: before.jobId, expectedSequence: before.sequence, successorJobId, explicitAuthorization: true });
    const done = await jobs.wait(successorJobId);
    assert.deepEqual([done.status, done.result.from], ['COMPLETED', { at: before.jobId }]);
    const after = await jobs.get(before.jobId);
    assert.deepEqual([after.status, after.reason, after.resumedBy], [before.status, before.reason, successorJobId]);
  }
});

test('concurrent resumes of one job: exactly one attempt runs', async t => {
  const directory = await temporaryDirectory(t);
  let runs = 0;
  const execute = async ({ resume }) => { if (resume) runs++; return { status: 'FAILED' }; };
  const jobs = new FilePhaseJobRegistry({ directory, execute });
  await jobs.start({ jobId: 'once', objective: 'Once', resumable: true });
  const failed = await jobs.wait('once');
  const attempts = ['once_a', 'once_b', 'once_c'].map(successorJobId =>
    new FilePhaseJobRegistry({ directory, execute }).resume({ jobId: 'once', expectedSequence: failed.sequence, successorJobId,
      explicitAuthorization: true }));
  const results = await Promise.allSettled(attempts);
  const won = results.filter(result => result.status === 'fulfilled');
  assert.equal(won.length, 1);
  assert.ok(results.filter(result => result.status === 'rejected')
    .every(result => ['JOB_RECORD_CHANGED', 'JOB_ALREADY_RESUMED'].includes(result.reason.code)));
  await until(async () => runs === 1 && (await jobs.get(won[0].value.jobId)).status !== 'RUNNING', 'the attempt to run');
  await sleep(100);
  assert.equal(runs, 1);
});

test('a resume interrupted between its two writes is completed by resuming again with the same successor', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const jobs = new FilePhaseJobRegistry({ directory, execute: async ({ resume }) => ({ status: resume ? 'COMPLETED' : 'FAILED' }) });
  await jobs.start({ jobId: 'half', objective: 'Half', resumable: true });
  const failed = await jobs.wait('half');
  // As if the process died after linking the job and before recording its successor.
  const linked = await store.update('half', { expectedSequence: failed.sequence, changes: { resumedBy: 'half_2' } });
  assert.deepEqual((await jobs.recover()).resumeIncomplete, ['half']);
  assert.equal((await jobs.jobInspector()({ id: 'half' })).reason, 'RESUME_INCOMPLETE');

  const request = { jobId: 'half', expectedSequence: linked.sequence, explicitAuthorization: true };
  await assert.rejects(jobs.resume({ ...request, successorJobId: 'half_3' }), { code: 'JOB_ALREADY_RESUMED' });
  await jobs.resume({ ...request, successorJobId: 'half_2' });
  assert.equal((await jobs.wait('half_2')).status, 'COMPLETED');
  assert.deepEqual((await jobs.recover()).resumeIncomplete, []);
  // Resuming again reports the existing attempt; it never runs twice.
  const again = await jobs.resume({ ...request, successorJobId: 'half_2' });
  assert.deepEqual([again.jobId, again.status], ['half_2', 'COMPLETED']);
});

test('attempts form a chain: the root stays, effect keys stay, the inspector follows to the latest', async t => {
  const directory = await temporaryDirectory(t);
  const keys = [];
  const jobs = new FilePhaseJobRegistry({ directory, execute: async ({ resume, effectKey, saveProgress }) => {
    keys.push(effectKey('upload'));
    await saveProgress({ attempt: resume?.attempt ?? 1 });
    return { status: (resume?.attempt ?? 1) < 3 ? 'FAILED' : 'COMPLETED' };
  } });
  await jobs.start({ jobId: 'upload', objective: 'Upload', resumable: true });
  let current = await jobs.wait('upload');
  for (const successorJobId of ['upload_2', 'upload_3']) {
    await jobs.resume({ jobId: current.jobId, expectedSequence: current.sequence, successorJobId, explicitAuthorization: true });
    current = await jobs.wait(successorJobId);
  }
  assert.deepEqual(current.resumeOf, { jobId: 'upload_2', rootJobId: 'upload', attempt: 3 });
  assert.deepEqual(keys, ['upload:upload', 'upload:upload', 'upload:upload']);
  assert.deepEqual(await jobs.jobInspector()({ id: 'upload' }), { status: 'COMPLETED', reason: null, effectAbsent: false,
    reconciliationRequired: false });
});

test('recover() lists as resumable only jobs that resume() accepts', { skip: process.platform === 'win32' }, async t => {
  const directory = await temporaryDirectory(t);
  const { owner, jobs, settled } = await contestedJob(t, directory);
  await assert.rejects(jobs.resume({ jobId: 'contested', expectedSequence: settled.sequence, successorJobId: 'contested_2',
    explicitAuthorization: true }), error => error.code === 'JOB_STOP_UNVERIFIED');
  assert.deepEqual((await jobs.recover()).resumable, []);
  // Once the owner is gone, both agree again.
  owner.kill('SIGKILL');
  await until(() => processIdentity(owner.pid) === null, 'the owner to exit');
  assert.deepEqual((await jobs.recover()).resumable, ['contested']);
});
