import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { hostname } from 'node:os';
import path from 'node:path';
import { FileCheckpointStore, FilePhaseJobRegistry, GoalRecoveryCoordinator } from '../src/index.mjs';
import { FileJobStore } from '../src/job-store.mjs';
import { deferred, temporaryDirectory } from './helpers.mjs';

const noExecute = async () => { throw new Error('recovery must never run a job'); };
const evidence = { authoritative: true, source: 'deploy-api:release/1' };
const summary = jobs => Object.fromEntries(jobs.map(job =>
  [job.id, [job.recoveryStatus, job.recoveryReason, job.recoveryEffectAbsent, job.checkpointStatusStale]]));

function runChild(source, args) {
  const moduleUrl = new URL('../src/index.mjs', import.meta.url).href;
  const prelude = `
    import { FileCheckpointStore, FilePhaseJobRegistry, GoalRecoveryCoordinator } from ${JSON.stringify(moduleUrl)};
    const [directory] = process.argv.slice(1);
    const emit = value => process.stdout.write(JSON.stringify(value) + '\\n');
  `;
  const child = spawn(process.execPath, ['--input-type=module', '-e', prelude + source, '--', ...args], { stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '';
  let stderr = '';
  const line = deferred();
  child.stdout.on('data', chunk => { stdout += chunk; if (stdout.includes('\n')) line.resolve(JSON.parse(stdout.split('\n')[0])); });
  child.stderr.on('data', chunk => { stderr += chunk; });
  child.exited = new Promise(resolve => child.on('close', code => {
    line.resolve({ exited: code, stderr });
    resolve(code);
  }));
  child.line = line.promise;
  return child;
}

test('after a crash, a goal is recovered from its checkpoint and the job records, without running anything', async t => {
  const directory = await temporaryDirectory(t);
  // The first process runs part of a release, checkpoints it and is killed while deploying.
  const child = runChild(`
    const jobs = new FilePhaseJobRegistry({ directory, execute: async ({ jobId, setPhase }) => {
      if (jobId === 'build') return { status: 'COMPLETED', artifact: 'build/42' };
      if (jobId === 'upload') return { status: 'FAILED', effectAbsent: true }; // rejected before anything was stored
      setPhase('deploying');
      return new Promise(() => setInterval(() => {}, 1000));
    } });
    for (const jobId of ['build', 'upload']) { await jobs.start({ jobId, objective: jobId }); await jobs.wait(jobId); }
    await jobs.start({ jobId: 'deploy', objective: 'deploy' });
    while ((await jobs.get('deploy')).phase !== 'deploying') await new Promise(resolve => setTimeout(resolve, 5));
    const coordinator = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) });
    await coordinator.checkpoint({ goal: { goalId: 'release_1' }, dag: { order: ['build', 'upload', 'deploy', 'notify'] },
      activeJobs: [{ id: 'build', status: 'COMPLETED' }, { id: 'upload' }, { id: 'deploy', status: 'RUNNING' }, { id: 'notify' }] });
    emit({ checkpointed: true });`, [directory]);
  t.after(() => child.kill('SIGKILL'));
  assert.deepEqual(await child.line, { checkpointed: true });
  child.kill('SIGKILL');
  await child.exited;

  // A fresh process recovers.
  const jobs = new FilePhaseJobRegistry({ directory, execute: noExecute });
  const coordinator = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory), jobInspector: jobs.jobInspector() });
  const recovered = await coordinator.recover();
  assert.equal(recovered.goal.goalId, 'release_1');
  assert.equal(recovered.recoveryRequired, true);
  assert.deepEqual(summary(recovered.jobs), {
    build: ['COMPLETED', undefined, false, false],
    upload: ['FAILED', 'EXECUTOR_REPORTED_FAILURE', true, false],     // safe to retry under a new ID
    deploy: ['UNKNOWN', 'OWNER_DIED_AFTER_DISPATCH', false, true],    // the checkpoint's RUNNING is stale
    notify: ['UNKNOWN', 'JOB_RECORD_MISSING', false, false]           // checkpointed, never recorded
  });
  assert.equal((await new FileJobStore({ directory }).read('deploy')).status, 'UNKNOWN', 'the reclassification is recorded');

  // The resume policy acts explicitly: reconcile the ambiguous deploy, retry the upload under a new ID,
  // start the notification (no record exists, and the operator confirms it never ran).
  const deploy = await jobs.get('deploy');
  await jobs.reconcile({ jobId: 'deploy', expectedSequence: deploy.sequence, disposition: 'COMPLETED', evidence, explicitAuthorization: true });
  const worker = new FilePhaseJobRegistry({ directory, execute: async () => ({ status: 'COMPLETED' }) });
  for (const jobId of ['upload_2', 'notify']) { await worker.start({ jobId, objective: jobId }); await worker.wait(jobId); }
  await coordinator.checkpoint({ goal: recovered.goal, dag: recovered.dag,
    activeJobs: ['build', 'upload_2', 'deploy', 'notify'].map(id => ({ id })) });
  const final = await coordinator.recover();
  assert.deepEqual(summary(final.jobs).deploy, ['COMPLETED', 'RECONCILED', false, false]);
  assert.ok(final.jobs.every(job => job.recoveryStatus === 'COMPLETED'));
  assert.equal(final.recoveryRequired, false);
});

test('the job inspector reports unreadable records, live jobs and unverifiable owners without running anything', async t => {
  const directory = await temporaryDirectory(t);
  const release = deferred();
  const entered = deferred();
  const owner = new FilePhaseJobRegistry({ directory, execute: async () => { entered.resolve(); await release.promise; return { status: 'COMPLETED' }; } });
  await owner.start({ jobId: 'live', objective: 'Still running' });
  await entered.promise;
  await writeFile(path.join(directory, `${createHash('sha256').update('torn').digest('hex')}.job.json`), '{"torn');
  const store = new FileJobStore({ directory });
  await store.create({ jobId: 'remote', objective: 'Elsewhere', fingerprint: 'a'.repeat(64),
    owner: { pid: 4242, host: 'other-host.invalid', executionId: 'remote' } });

  const coordinator = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory),
    jobInspector: new FilePhaseJobRegistry({ directory, execute: noExecute }).jobInspector() });
  await coordinator.checkpoint({ goal: { goalId: 'goal' }, activeJobs: ['live', 'torn', 'remote'].map(id => ({ id })) });
  const recovered = await coordinator.recover();
  assert.deepEqual(summary(recovered.jobs), {
    live: ['RUNNING', undefined, false, false],
    torn: ['UNKNOWN', 'JOB_STATE_INVALID', false, false],
    remote: ['RUNNING', undefined, false, false]
  });
  assert.equal(recovered.recoveryRequired, true);
  release.resolve();
  await owner.wait('live');
});

test('job status in a checkpoint is only a hint: invalid values are refused on save and never trusted on recovery', async t => {
  const directory = await temporaryDirectory(t);
  const checkpoints = new FileCheckpointStore(directory);
  const coordinator = new GoalRecoveryCoordinator({ store: checkpoints, jobInspector: async () => ({ status: 'UNKNOWN' }) });
  for (const job of [{ id: 'a', status: 'DONE???' }, { id: 'a', status: 7 }, { id: 'a', phase: 'bad phase' }, { id: 'a', phase: 3 }]) {
    await assert.rejects(coordinator.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [job] }), /status or phase/);
  }
  await coordinator.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [{ id: 'a', status: 'COMPLETED', phase: 'report' }] });

  // A checkpoint written before these checks, with a value no job can have, still loads.
  await checkpoints.save({ version: 1, goal: { goalId: 'goal' }, dag: null, checkpointedAt: new Date().toISOString(),
    activeJobs: [{ id: 'a', status: 'DONE???' }, { id: 'b', status: 'COMPLETED' }, { id: 'c' }] });
  const recovered = await new GoalRecoveryCoordinator({ store: checkpoints, jobInspector: async job =>
    ({ status: job.id === 'b' ? 'UNKNOWN' : 'COMPLETED' }) }).recover();
  assert.deepEqual(summary(recovered.jobs), {
    a: ['COMPLETED', undefined, false, true],
    b: ['UNKNOWN', undefined, false, true],
    c: ['COMPLETED', undefined, false, false]
  });
  assert.equal(recovered.recoveryRequired, true, 'decided by the inspection, not the stored status');
});

test('the coordinator passes on only well-formed reasons and effect-free reports for jobs that did not complete', async t => {
  const store = new FileCheckpointStore(await temporaryDirectory(t));
  const answers = {
    failed_clean: { status: 'FAILED', effectAbsent: true, reason: 'EXECUTOR_REPORTED_FAILURE' },
    cancelled_clean: { status: 'CANCELLED', effectAbsent: true },
    completed_claim: { status: 'COMPLETED', effectAbsent: true },
    unknown_claim: { status: 'UNKNOWN', effectAbsent: true, reason: 'OWNER_DIED_AFTER_DISPATCH' },
    truthy_claim: { status: 'FAILED', effectAbsent: 'yes' },
    lower_reason: { status: 'FAILED', reason: 'not a code' },
    long_reason: { status: 'FAILED', reason: 'A'.repeat(65) }
  };
  await new GoalRecoveryCoordinator({ store }).checkpoint({ goal: { goalId: 'goal' }, activeJobs: Object.keys(answers).map(id => ({ id })) });
  const { jobs } = await new GoalRecoveryCoordinator({ store, jobInspector: async job => answers[job.id] }).recover();
  assert.deepEqual(summary(jobs), {
    failed_clean: ['FAILED', 'EXECUTOR_REPORTED_FAILURE', true, false],
    cancelled_clean: ['CANCELLED', undefined, true, false],
    completed_claim: ['COMPLETED', undefined, false, false],
    unknown_claim: ['UNKNOWN', 'OWNER_DIED_AFTER_DISPATCH', false, false],
    truthy_claim: ['FAILED', undefined, false, false],
    lower_reason: ['FAILED', undefined, false, false],
    long_reason: ['FAILED', undefined, false, false]
  });
});

test('a dead owner found through the job inspector is reclassified once; recovering again changes nothing', async t => {
  const directory = await temporaryDirectory(t);
  const exited = spawn(process.execPath, ['-e', '']);
  await new Promise(resolve => exited.on('close', resolve));
  const store = new FileJobStore({ directory });
  const { record } = await store.create({ jobId: 'queued', objective: 'Never dispatched', fingerprint: 'b'.repeat(64),
    owner: { pid: exited.pid, host: hostname(), executionId: 'gone' } });
  const coordinator = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory),
    jobInspector: new FilePhaseJobRegistry({ directory, execute: noExecute }).jobInspector() });
  await coordinator.checkpoint({ goal: { goalId: 'goal' }, activeJobs: [{ id: 'queued' }] });
  const first = await coordinator.recover();
  assert.deepEqual(summary(first.jobs).queued, ['CANCELLED', 'OWNER_DIED_BEFORE_DISPATCH', true, false]);
  const second = await coordinator.recover();
  assert.deepEqual(second.jobs, first.jobs);
  assert.equal((await store.read('queued')).sequence, record.sequence + 1);
});
