import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { FileCheckpointStore, FilePhaseJobRegistry, GoalRecoveryCoordinator } from '../src/index.mjs';

const executeFile = promisify(execFile);
const windows = process.platform === 'win32';

// The job's command: it starts a grandchild and records both process IDs, then runs until stopped.
const treeCommand = pidsFile => `
  const child = require('node:child_process').spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore', windowsHide: true });
  child.once('spawn', () => require('node:fs').writeFileSync(${JSON.stringify(pidsFile)}, JSON.stringify([process.pid, child.pid])));
  setInterval(() => {}, 1000);`;

const running = pid => { try { process.kill(pid, 0); return true; } catch (error) { return error.code !== 'ESRCH'; } };

async function recordedPids(pidsFile) {
  for (const deadline = Date.now() + 15_000; Date.now() < deadline; await sleep(20)) {
    try { return JSON.parse(await readFile(pidsFile, 'utf8')); } catch { /* not written yet */ }
  }
  throw new Error('The example command did not start');
}

async function recoverInNewProcess(directory) {
  // A fresh process: everything it knows comes from the checkpoint and the job records.
  const jobs = new FilePhaseJobRegistry({ directory, execute: async () => { throw new Error('recovery never runs a job'); } });
  const coordinator = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory), jobInspector: jobs.jobInspector() });
  const recovery = await coordinator.recover();
  const [attempt] = recovery.jobs;
  assert.equal(recovery.recoveryRequired, true);

  // The example's resume policy. A confirmed cancellation may be retried under a new job ID.
  // An uncertain one (Windows cannot verify a process tree) is reconciled first, with
  // evidence this process checks itself: the recorded process IDs no longer exist.
  let reconciled = false;
  if (attempt.recoveryStatus === 'UNKNOWN') {
    const pids = JSON.parse(await readFile(path.join(directory, 'pids.json'), 'utf8'));
    assert.ok(pids.every(pid => !running(pid)), 'the recorded processes still run: nothing may be retried');
    const job = await jobs.get(attempt.id);
    await jobs.reconcile({ jobId: attempt.id, expectedSequence: job.sequence, disposition: 'CANCELLED', explicitAuthorization: true,
      evidence: { authoritative: true, source: 'example: the recorded process IDs no longer exist' } });
    reconciled = true;
  } else {
    assert.equal(attempt.recoveryStatus, 'CANCELLED');
  }

  // The retry: a new job ID. Exclusive creation prevents a repeated example run from duplicating the effect.
  const report = path.join(directory, 'report.txt');
  const retry = new FilePhaseJobRegistry({ directory, execute: async () => {
    await writeFile(report, 'Recovered example task completed.\n', { flag: 'wx' });
    return { status: 'COMPLETED' };
  } });
  await retry.start({ jobId: 'example_attempt_2', objective: 'Create an example report' });
  assert.equal((await retry.wait('example_attempt_2')).status, 'COMPLETED');
  await coordinator.checkpoint({ goal: recovery.goal, dag: recovery.dag, activeJobs: [{ id: 'example_attempt_2' }] });
  const final = await coordinator.recover();
  assert.equal(final.recoveryRequired, false);
  return { freshProcessRecovery: attempt.recoveryStatus, reconciled, explicitResume: true, reportCreated: true };
}

export async function runRecoveryDemo() {
  const parent = path.resolve(os.tmpdir());
  const directory = await mkdtemp(path.join(parent, 'mnemos-public-demo-'));
  const pidsFile = path.join(directory, 'pids.json');
  let pids = [];
  try {
    // The job runs its command as a supervised process tree, recorded in the job before it starts.
    const jobs = new FilePhaseJobRegistry({ directory, execute: ({ setPhase, runProcess }) => {
      setPhase('execution');
      return runProcess({ command: process.execPath, args: ['-e', treeCommand(pidsFile)], graceMs: 1000 });
    } });
    await jobs.start({ jobId: 'example_attempt_1', objective: 'Create an example report' });
    pids = await recordedPids(pidsFile);

    const cancellation = await jobs.requestCancellation('example_attempt_1', { timeoutMs: 20_000 });
    // Verified here independently of the receipt: neither the command nor its grandchild runs.
    for (const deadline = Date.now() + 10_000; pids.some(running) && Date.now() < deadline;) await sleep(20);
    const treeStopped = !pids.some(running);
    assert.ok(treeStopped, 'the process tree was stopped');
    assert.equal(cancellation.status, windows ? 'UNKNOWN' : 'CANCELLED');
    assert.equal(cancellation.cancellationConfirmed, !windows);
    assert.equal(cancellation.processes.length, 1);

    const coordinator = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) });
    await coordinator.checkpoint({ goal: { goalId: 'example_report' }, dag: { nodes: ['report'] }, activeJobs: [{ id: 'example_attempt_1' }] });
    const { stdout } = await executeFile(process.execPath, [fileURLToPath(import.meta.url), '--recover', directory], { timeout: 20_000, windowsHide: true });
    return { ownedChildStopped: treeStopped, processTreeStopped: treeStopped, cancellationConfirmed: cancellation.cancellationConfirmed,
      checkpointPersisted: true, ...JSON.parse(stdout) };
  } finally {
    for (const pid of pids) { try { process.kill(pid, 'SIGKILL'); } catch { /* already gone */ } }
    if (path.dirname(path.resolve(directory)) !== parent) throw new Error('Unsafe cleanup target');
    await rm(directory, { recursive: true, force: true });
  }
}

if (process.argv[2] === '--recover') {
  console.log(JSON.stringify(await recoverInNewProcess(process.argv[3])));
}
