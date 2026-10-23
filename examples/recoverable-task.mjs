import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { promisify } from 'node:util';
import { execFile } from 'node:child_process';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { PhaseJobRegistry, FileCheckpointStore, GoalRecoveryCoordinator } from '../src/index.mjs';

const executeFile = promisify(execFile);

async function recoverInNewProcess(directory) {
  const receipt = JSON.parse(await readFile(path.join(directory, 'receipt.json'), 'utf8'));
  const coordinator = new GoalRecoveryCoordinator({
    store: new FileCheckpointStore(directory),
    jobInspector: async job => ({ status: job.id === receipt.jobId ? receipt.status : 'UNKNOWN' })
  });
  const recovery = await coordinator.recover();
  assert.equal(recovery.jobs[0].recoveryStatus, 'CANCELLED');
  assert.equal(recovery.recoveryRequired, true);

  // The example's explicit policy authorizes a new attempt only after reconciliation.
  // Exclusive creation prevents a repeated example attempt from duplicating this effect.
  const report = path.join(directory, 'report.txt');
  await writeFile(report, 'Recovered example task completed.\n', { flag: 'wx' });
  await coordinator.checkpoint({ goal: recovery.goal, dag: recovery.dag, activeJobs: [] });
  const final = await coordinator.recover();
  assert.equal(final.recoveryRequired, false);
  return { freshProcessRecovery: 'CANCELLED', explicitResume: true, reportCreated: true };
}

export async function runRecoveryDemo() {
  const parent = path.resolve(os.tmpdir());
  const directory = await mkdtemp(path.join(parent, 'mnemos-public-demo-'));
  let entered;
  const started = new Promise(resolve => { entered = resolve; });
  let child;
  try {
    const registry = new PhaseJobRegistry({ execute: ({ signal, setPhase }) => new Promise((resolve, reject) => {
      let killRequested = false;
      child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore', windowsHide: true });
      const abort = () => { killRequested = child.kill(); };
      signal.addEventListener('abort', abort, { once: true });
      child.once('error', reject);
      child.once('spawn', () => {
        setPhase('execution');
        entered();
        if (signal.aborted) abort();
      });
      child.once('close', code => {
        signal.removeEventListener('abort', abort);
        resolve(killRequested
          ? { status: 'CANCELLED', termination: { confirmed: true } }
          : { status: code === 0 ? 'COMPLETED' : 'FAILED' });
      });
    }) });
    registry.start({ jobId: 'example_attempt_1', objective: 'Create an example report' });
    await started;
    const cancellation = await registry.requestCancellation('example_attempt_1', { timeoutMs: 10000 });
    assert.equal(cancellation.status, 'CANCELLED');
    assert.equal(cancellation.cancellationConfirmed, true);
    await writeFile(path.join(directory, 'receipt.json'), JSON.stringify(cancellation));
    const coordinator = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) });
    await coordinator.checkpoint({ goal: { goalId: 'example_report' }, dag: { nodes: ['report'] }, activeJobs: [{ id: 'example_attempt_1' }] });
    const { stdout } = await executeFile(process.execPath, [fileURLToPath(import.meta.url), '--recover', directory], { timeout: 10000, windowsHide: true });
    return { ownedChildStopped: true, checkpointPersisted: true, ...JSON.parse(stdout) };
  } finally {
    if (child && child.exitCode === null && child.signalCode === null) child.kill();
    if (path.dirname(path.resolve(directory)) !== parent) throw new Error('Unsafe cleanup target');
    await rm(directory, { recursive: true, force: true });
  }
}

if (process.argv[2] === '--recover') {
  console.log(JSON.stringify(await recoverInNewProcess(process.argv[3])));
}
