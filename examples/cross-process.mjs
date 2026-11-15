import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  FencingGuard, FileCheckpointStore, FileIdempotencyRegistry, FileResourceLeaseManager,
  GoalRecoveryCoordinator, keepLeaseAlive
} from '../src/index.mjs';

// Synthetic demonstration: several OS processes on one host share a state directory.
// No GPU, credential or remote service is used; "using the GPU" is a short sleep.
const WORKERS = 4;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const nodes = { require: () => ({ trust: 'VERIFIED', status: 'AVAILABLE' }) };

async function worker(directory, name) {
  // Every message to the resource owner waits for its acknowledgement, so a use is
  // confirmed by the resource before the lease is released (causal order, not timing).
  let nextId = 0;
  const pending = new Map();
  process.on('message', reply => { pending.get(reply.id)?.(reply); pending.delete(reply.id); });
  const send = message => new Promise(resolve => { const id = ++nextId; pending.set(id, resolve); process.send({ ...message, id }); });
  const leases = new FileResourceLeaseManager({ directory, nodeRegistry: nodes, lockTimeoutMs: 20_000 });
  const effects = new FileIdempotencyRegistry({ directory, lockTimeoutMs: 20_000 });
  const coordinator = new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory, { lockTimeoutMs: 20_000 }) });

  // 1. Exclusive lease: wait for gpu:0, keep it alive while working, report use with the fencing token.
  const request = { ownerId: `worker:${name}`, nodeId: 'local', resources: ['gpu:0'], ttlMs: 2_000, explicitAuthorization: true };
  let acquired;
  for (const deadline = Date.now() + 30_000; ; await sleep(20)) {
    acquired = await leases.acquire(request);
    if (acquired.status === 'LEASE_ACQUIRED') break;
    if (Date.now() > deadline) throw new Error('gpu:0 never became available');
  }
  const keeper = keepLeaseAlive({ manager: leases, lease: acquired.lease, ownerId: request.ownerId, ttlMs: 2_000, explicitAuthorization: true });
  const token = acquired.lease.fencingToken;
  await send({ type: 'gpu', phase: 'start', worker: name, token });
  await sleep(50);
  await send({ type: 'gpu', phase: 'end', worker: name, token });
  const lost = keeper.signal.aborted;
  await keeper.stop({ release: true });

  // 2. One effect for everyone: all workers request the weekly report at the same
  // moment, and it is produced exactly once.
  await send({ type: 'barrier', name: 'report' });
  const report = await effects.execute({ key: 'report:weekly', request: { week: 7 }, operation: async () => {
    await writeFile(path.join(directory, 'weekly-report.txt'), `produced by ${name}\n`, { flag: 'wx' });
    await sleep(200); // long enough for the other workers to arrive while it runs
    return { producedBy: name };
  } });

  // 3. Shared checkpoint: all workers save at the same moment (the parent releases them
  // together); each adds itself without losing anyone else's update.
  await send({ type: 'barrier', name: 'checkpoint' });
  let conflicts = 0;
  for (;;) {
    const recovered = await coordinator.recover();
    const completed = recovered?.dag?.completedWorkers ?? [];
    try {
      await coordinator.checkpoint({ goal: { goalId: 'weekly_report' }, dag: { completedWorkers: [...completed, name] } });
      break;
    } catch (error) {
      if (error.code !== 'CHECKPOINT_CONFLICT') throw error;
      conflicts++; // another worker saved in between: reload and merge again
    }
  }
  await send({ type: 'done', worker: name, lost, report: report.status, conflicts });
}

export async function runCrossProcessDemo() {
  const parent = path.resolve(os.tmpdir());
  const directory = await mkdtemp(path.join(parent, 'mnemos-public-demo-'));
  const children = [];
  try {
    // The parent owns the protected resource: every use must pass the fencing guard.
    const guard = new FencingGuard();
    const gpuLog = [];
    const done = [];
    const waiting = new Map(); // barrier name -> acknowledgements held back
    const finished = Array.from({ length: WORKERS }, (_, index) => new Promise((resolve, reject) => {
      const child = spawn(process.execPath, [fileURLToPath(import.meta.url), '--worker', directory, `w${index + 1}`],
        { stdio: ['ignore', 'ignore', 'pipe', 'ipc'], windowsHide: true });
      children.push(child);
      let stderr = '';
      child.stderr.on('data', chunk => { stderr += chunk; });
      child.on('message', message => {
        let admitted = true;
        if (message.type === 'barrier') {
          const held = waiting.get(message.name) ?? [];
          held.push(() => child.send({ id: message.id, admitted }));
          waiting.set(message.name, held);
          if (held.length === WORKERS) for (const release of held) release(); // everyone goes at once
          return;
        }
        if (message.type === 'done') done.push(message);
        else {
          try { guard.admit('local/gpu:0', message.token); }
          catch (error) { if (error.code !== 'FENCE_STALE') throw error; admitted = false; }
          gpuLog.push({ ...message, admitted });
        }
        child.send({ id: message.id, admitted });
      });
      child.on('error', reject);
      child.on('close', code => code === 0 ? resolve() : reject(new Error(`worker exited ${code}: ${stderr}`)));
    }));
    await Promise.all(finished);

    // Verify what the processes did, from the shared state and the resource owner's log.
    const overlaps = gpuLog.filter((entry, index) => entry.phase === 'start' && gpuLog[index + 1]?.phase !== 'end').length;
    const tokens = gpuLog.filter(entry => entry.phase === 'start').map(entry => entry.token);
    const reports = done.map(entry => entry.report);
    const checkpoint = await new GoalRecoveryCoordinator({ store: new FileCheckpointStore(directory) }).recover();
    const reportFile = await readFile(path.join(directory, 'weekly-report.txt'), 'utf8');
    assert.equal(overlaps, 0);
    assert.deepEqual(tokens, [...tokens].sort((a, b) => a - b));
    assert.equal(reports.filter(status => status === 'IDEMPOTENCY_EXECUTED').length, 1);
    assert.equal(checkpoint.dag.completedWorkers.length, WORKERS);
    return {
      workers: WORKERS,
      lease: { exclusiveUses: tokens.length, overlaps, fencingTokens: tokens,
        staleWritesRejected: gpuLog.filter(entry => !entry.admitted).length, leasesLost: done.filter(entry => entry.lost).length },
      idempotency: { effectsPerformed: reportFile.trim().split('\n').length,
        outcomes: Object.fromEntries([...new Set(reports)].sort().map(status => [status, reports.filter(s => s === status).length])) },
      checkpoint: { workersRecorded: checkpoint.dag.completedWorkers.length,
        conflictsDetectedAndMerged: done.reduce((sum, entry) => sum + entry.conflicts, 0) }
    };
  } finally {
    for (const child of children) if (child.exitCode === null && child.signalCode === null) child.kill();
    if (path.dirname(path.resolve(directory)) !== parent) throw new Error('Unsafe cleanup target');
    await rm(directory, { recursive: true, force: true });
  }
}

if (process.argv[2] === '--worker') {
  await worker(process.argv[3], process.argv[4]);
  process.disconnect();
} else if (process.argv[1] === fileURLToPath(import.meta.url)) {
  console.log(JSON.stringify(await runCrossProcessDemo(), null, 2));
}
