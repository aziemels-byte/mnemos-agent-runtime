import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';

test('demonstrates actual regression, owned child termination and recovery in a fresh process', { timeout: 20000 }, async () => {
  const { stdout } = await promisify(execFile)(process.execPath,
    [fileURLToPath(new URL('../examples/run.mjs', import.meta.url))], { timeout: 15000, windowsHide: true });
  const result = JSON.parse(stdout);
  assert.equal(result.candidateVerification.realRegression, 'VERIFIED');
  assert.equal(result.candidateVerification.regressionRuns, 1);
  assert.equal(result.checkpointRecovery.ownedChildStopped, true);
  assert.equal(result.checkpointRecovery.freshProcessRecovery, 'CANCELLED');
  assert.equal(result.checkpointRecovery.reportCreated, true);
});

test('worker processes share a lease, one effect and a checkpoint without conflicts being lost', { timeout: 90000 }, async () => {
  const { stdout } = await promisify(execFile)(process.execPath,
    [fileURLToPath(new URL('../examples/cross-process.mjs', import.meta.url))], { timeout: 80000, windowsHide: true });
  const result = JSON.parse(stdout);
  // Only invariants: which worker wins each race varies from run to run.
  assert.equal(result.lease.exclusiveUses, result.workers);
  assert.equal(result.lease.overlaps, 0);
  assert.deepEqual(result.lease.fencingTokens, [...result.lease.fencingTokens].sort((a, b) => a - b));
  assert.equal(result.lease.staleWritesRejected, 0);
  assert.equal(result.idempotency.effectsPerformed, 1);
  assert.equal(result.idempotency.outcomes.IDEMPOTENCY_EXECUTED, 1);
  assert.equal(result.checkpoint.workersRecorded, result.workers);
});
