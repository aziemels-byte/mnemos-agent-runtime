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
