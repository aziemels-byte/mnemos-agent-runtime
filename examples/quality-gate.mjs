import { createHash } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { QualityGate, REQUIRED_EVALUATIONS, TestEvidence } from '../src/index.mjs';

// Every required check is actually run, as a recorded job; the gate reads the
// results from those records. The test file and metrics are synthetic.
const parent = path.resolve(os.tmpdir());
const directory = await mkdtemp(path.join(parent, 'mnemos-public-gate-'));
try {
  const test = "import test from 'node:test';\nimport assert from 'node:assert/strict';\ntest('adds', () => assert.equal(1 + 1, 2));\n";
  await writeFile(path.join(directory, 'example.test.mjs'), test);
  const subject = `example:sha256:${createHash('sha256').update(test).digest('hex')}`; // what is checked
  const evidence = new TestEvidence({ directory: path.join(directory, 'state') });
  const receipts = await Promise.all(REQUIRED_EVALUATIONS.map(category => evidence.run({ category, subject,
    command: process.execPath, args: ['--test', 'example.test.mjs'], cwd: directory })));
  const baseline = { sampleCount: 20, successRate: 0.9, quality: 0.85, cost: 1, latency: 10, regressions: 0 };
  const result = await new QualityGate({ evidence }).review({
    definition: { executionPolicy: { requiresIsolatedWorktree: true }, approvalPolicy: { autoMerge: false } },
    subject, checks: Object.fromEntries(receipts.map(receipt => [receipt.category, receipt.evidenceId])),
    baseline, candidate: { ...baseline, quality: 0.9 }
  });
  if (result.status !== 'READY_FOR_HUMAN_REVIEW' || result.automaticMerge !== false) {
    throw new Error(`Quality gate unexpectedly rejected the synthetic candidate: ${JSON.stringify(result.evaluations)}`);
  }
  console.log(JSON.stringify({
    status: result.status, verifiedChecks: Object.values(result.evaluations.evidence).filter(check => check.verified && check.passed).length,
    automaticMerge: result.automaticMerge, productionMutation: result.productionMutation
  }));
} finally {
  if (path.dirname(path.resolve(directory)) !== parent) throw new Error('Unsafe cleanup target');
  await rm(directory, { recursive: true, force: true });
}
