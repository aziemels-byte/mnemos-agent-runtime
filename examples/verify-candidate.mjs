import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { TestEvidence, createCandidateVerifier } from '../src/index.mjs';

export async function runVerificationDemo() {
  const parent = path.resolve(os.tmpdir());
  const directory = await mkdtemp(path.join(parent, 'mnemos-public-verify-'));
  const before = 'export const sum = (a, b) => a - b;\n';
  const after = 'export const sum = (a, b) => a + b;\n';
  const file = path.join(directory, 'sum.mjs');
  let regressionRuns = 0;
  try {
    await writeFile(file, before);
    // The candidate's test file (its import names the candidate's file, not one of this repository).
    const specifier = JSON.stringify('./sum.mjs');
    await writeFile(path.join(directory, 'sum.test.mjs'),
      `import test from 'node:test';\nimport assert from 'node:assert/strict';\nimport { sum } from ${specifier};\ntest('adds', () => assert.equal(sum(2, 3), 5));\n`);
    // A real single-file adapter. Production adapters must enumerate ALL changed files.
    const workspace = {
      diff: async () => (await readFile(file, 'utf8')) === before ? '' : `-${before}+${await readFile(file, 'utf8')}`,
      changedFiles: async () => (await readFile(file, 'utf8')) === before ? [] : ['sum.mjs']
    };
    // The regression suite runs as a recorded check; its record is the evidence.
    const evidence = new TestEvidence({ directory: path.join(directory, 'state') });
    const runner = evidence.regressionRunner({ command: process.execPath, args: ['--test', 'sum.test.mjs'], cwd: directory });
    const verify = createCandidateVerifier({ runRegression: async context => { regressionRuns++; return runner(context); } });
    const empty = await verify({ workspace, allowedFiles: ['sum.mjs'] });
    assert.equal(empty.status, 'REJECTED');
    assert.equal(regressionRuns, 0);
    await writeFile(file, after);
    const verified = await verify({ workspace, allowedFiles: ['sum.mjs'] });
    assert.equal(verified.status, 'VERIFIED');
    assert.equal(regressionRuns, 1);
    // What QualityGate does with it: resolve the evidence from the records, for this candidate.
    const evidenceCheck = await evidence.verify({ category: 'regression', subject: verified.subject, evidenceId: verified.evidenceId });
    assert.equal(evidenceCheck.verified && evidenceCheck.receipt.passed, true);
    return { emptyCandidate: 'REJECTED', realRegression: 'VERIFIED', regressionRuns, evidenceVerified: true,
      testsRun: evidenceCheck.receipt.summary.tests };
  } finally {
    if (path.dirname(path.resolve(directory)) !== parent) throw new Error('Unsafe cleanup target');
    await rm(directory, { recursive: true, force: true });
  }
}
