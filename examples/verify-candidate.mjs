import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { createCandidateVerifier } from '../src/index.mjs';

const executeFile = promisify(execFile);

export async function runVerificationDemo() {
  const parent = path.resolve(os.tmpdir());
  const directory = await mkdtemp(path.join(parent, 'mnemos-public-verify-'));
  const before = 'export const sum = (a, b) => a - b;\n';
  const after = 'export const sum = (a, b) => a + b;\n';
  const file = path.join(directory, 'sum.mjs');
  let regressionRuns = 0;
  try {
    await writeFile(file, before);
    // A real single-file adapter. Production adapters must enumerate ALL changed files.
    const workspace = {
      diff: async () => (await readFile(file, 'utf8')) === before ? '' : `-${before}+${await readFile(file, 'utf8')}`,
      changedFiles: async () => (await readFile(file, 'utf8')) === before ? [] : ['sum.mjs']
    };
    const verify = createCandidateVerifier({ runRegression: async ({ signal }) => {
      regressionRuns++;
      const source = `import assert from 'node:assert/strict'; import {sum} from ${JSON.stringify(pathToFileURL(file).href)}; assert.equal(sum(2, 3), 5);`;
      try {
        await executeFile(process.execPath, ['--input-type=module', '-e', source], { signal, timeout: 5000, windowsHide: true });
        return { passed: true, exitCode: 0 };
      } catch (error) {
        // This adapter does not prove child termination after abort; keep it unconfirmed.
        if (signal?.aborted) return { cancelled: true, termination: { confirmed: false } };
        return { passed: false, exitCode: typeof error.code === 'number' ? error.code : 1 };
      }
    } });
    const empty = await verify({ workspace, allowedFiles: ['sum.mjs'] });
    assert.equal(empty.status, 'REJECTED');
    assert.equal(regressionRuns, 0);
    await writeFile(file, after);
    const verified = await verify({ workspace, allowedFiles: ['sum.mjs'] });
    assert.equal(verified.status, 'VERIFIED');
    assert.equal(regressionRuns, 1);
    return { emptyCandidate: 'REJECTED', realRegression: 'VERIFIED', regressionRuns };
  } finally {
    if (path.dirname(path.resolve(directory)) !== parent) throw new Error('Unsafe cleanup target');
    await rm(directory, { recursive: true, force: true });
  }
}
