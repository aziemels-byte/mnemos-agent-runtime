import test from 'node:test';
import assert from 'node:assert/strict';
import { createCandidateVerifier } from '../src/index.mjs';

function fixture({ paths = ['src/task.mjs'], diff = '+ change', result = { passed: true, exitCode: 0 } } = {}) {
  let calls = 0;
  return {
    workspace: { changedFiles: async () => paths, diff: async () => diff },
    verify: createCandidateVerifier({ runRegression: async () => { calls++; return result; } }),
    calls: () => calls
  };
}

for (const [name, options, allowedFiles, reason] of [
  ['empty scope', {}, [], 'allowed_scope_missing'],
  ['empty candidate', { paths: [] }, ['src/task.mjs'], 'changed_files_missing'],
  ['scope violation', { paths: ['other.mjs'] }, ['src/task.mjs'], 'scope_violation'],
  ['missing diff', { diff: '  ' }, ['src/task.mjs'], 'diff_missing']
]) {
  test(`rejects ${name} without calling regression`, async () => {
    const f = fixture(options);
    const result = await f.verify({ workspace: f.workspace, allowedFiles });
    assert.equal(result.reason, reason);
    assert.equal(result.passed, false);
    assert.equal(f.calls(), 0);
  });
}

test('valid candidate runs exactly one regression and records phases', async () => {
  const f = fixture();
  const phases = [];
  const result = await f.verify({ workspace: f.workspace, allowedFiles: ['src/task.mjs'], onPhase: phase => phases.push(phase) });
  assert.equal(result.status, 'VERIFIED');
  assert.deepEqual(phases, ['candidate_preflight', 'full_regression']);
  assert.equal(f.calls(), 1);
});

test('a success flag with a failing exit code cannot verify a candidate', async () => {
  const f = fixture({ result: { passed: true, exitCode: 1 } });
  assert.equal((await f.verify({ workspace: f.workspace, allowedFiles: ['src/task.mjs'] })).status, 'REJECTED');
});

for (const path of ['../escape.mjs', '/absolute', 'C:\\private', 'C:relative', 'src/../escape', 'src//task', 'src/./task', 'src\\task.mjs', 'bad\0path']) {
  test(`rejects invalid repository path ${JSON.stringify(path)}`, async () => {
    const f = fixture({ paths: ['src/task.mjs', path] });
    await assert.rejects(f.verify({ workspace: f.workspace, allowedFiles: ['src/task.mjs'] }), TypeError);
    assert.equal(f.calls(), 0);
  });
}

test('pre-aborted work never starts regression', async () => {
  const f = fixture();
  const result = await f.verify({ workspace: f.workspace, allowedFiles: ['src/task.mjs'], signal: AbortSignal.abort() });
  assert.equal(result.status, 'CANCELLED');
  assert.equal(f.calls(), 0);
});

test('cancellation during regression needs owned termination evidence', async () => {
  for (const confirmed of [false, true]) {
    const controller = new AbortController();
    const verify = createCandidateVerifier({ runRegression: async ({ signal }) => {
      assert.equal(signal, controller.signal);
      controller.abort();
      return { cancelled: true, termination: { confirmed } };
    } });
    const result = await verify({ workspace: fixture().workspace, allowedFiles: ['src/task.mjs'], signal: controller.signal });
    assert.equal(result.status, confirmed ? 'CANCELLED' : 'UNKNOWN');
    assert.equal(result.reconciliationRequired, !confirmed);
  }
});

test('workspace inspection failures propagate without executing regression', async () => {
  const f = fixture();
  f.workspace.changedFiles = async () => { throw new Error('unreadable workspace'); };
  await assert.rejects(f.verify({ workspace: f.workspace, allowedFiles: ['src/task.mjs'] }), /unreadable/);
  assert.equal(f.calls(), 0);
});
