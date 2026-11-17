import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { FilePhaseJobRegistry, TestEvidence, createCandidateVerifier } from '../src/index.mjs';
import { identityState } from '../src/process-identity.mjs';
import { temporaryDirectory } from './helpers.mjs';

const subject = 'candidate:sha256:0123abcd';
const node = process.execPath;

// A directory with test files: one that passes, one that fails.
async function testFiles(t) {
  const directory = await temporaryDirectory(t);
  await writeFile(path.join(directory, 'passing.test.mjs'), "import test from 'node:test';\ntest('adds', () => {});\n");
  await writeFile(path.join(directory, 'failing.test.mjs'),
    "import test from 'node:test';\nimport assert from 'node:assert/strict';\ntest('adds', () => assert.equal(1 + 1, 3));\n");
  return directory;
}

// Every job record in the state directory, as stored.
const storedRecords = async directory => (await Promise.all((await readdir(directory)).filter(name => name.endsWith('.job.json'))
  .map(name => readFile(path.join(directory, name), 'utf8')))).join('\n');

test('a check runs as a recorded job, and its record is the evidence', async t => {
  const [state, files] = [await temporaryDirectory(t), await testFiles(t)];
  const evidence = new TestEvidence({ directory: state });
  const receipt = await evidence.run({ category: 'unit', subject, command: node, args: ['--test', 'passing.test.mjs'], cwd: files });
  assert.deepEqual([receipt.status, receipt.completed, receipt.passed, receipt.exitCode], ['COMPLETED', true, true, 0]);
  assert.deepEqual([receipt.summary.tests, receipt.summary.pass, receipt.summary.fail], [1, 1, 0]);
  assert.equal(receipt.output.complete, true);
  assert.match(receipt.output.sha256, /^[0-9a-f]{64}$/);
  // The job record shows that a process actually ran: its tree was recorded before it started.
  const job = await evidence.jobs.get(receipt.evidenceId);
  assert.equal(job.dispatched, true);
  assert.equal(job.processes.length, 1);
  assert.equal(identityState(job.processes[0].leader) === 'DEAD', true, 'the command ran and has ended');
  assert.deepEqual([job.result.command, job.result.args, job.result.cwd], [node, ['--test', 'passing.test.mjs'], files]);
  const history = await evidence.jobs.history(receipt.evidenceId);
  assert.deepEqual(history.events.map(event => event.type).filter(type => type !== 'PHASE_CHANGED'),
    ['CREATED', 'DISPATCHED', 'PROCESS_RECORDED', 'PROCESS_RECORDED', 'STATUS_CHANGED']);
  const verification = await evidence.verify({ category: 'unit', subject, evidenceId: receipt.evidenceId });
  assert.deepEqual([verification.verified, verification.reason, verification.receipt.passed], [true, null, true]);
});

test('a failing check, and one that ran no test at all, did not pass', async t => {
  const [state, files] = [await temporaryDirectory(t), await testFiles(t)];
  const evidence = new TestEvidence({ directory: state });
  const failing = await evidence.run({ category: 'unit', subject, command: node, args: ['--test', 'failing.test.mjs'], cwd: files });
  assert.deepEqual([failing.status, failing.completed, failing.passed, failing.exitCode, failing.summary.fail], ['FAILED', true, false, 1, 1]);
  // node --test exits 0 when no file matches: the summary shows that nothing ran.
  const empty = await evidence.run({ category: 'unit', subject, command: node, args: ['--test', 'no-such-dir/*.test.mjs'], cwd: files });
  assert.deepEqual([empty.status, empty.exitCode, empty.summary.tests, empty.passed], ['COMPLETED', 0, 0, false]);
  const missing = await evidence.run({ category: 'unit', subject, command: 'mnemos-command-that-does-not-exist' });
  assert.deepEqual([missing.status, missing.passed], ['FAILED', false]);
});

test('a clean exit without a test summary passes only when no summary is expected', async t => {
  const evidence = new TestEvidence({ directory: await temporaryDirectory(t) });
  const quiet = { category: 'security', subject, command: node, args: ['-e', 'process.exit(0)'] };
  const expected = await evidence.run(quiet);
  assert.deepEqual([expected.status, expected.exitCode, expected.summary, expected.passed], ['COMPLETED', 0, null, false]);
  assert.equal((await evidence.run({ ...quiet, expectTestSummary: false })).passed, true);
});

test('a nested node --test really runs: this test process\'s NODE_TEST_CONTEXT is not passed on', async t => {
  // Under node --test this process has NODE_TEST_CONTEXT; a child node --test that inherits it
  // skips every file and exits 0 (measured). The summary shows that the test file ran.
  assert.ok(process.env.NODE_TEST_CONTEXT, 'this test runs under node --test');
  const [state, files] = [await temporaryDirectory(t), await testFiles(t)];
  const evidence = new TestEvidence({ directory: state });
  for (const env of [undefined, { ...process.env }]) {
    const receipt = await evidence.run({ category: 'unit', subject, command: node, args: ['--test', 'passing.test.mjs'], cwd: files, env });
    assert.deepEqual([receipt.passed, receipt.summary?.tests], [true, 1]);
  }
});

test('a check that runs too long is stopped and did not pass', async t => {
  const evidence = new TestEvidence({ directory: await temporaryDirectory(t) });
  const receipt = await evidence.run({ category: 'integration', subject, command: node, args: ['-e', 'setInterval(() => {}, 1000)'],
    timeoutMs: 500, expectTestSummary: false });
  assert.equal(receipt.passed, false);
  assert.equal(receipt.status, process.platform === 'win32' ? 'UNKNOWN' : 'CANCELLED'); // Windows cannot confirm a stop
  const job = await evidence.jobs.get(receipt.evidenceId);
  if (process.platform !== 'win32') assert.equal(identityState(job.processes[0].leader), 'DEAD');
});

test('evidence is verified against the records: unknown, other checks and other subjects are refused', async t => {
  const [state, files] = [await temporaryDirectory(t), await testFiles(t)];
  const evidence = new TestEvidence({ directory: state });
  const { evidenceId } = await evidence.run({ category: 'unit', subject, command: node, args: ['--test', 'passing.test.mjs'], cwd: files });
  const other = new FilePhaseJobRegistry({ directory: state, execute: async () => ({ status: 'COMPLETED', passed: true, exitCode: 0 }) });
  await other.start({ jobId: 'not_a_check', objective: 'Something else' });
  await other.wait('not_a_check');
  for (const [request, reason] of [
    [{ category: 'unit', subject }, 'EVIDENCE_MISSING'],
    [{ category: 'unit', subject, evidenceId: 'made_up_evidence' }, 'EVIDENCE_NOT_FOUND'],
    [{ category: 'unit', subject, evidenceId: '../../etc/passwd' }, 'EVIDENCE_NOT_FOUND'],
    [{ category: 'unit', subject, evidenceId: 'not_a_check' }, 'EVIDENCE_NOT_A_CHECK'],
    [{ category: 'security', subject, evidenceId }, 'EVIDENCE_CATEGORY_MISMATCH'],
    [{ category: 'unit', subject: 'candidate:another', evidenceId }, 'EVIDENCE_SUBJECT_MISMATCH']
  ]) {
    assert.deepEqual(await evidence.verify(request), { verified: false, reason, receipt: null }, reason);
  }
});

test('an evidence ID runs once: asking again reports the recorded check; another command is refused', async t => {
  const [state, files] = [await temporaryDirectory(t), await testFiles(t)];
  const evidence = new TestEvidence({ directory: state });
  const request = { category: 'unit', subject, command: node, args: ['--test', 'passing.test.mjs'], cwd: files, evidenceId: 'unit_run_1' };
  const first = await evidence.run(request);
  const again = await evidence.run(request);
  assert.deepEqual(again, first);
  assert.equal((await evidence.jobs.searchHistory({ jobId: 'unit_run_1', type: 'DISPATCHED' })).events.length, 1, 'it ran once');
  await assert.rejects(evidence.run({ ...request, args: ['--test', 'failing.test.mjs'] }), { code: 'JOB_IDENTITY_CONFLICT' });
});

test('all output is digested; its kept tail is redacted; the environment is used but never recorded', async t => {
  const state = await temporaryDirectory(t);
  const evidence = new TestEvidence({ directory: state });
  const secret = 'fixture' + 'Z9'.repeat(12);
  // The secret is only in the environment and the output; the command (recorded) names a hash of it.
  const script = `const digest = require('node:crypto').createHash('sha256').update(process.env.CHECK_TOKEN ?? '').digest('hex');
    if (digest !== ${JSON.stringify(createHash('sha256').update(secret).digest('hex'))}) process.exit(9);
    process.stdout.write('x'.repeat(1024 * 1024) + '\\npassword=' + process.env.CHECK_TOKEN + '\\n');`;
  const receipt = await evidence.run({ category: 'security', subject, command: node, args: ['-e', script],
    env: { PATH: process.env.PATH, CHECK_TOKEN: secret }, expectTestSummary: false });
  assert.deepEqual([receipt.passed, receipt.exitCode, receipt.summary], [true, 0, null]);
  const expected = 'x'.repeat(1024 * 1024) + `\npassword=${secret}\n`;
  assert.deepEqual([receipt.output.bytes, receipt.output.sha256], [Buffer.byteLength(expected), createHash('sha256').update(expected).digest('hex')]);
  const job = await evidence.jobs.get(receipt.evidenceId);
  assert.match(job.result.output.tail, /REDACTED/);
  assert.equal((await storedRecords(state)).includes(secret), false, 'neither the output secret nor the environment is stored');
});

test('check arguments are validated before anything runs', async t => {
  const evidence = new TestEvidence({ directory: await temporaryDirectory(t) });
  for (const invalid of [{ category: 'Unit' }, { category: 'unit', subject: '' }, { category: 'unit', subject, command: '' },
    { category: 'unit', subject, command: node, args: [1] }, { category: 'unit', subject, command: node, timeoutMs: 0 },
    { category: 'unit', subject, command: node, expectTestSummary: 'yes' }, { category: 'unit', subject, command: node, env: 'X=1' }]) {
    await assert.rejects(evidence.run(invalid), TypeError);
  }
});

test('the regression runner gives createCandidateVerifier a recorded check, and an abort stops it', async t => {
  const [state, files] = [await temporaryDirectory(t), await testFiles(t)];
  const evidence = new TestEvidence({ directory: state });
  const workspace = { diff: async () => '-a\n+b\n', changedFiles: async () => ['src/task.mjs'] };
  const verify = createCandidateVerifier({ runRegression: evidence.regressionRunner({ command: node, args: ['--test', 'passing.test.mjs'], cwd: files }) });
  const verified = await verify({ workspace, allowedFiles: ['src/task.mjs'] });
  assert.equal(verified.status, 'VERIFIED');
  assert.equal(verified.subject, `diff:sha256:${createHash('sha256').update('-a\n+b\n').digest('hex')}`);
  const checked = await evidence.verify({ category: 'regression', subject: verified.subject, evidenceId: verified.evidenceId });
  assert.deepEqual([checked.verified, checked.receipt.passed], [true, true]);

  const controller = new AbortController();
  const hanging = createCandidateVerifier({ runRegression: evidence.regressionRunner({ command: node,
    args: ['-e', 'setInterval(() => {}, 1000)'], expectTestSummary: false }) });
  setTimeout(() => controller.abort(), 500);
  const aborted = await hanging({ workspace, allowedFiles: ['src/task.mjs'], signal: controller.signal });
  assert.equal(aborted.status, process.platform === 'win32' ? 'UNKNOWN' : 'CANCELLED'); // a stop is confirmed on POSIX only
});
