import test from 'node:test';
import assert from 'node:assert/strict';
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import {
  AutomatedEvaluationGate, CandidateComparisonGate, IsolatedSelfModificationGate,
  QualificationGate, QualityFloorPolicy, QualityGate, TestEvidence,
  REQUIRED_EVALUATIONS, REQUIRED_QUALIFICATIONS
} from '../src/index.mjs';
import { temporaryDirectory } from './helpers.mjs';

const checks = () => Object.fromEntries(REQUIRED_EVALUATIONS.map(name =>
  [name, { passed: true, completed: true, exitCode: 0, evidenceId: 'fixture-' + name }]));
const definition = () => ({
  executionPolicy: { requiresIsolatedWorktree: true },
  approvalPolicy: { autoMerge: false }
});
const metrics = () => ({
  sampleCount: 25, successRate: 0.92, quality: 0.85,
  cost: 2, latency: 25, regressions: 0
});
const qualification = () => Object.fromEntries(REQUIRED_QUALIFICATIONS.map(name => [name, {
  passed: true, evidenceId: 'fixture-' + name, recordedAt: '2026-01-01T00:00:00.000Z',
  summary: 'Synthetic test evidence', checks: [{ id: name + '-check', passed: true }]
}]));

test('all test receipt categories must be completed and have an exit code and evidence ID', () => {
  const gate = new AutomatedEvaluationGate();
  assert.equal(gate.evaluate(checks()).status, 'ACCEPTED_FOR_COMPARISON');
  for (const broken of [
    { passed: false }, { passed: true }, { passed: true, completed: true, exitCode: 1, evidenceId: 'x' },
    { passed: true, completed: true, exitCode: 0 }
  ]) {
    const result = gate.evaluate({ ...checks(), security: broken });
    assert.equal(result.status, 'REJECTED');
    assert.deepEqual(result.missingOrFailed, ['security']);
    assert.equal(result.automaticPromotion, false);
  }
  assert.deepEqual(gate.evaluate({}).missingOrFailed, [...REQUIRED_EVALUATIONS]);
});

test('comparison requires finite complete metrics and matching sample sizes', () => {
  const gate = new CandidateComparisonGate();
  const base = metrics();
  assert.equal(gate.compare({ baseline: base, candidate: { ...base } }).status, 'CANDIDATE_NON_REGRESSING');
  assert.equal(gate.compare({ baseline: base, candidate: { ...base, quality: 0.84 } }).status, 'CANDIDATE_REGRESSION');
  assert.equal(gate.compare({ baseline: base, candidate: { ...base, regressions: 1 } }).status, 'CANDIDATE_REGRESSION');
  assert.equal(gate.compare({ baseline: base, candidate: { ...base, sampleCount: 24 } }).status, 'INSUFFICIENT_COMPARABLE_EVIDENCE');
  assert.equal(gate.compare({ baseline: base, candidate: { ...base, latency: Number.NaN } }).status, 'INSUFFICIENT_COMPARABLE_EVIDENCE');
  assert.equal(gate.compare({ baseline: base, candidate: { ...base, successRate: Infinity } }).status, 'INSUFFICIENT_COMPARABLE_EVIDENCE');
  assert.equal(gate.compare({ baseline: base }).status, 'INSUFFICIENT_COMPARABLE_EVIDENCE');
});

test('isolated worktree declaration and explicitly disabled auto-merge are required', () => {
  const gate = new IsolatedSelfModificationGate();
  assert.equal(gate.authorize(definition()).status, 'AUTHORIZED_FOR_CANDIDATE_ONLY');
  assert.equal(gate.authorize({ approvalPolicy: { autoMerge: false } }).reason, 'ISOLATED_WORKTREE_REQUIRED');
  assert.equal(gate.authorize({ executionPolicy: { requiresIsolatedWorktree: true } }).reason, 'AUTO_MERGE_MUST_BE_DISABLED');
  assert.equal(gate.authorize({ executionPolicy: { requiresIsolatedWorktree: true }, approvalPolicy: { autoMerge: true } }).status, 'REJECTED');
});

test('qualification rejects missing categories, empty checks and invalid timestamps', () => {
  const gate = new QualificationGate();
  assert.equal(gate.assess(qualification()).status, 'QUALIFIED_FOR_RELEASE_REVIEW');
  const incomplete = qualification();
  incomplete.chaos = { ...incomplete.chaos, checks: [] };
  assert.deepEqual(gate.assess(incomplete).missing, ['chaos']);
  const invalid = qualification();
  invalid.security.recordedAt = 'invalid';
  assert.deepEqual(gate.assess(invalid).missing, ['security']);
  assert.equal(gate.assess({}).automaticRelease, false);
});

test('quality floor filters unsupported routes and never executes them', () => {
  const gate = new QualityFloorPolicy();
  const candidates = [
    { routeId: 'cheap', expectedSuccess: 0.8, marginalCost: 0.1 },
    { routeId: 'strong', expectedSuccess: 0.99, marginalCost: 2 },
    { routeId: 'unknown' }
  ];
  const result = gate.apply({ qualityFloor: 0.95, routeProposal: { selected: { routeId: 'cheap' }, candidates } });
  assert.equal(result.status, 'QUALITY_FLOOR_MET');
  assert.equal(result.selected.routeId, 'strong');
  assert.equal(result.automaticExecution, false);
  assert.equal(gate.apply({ qualityFloor: 1, routeProposal: { candidates } }).status, 'QUALITY_FLOOR_UNMET');
  assert.throws(() => gate.apply({ qualityFloor: 1.2, routeProposal: { candidates } }), /qualityFloor/);
  assert.throws(() => gate.apply({ qualityFloor: 0.5, routeProposal: { candidates: [candidates[0], candidates[0]] } }), /duplicate/);
  assert.throws(() => gate.apply({ qualityFloor: 0.5, routeProposal: { candidates: [{ routeId: 'a', expectedSuccess: Number.NaN }] } }), /Invalid/);
});

// An evidence store that verifies what a map of evidence IDs says; the gate's own logic is what is tested here.
const stubEvidence = records => ({ verify: async ({ category, subject, evidenceId }) => {
  const record = records[evidenceId];
  if (!record) return { verified: false, reason: 'EVIDENCE_NOT_FOUND', receipt: null };
  if (record.subject !== subject) return { verified: false, reason: 'EVIDENCE_SUBJECT_MISMATCH', receipt: null };
  return { verified: true, reason: null, receipt: { category, evidenceId, completed: true, passed: record.passed, exitCode: record.passed ? 0 : 1 } };
} });
const evidenceIds = () => Object.fromEntries(REQUIRED_EVALUATIONS.map(name => [name, `check_${name}`]));
const passingRecords = () => Object.fromEntries(REQUIRED_EVALUATIONS.map(name => [`check_${name}`, { subject: 'candidate_1', passed: true }]));

test('integrated gate reports actual rejection and never authorizes automatic mutation', async () => {
  const gate = new QualityGate({ evidence: stubEvidence(passingRecords()) });
  const input = { definition: definition(), subject: 'candidate_1', checks: evidenceIds(), baseline: metrics(), candidate: metrics() };
  const approved = await gate.review(input);
  assert.equal(approved.status, 'READY_FOR_HUMAN_REVIEW');
  assert.equal(approved.automaticMerge, false);
  assert.equal(approved.productionMutation, false);
  assert.equal(approved.requiresHumanReview, true);
  assert.equal((await gate.review({ ...input, definition: {} })).status, 'REJECTED_POLICY');
  assert.equal((await gate.review({ ...input, candidate: { ...metrics(), quality: 0.2 } })).status, 'CANDIDATE_REGRESSION');
  assert.equal((await gate.review({ ...input, requireQualification: true })).status, 'NOT_QUALIFIED');
  assert.equal((await gate.review({ ...input, requireQualification: true, qualification: qualification() })).status, 'READY_FOR_HUMAN_REVIEW');
  const failing = new QualityGate({ evidence: stubEvidence({ ...passingRecords(), check_security: { subject: 'candidate_1', passed: false } }) });
  const rejected = await failing.review(input);
  assert.deepEqual([rejected.status, rejected.evaluations.missingOrFailed, rejected.evaluations.evidence.security],
    ['REJECTED_CHECKS', ['security'], { evidenceId: 'check_security', verified: true, reason: null, passed: false }]);
});

test('the gate takes no check result from its caller: every check is resolved from its evidence', async () => {
  const gate = new QualityGate({ evidence: stubEvidence(passingRecords()) });
  const input = { definition: definition(), subject: 'candidate_1', baseline: metrics(), candidate: metrics() };
  // Receipts claiming success, as the gate used to accept them: their IDs resolve to nothing.
  const claimed = await gate.review({ ...input, checks: checks() });
  assert.equal(claimed.status, 'REJECTED_CHECKS');
  assert.deepEqual(claimed.evaluations.missingOrFailed, [...REQUIRED_EVALUATIONS]);
  assert.equal(claimed.evaluations.evidence.unit.reason, 'EVIDENCE_NOT_FOUND');
  const replayed = await gate.review({ ...input, subject: 'candidate_2', checks: evidenceIds() });
  assert.deepEqual([replayed.status, replayed.evaluations.evidence.unit.reason], ['REJECTED_CHECKS', 'EVIDENCE_SUBJECT_MISMATCH']);
  assert.equal((await gate.review({ ...input, checks: { ...evidenceIds(), recovery: undefined } })).evaluations.evidence.recovery.verified, false);
  assert.throws(() => new QualityGate(), /evidence store/);
});

test('with checks the runtime ran, only verified passing evidence for this candidate qualifies', async t => {
  const [state, files] = [await temporaryDirectory(t), await temporaryDirectory(t)];
  await writeFile(path.join(files, 'passing.test.mjs'), "import test from 'node:test';\ntest('adds', () => {});\n");
  await writeFile(path.join(files, 'failing.test.mjs'), "import test from 'node:test';\ntest('adds', () => { throw new Error('no'); });\n");
  const evidence = new TestEvidence({ directory: state });
  const subject = 'candidate:sha256:feedbeef';
  const run = (category, file) => evidence.run({ category, subject, command: process.execPath, args: ['--test', file], cwd: files });
  const receipts = await Promise.all(REQUIRED_EVALUATIONS.map(category => run(category, 'passing.test.mjs')));
  const checks = Object.fromEntries(receipts.map(receipt => [receipt.category, receipt.evidenceId]));
  const gate = new QualityGate({ evidence });
  const input = { definition: definition(), subject, checks, baseline: metrics(), candidate: metrics() };
  assert.equal((await gate.review(input)).status, 'READY_FOR_HUMAN_REVIEW');
  const failing = await run('security', 'failing.test.mjs');
  const rejected = await gate.review({ ...input, checks: { ...checks, security: failing.evidenceId } });
  assert.deepEqual([rejected.status, rejected.evaluations.evidence.security.passed], ['REJECTED_CHECKS', false]);
  const swapped = await gate.review({ ...input, checks: { ...checks, security: checks.unit } }); // unit evidence for security
  assert.equal(swapped.evaluations.evidence.security.reason, 'EVIDENCE_CATEGORY_MISMATCH');
  assert.equal((await gate.review({ ...input, subject: 'candidate:sha256:other' })).status, 'REJECTED_CHECKS');
});
