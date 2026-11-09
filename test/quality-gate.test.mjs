import test from 'node:test';
import assert from 'node:assert/strict';
import {
  AutomatedEvaluationGate, CandidateComparisonGate, IsolatedSelfModificationGate,
  QualificationGate, QualityFloorPolicy, QualityGate,
  REQUIRED_EVALUATIONS, REQUIRED_QUALIFICATIONS
} from '../src/index.mjs';

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

test('integrated gate reports actual rejection and never authorizes automatic mutation', () => {
  const gate = new QualityGate();
  const input = { definition: definition(), checks: checks(), baseline: metrics(), candidate: metrics() };
  const approved = gate.review(input);
  assert.equal(approved.status, 'READY_FOR_HUMAN_REVIEW');
  assert.equal(approved.automaticMerge, false);
  assert.equal(approved.productionMutation, false);
  assert.equal(approved.requiresHumanReview, true);
  assert.equal(gate.review({ ...input, definition: {} }).status, 'REJECTED_POLICY');
  assert.equal(gate.review({ ...input, checks: { ...checks(), security: { passed: true } } }).status, 'REJECTED_CHECKS');
  assert.equal(gate.review({ ...input, candidate: { ...metrics(), quality: 0.2 } }).status, 'CANDIDATE_REGRESSION');
  assert.equal(gate.review({ ...input, requireQualification: true }).status, 'NOT_QUALIFIED');
  assert.equal(gate.review({ ...input, requireQualification: true, qualification: qualification() }).status, 'READY_FOR_HUMAN_REVIEW');
});
