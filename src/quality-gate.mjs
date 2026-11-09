// Standalone, side-effect-free candidate quality gates adapted from MNEMOS.
// A caller must use trusted test runners and keep sensitive evidence private.
export const REQUIRED_EVALUATIONS = Object.freeze([
  'unit', 'integration', 'regression', 'benchmark', 'security', 'recovery'
]);
export const REQUIRED_QUALIFICATIONS = Object.freeze([
  'bugs', 'security', 'recovery', 'determinism', 'migrations',
  'telemetry', 'benchmarks', 'chaos'
]);
const nonempty = value => typeof value === 'string' && value.trim().length > 0;
const unitInterval = value => typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1;
const nonnegative = value => typeof value === 'number' && Number.isFinite(value) && value >= 0;

/**
 * Every required check must supply a completed, successful process receipt.
 * The caller is responsible for ensuring that evidence originates from a
 * trusted runner; this module does not execute or authenticate test runs.
 */
export class AutomatedEvaluationGate {
  evaluate(results = {}) {
    const missingOrFailed = REQUIRED_EVALUATIONS.filter(category => {
      const receipt = results?.[category];
      return receipt?.passed !== true ||
        receipt?.completed !== true ||
        receipt?.exitCode !== 0 ||
        !nonempty(receipt?.evidenceId);
    });
    return {
      status: missingOrFailed.length ? 'REJECTED' : 'ACCEPTED_FOR_COMPARISON',
      required: [...REQUIRED_EVALUATIONS],
      missingOrFailed,
      automaticPromotion: false
    };
  }
}

/**
 * Only compares explicitly supplied, finite baseline/candidate metrics.
 * Equal sample count alone does not establish dataset equivalence: the
 * caller must ensure both measurements use comparable test distributions.
 */
export class CandidateComparisonGate {
  compare({ baseline, candidate } = {}) {
    const valid = row =>
      row !== null && typeof row === 'object' &&
      Number.isSafeInteger(row.sampleCount) && row.sampleCount > 0 &&
      unitInterval(row.successRate) && unitInterval(row.quality) &&
      nonnegative(row.cost) && nonnegative(row.latency) &&
      Number.isSafeInteger(row.regressions) && row.regressions >= 0;
    if (!valid(baseline) || !valid(candidate) || baseline.sampleCount !== candidate.sampleCount) {
      return { status: 'INSUFFICIENT_COMPARABLE_EVIDENCE', deltas: null, automaticPromotion: false };
    }
    const deltas = {
      success: candidate.successRate - baseline.successRate,
      quality: candidate.quality - baseline.quality,
      cost: candidate.cost - baseline.cost,
      latency: candidate.latency - baseline.latency,
      regressions: candidate.regressions - baseline.regressions
    };
    const nonRegressing = deltas.success >= -1e-12 &&
      deltas.quality >= -1e-12 &&
      deltas.regressions <= 0;
    return {
      status: nonRegressing ? 'CANDIDATE_NON_REGRESSING' : 'CANDIDATE_REGRESSION',
      deltas,
      automaticPromotion: false
    };
  }
}

/**
 * Policy declaration only. The caller must actually create and verify an
 * isolated worktree; setting requiresIsolatedWorktree does not prove one exists.
 */
export class IsolatedSelfModificationGate {
  authorize(definition) {
    if (definition?.executionPolicy?.requiresIsolatedWorktree !== true) {
      return { status: 'REJECTED', reason: 'ISOLATED_WORKTREE_REQUIRED', productionMutation: false };
    }
    if (definition?.approvalPolicy?.autoMerge !== false) {
      return { status: 'REJECTED', reason: 'AUTO_MERGE_MUST_BE_DISABLED', productionMutation: false };
    }
    return {
      status: 'AUTHORIZED_FOR_CANDIDATE_ONLY',
      isolatedWorktreeRequired: true,
      productionMutation: false,
      automaticPromotion: false
    };
  }
}

/** Optional release-readiness evidence; does not approve or publish releases. */
export class QualificationGate {
  requiredCategories() { return [...REQUIRED_QUALIFICATIONS]; }

  assess(evidence = {}) {
    const valid = record => record?.passed === true &&
      nonempty(record.evidenceId) &&
      nonempty(record.summary) &&
      nonempty(record.recordedAt) &&
      Number.isFinite(Date.parse(record.recordedAt)) &&
      Array.isArray(record.checks) && record.checks.length > 0 &&
      record.checks.every(check => nonempty(check?.id) && check?.passed === true);
    const missing = REQUIRED_QUALIFICATIONS.filter(category => !valid(evidence?.[category]));
    return {
      status: missing.length ? 'NOT_QUALIFIED' : 'QUALIFIED_FOR_RELEASE_REVIEW',
      missing,
      featureFreeze: true,
      automaticRelease: false
    };
  }
}

/** Propose eligible execution routes without invoking any provider. */
export class QualityFloorPolicy {
  apply({ routeProposal, qualityFloor } = {}) {
    if (!unitInterval(qualityFloor) || !Array.isArray(routeProposal?.candidates)) {
      throw new TypeError('A candidate array and qualityFloor in [0,1] are required');
    }
    const candidates = routeProposal.candidates;
    const seen = new Set();
    for (const candidate of candidates) {
      if (!nonempty(candidate?.routeId) || seen.has(candidate.routeId) ||
          (candidate.expectedSuccess != null && !unitInterval(candidate.expectedSuccess)) ||
          (candidate.marginalCost != null && !nonnegative(candidate.marginalCost))) {
        throw new TypeError('Invalid or duplicate route candidate');
      }
      seen.add(candidate.routeId);
    }
    const eligible = candidates.filter(row => unitInterval(row.expectedSuccess) &&
      row.expectedSuccess >= qualityFloor);
    if (!eligible.length) {
      return {
        status: 'QUALITY_FLOOR_UNMET', qualityFloor, automaticExecution: false,
        rejected: candidates.map(row => ({ routeId: row.routeId, expectedSuccess: row.expectedSuccess ?? null }))
      };
    }
    const chosen = eligible.find(row => row.routeId === routeProposal.selected?.routeId) ??
      [...eligible].sort((a, b) =>
        b.expectedSuccess - a.expectedSuccess ||
        (a.marginalCost ?? Infinity) - (b.marginalCost ?? Infinity) ||
        a.routeId.localeCompare(b.routeId))[0];
    return {
      status: 'QUALITY_FLOOR_MET', qualityFloor, selected: structuredClone(chosen),
      eligible: structuredClone(eligible), automaticExecution: false
    };
  }
}

/**
 * Integrated fail-closed review. A positive result means "eligible for human
 * review", not verified production safety, automatic merge or deployment.
 */
export class QualityGate {
  constructor() {
    this.policyGate = new IsolatedSelfModificationGate();
    this.evaluationGate = new AutomatedEvaluationGate();
    this.comparisonGate = new CandidateComparisonGate();
    this.qualificationGate = new QualificationGate();
  }

  review({ definition, checks, baseline, candidate, qualification, requireQualification = false } = {}) {
    const policy = this.policyGate.authorize(definition);
    const evaluations = this.evaluationGate.evaluate(checks);
    const comparison = this.comparisonGate.compare({ baseline, candidate });
    const qualificationResult = requireQualification ? this.qualificationGate.assess(qualification) : null;
    const status =
      policy.status !== 'AUTHORIZED_FOR_CANDIDATE_ONLY' ? 'REJECTED_POLICY' :
      evaluations.status !== 'ACCEPTED_FOR_COMPARISON' ? 'REJECTED_CHECKS' :
      comparison.status !== 'CANDIDATE_NON_REGRESSING' ? comparison.status :
      qualificationResult && qualificationResult.status !== 'QUALIFIED_FOR_RELEASE_REVIEW' ?
        'NOT_QUALIFIED' : 'READY_FOR_HUMAN_REVIEW';
    return {
      status, policy, evaluations, comparison, qualification: qualificationResult,
      automaticPromotion: false, automaticMerge: false, productionMutation: false,
      requiresHumanReview: true
    };
  }
}
