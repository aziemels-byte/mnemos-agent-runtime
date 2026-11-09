import { QualityGate, REQUIRED_EVALUATIONS } from '../src/index.mjs';

// Synthetic evidence only. Actual CI receipts must come from a trusted runner.
const checks = Object.fromEntries(REQUIRED_EVALUATIONS.map(name => [
  name, { passed: true, completed: true, exitCode: 0, evidenceId: 'example-' + name }
]));
const baseline = { sampleCount: 20, successRate: 0.9, quality: 0.85, cost: 1, latency: 10, regressions: 0 };
const candidate = { ...baseline, quality: 0.9 };
const result = new QualityGate().review({
  definition: {
    executionPolicy: { requiresIsolatedWorktree: true },
    approvalPolicy: { autoMerge: false }
  },
  checks, baseline, candidate
});
if (result.status !== 'READY_FOR_HUMAN_REVIEW' || result.automaticMerge !== false) {
  throw new Error('Quality gate unexpectedly rejected the synthetic candidate');
}
console.log(JSON.stringify({
  status: result.status, verifiedChecks: result.evaluations.required.length,
  automaticMerge: result.automaticMerge, productionMutation: result.productionMutation
}));
