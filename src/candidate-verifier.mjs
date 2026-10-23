/** Validate adapter-supplied repository paths without normalizing away traversal. */
function repositoryPath(value) {
  if (typeof value !== 'string' || !value || value.includes('\0')) {
    throw new TypeError('Repository path must be a non-empty string');
  }
  if (value.includes('\\')) throw new TypeError('Repository paths must use forward slashes');
  const path = value;
  if (/^[A-Za-z]:/.test(path) || path.startsWith('/')) {
    throw new TypeError('Repository path must be relative');
  }
  if (path.split('/').some(part => !part || part === '.' || part === '..')) {
    throw new TypeError('Repository path contains an invalid segment');
  }
  return path;
}

function cancellation(phase, confirmed) {
  return {
    status: confirmed ? 'CANCELLED' : 'UNKNOWN',
    passed: false,
    reason: confirmed ? 'verification_cancelled' : 'cancellation_unconfirmed',
    phase,
    cancellationConfirmed: confirmed,
    reconciliationRequired: !confirmed
  };
}

/**
 * Check a candidate before invoking an expensive, caller-owned regression run.
 * The workspace adapter must report every changed path, including untracked files.
 */
export function createCandidateVerifier({ runRegression } = {}) {
  if (typeof runRegression !== 'function') {
    throw new TypeError('runRegression is required');
  }

  return async function verify({ workspace, allowedFiles, signal, onPhase } = {}) {
    if (typeof workspace?.diff !== 'function' ||
        typeof workspace?.changedFiles !== 'function') {
      throw new TypeError('A diff and changedFiles workspace adapter is required');
    }
    if (!Array.isArray(allowedFiles) || allowedFiles.length === 0) {
      return { status: 'REJECTED', passed: false, reason: 'allowed_scope_missing' };
    }
    const allowed = new Set(allowedFiles.map(repositoryPath));
    onPhase?.('candidate_preflight');
    if (signal?.aborted) return cancellation('candidate_preflight', true);

    const [diff, paths] = await Promise.all([workspace.diff(), workspace.changedFiles()]);
    if (!Array.isArray(paths)) throw new TypeError('changedFiles must return an array');
    const changedFiles = [...new Set(paths.map(repositoryPath))];
    if (signal?.aborted) return cancellation('candidate_preflight', true);

    const reason = changedFiles.length === 0 ? 'changed_files_missing'
      : changedFiles.some(file => !allowed.has(file)) ? 'scope_violation'
        : typeof diff !== 'string' || !diff.trim() ? 'diff_missing' : null;
    if (reason) return { status: 'REJECTED', passed: false, reason, changedFiles };

    onPhase?.('full_regression');
    if (signal?.aborted) return cancellation('full_regression', true);
    const result = await runRegression({ workspace, signal });
    if (result?.cancelled === true || signal?.aborted) {
      return cancellation('full_regression',
        result?.cancelled === true && result?.termination?.confirmed === true);
    }
    const passed = result?.passed === true && result?.exitCode === 0;
    return {
      status: passed ? 'VERIFIED' : 'REJECTED',
      passed,
      reason: passed ? 'regression_passed' : 'regression_failed',
      changedFiles,
      exitCode: result?.exitCode ?? null
    };
  };
}
