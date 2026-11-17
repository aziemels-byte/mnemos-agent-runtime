import { createHash, randomUUID } from 'node:crypto';
import { FilePhaseJobRegistry } from './file-phase-job-registry.mjs';
import { requireJobId } from './phase-job-registry.mjs';
import { redactSecrets } from './secret-filter.mjs';

const TEST_EVIDENCE_SCHEMA = 'mnemos.test-evidence.v1';
const CATEGORY = /^[a-z][a-z0-9_]{0,31}$/;
const TAIL_BYTES = 4 * 1024;
const MAX_TIMEOUT_MS = 24 * 3_600_000;
const STOP_WAIT_MS = 30_000;
// The summary `node --test` (TAP) prints at the end of its output.
const SUMMARY = /^# (tests|pass|fail|cancelled|skipped|todo) (\d+)$/gm;

/**
 * Checks run by the runtime, with their durable record as evidence.
 *
 *   const evidence = new TestEvidence({ directory });
 *   const receipt = await evidence.run({ category: 'unit', subject, command: 'node', args: ['--test'] });
 *   await new QualityGate({ evidence }).review({ subject, checks: { unit: receipt.evidenceId, ... }, ... });
 *
 * Each check is a FilePhaseJobRegistry job whose executor runs the command as a
 * supervised process tree (runProcess): it is recorded before it starts, its
 * processes are tracked, it is stopped on timeout, it appears in the job
 * history, and its record survives restarts. The record's result is the
 * evidence: { schema, category, subject, command, args, cwd, exitCode,
 * exitSignal, startedAt, finishedAt, output: { sha256, bytes, complete, tail },
 * summary }. `subject` names what was checked (for example a hash of the
 * candidate's diff), so that evidence cannot be reused for another candidate.
 * The evidence ID is the job ID; a job ID runs at most once.
 */
export class TestEvidence {
  #environments = new Map(); // kept in memory only: environments carry credentials

  constructor({ directory, lockTimeoutMs } = {}) {
    this.jobs = new FilePhaseJobRegistry({ directory, lockTimeoutMs, execute: context => this.#execute(context) });
  }

  /**
   * Run a check and resolve to its receipt (see receipt()). With timeoutMs
   * (default 10 minutes) the check is stopped when it runs longer; it then did
   * not pass. env (default: this process's) is passed to the command but never
   * recorded; NODE_TEST_CONTEXT is always left out, for under it a nested
   * `node --test` skips every file and exits 0. expectTestSummary (default
   * true): the check passes only if its output ends with a `node --test` (TAP)
   * summary showing at least one test; set it to false for a command that is
   * not a test runner.
   */
  async run({ category, subject, command, args = [], cwd, env, timeoutMs = 600_000, expectTestSummary = true, evidenceId, signal } = {}) {
    if (typeof category !== 'string' || !CATEGORY.test(category)) throw new TypeError('category must be 1 to 32 lower-case letters, digits or _');
    if (typeof subject !== 'string' || !subject.trim() || subject.length > 512) throw new TypeError('subject must name what is checked (1 to 512 characters)');
    if (typeof command !== 'string' || !command) throw new TypeError('command is required');
    if (!Array.isArray(args) || !args.every(arg => typeof arg === 'string')) throw new TypeError('args must be strings');
    if (cwd !== undefined && typeof cwd !== 'string') throw new TypeError('cwd must be a string');
    if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > MAX_TIMEOUT_MS) throw new TypeError('timeoutMs must be a positive integer');
    if (expectTestSummary !== true && expectTestSummary !== false) throw new TypeError('expectTestSummary must be a boolean');
    if (env !== undefined && (env === null || typeof env !== 'object')) throw new TypeError('env must be an object');
    if (signal !== undefined && !(signal instanceof AbortSignal)) throw new TypeError('signal must be an AbortSignal');
    const jobId = requireJobId(evidenceId ?? `check_${category}_${randomUUID().replaceAll('-', '')}`);
    const { NODE_TEST_CONTEXT, ...environment } = env ?? process.env;
    this.#environments.set(jobId, environment);
    try {
      await this.jobs.start({ jobId, objective: `Run the ${category} check`,
        input: { category, subject, command, args, cwd: cwd ?? null, expectTestSummary } });
      // An abort stops the check like a timeout does.
      const stop = () => { this.jobs.requestCancellation(jobId, { timeoutMs: 0 }).catch(() => {}); };
      if (signal?.aborted) stop();
      signal?.addEventListener('abort', stop, { once: true });
      let job;
      try { job = await this.jobs.wait(jobId, { timeoutMs }); }
      finally { signal?.removeEventListener('abort', stop); }
      if (job?.status === 'RUNNING') {
        await this.jobs.requestCancellation(jobId, { timeoutMs: STOP_WAIT_MS });
        job = await this.jobs.wait(jobId, { timeoutMs: STOP_WAIT_MS });
      }
      return receipt(job);
    } finally {
      this.#environments.delete(jobId);
    }
  }

  /**
   * A runRegression function for createCandidateVerifier that runs the
   * regression suite as a recorded check of category `regression`. Its subject
   * is `diff:sha256:<hash of the candidate's diff>`; the verifier's result
   * carries it with the evidence ID, for QualityGate.review().
   */
  regressionRunner({ command, args, cwd, env, timeoutMs, expectTestSummary } = {}) {
    return async ({ workspace, signal }) => {
      const subject = `diff:sha256:${createHash('sha256').update(String(await workspace.diff())).digest('hex')}`;
      const checked = await this.run({ category: 'regression', subject, command, args, cwd, env, timeoutMs, expectTestSummary, signal });
      const cancelled = ['CANCELLED', 'UNKNOWN'].includes(checked.status);
      return { passed: checked.passed, exitCode: checked.exitCode, evidenceId: checked.evidenceId, subject,
        ...(cancelled ? { cancelled: true, termination: { confirmed: checked.status === 'CANCELLED' } } : {}) };
    };
  }

  /**
   * Resolve evidence for a check from the job records: { verified, reason,
   * receipt }. verified is true only for a settled check of this category and
   * subject whose record carries its evidence; receipt.passed additionally
   * requires a clean exit, complete output, and (when the output has a test
   * summary) at least one test and no failure. Nothing supplied by the caller
   * except the evidence ID is trusted.
   */
  async verify({ category, subject, evidenceId } = {}) {
    if (typeof evidenceId !== 'string' || !evidenceId) return { verified: false, reason: 'EVIDENCE_MISSING', receipt: null };
    let job;
    try { job = await this.jobs.get(evidenceId); }
    catch (error) { return { verified: false, reason: error instanceof TypeError ? 'EVIDENCE_NOT_FOUND' : 'EVIDENCE_UNREADABLE', receipt: null }; }
    if (!job) return { verified: false, reason: 'EVIDENCE_NOT_FOUND', receipt: null };
    const result = job.result;
    if (result?.schema !== TEST_EVIDENCE_SCHEMA) return { verified: false, reason: 'EVIDENCE_NOT_A_CHECK', receipt: null };
    if (result.category !== category) return { verified: false, reason: 'EVIDENCE_CATEGORY_MISMATCH', receipt: null };
    if (result.subject !== subject) return { verified: false, reason: 'EVIDENCE_SUBJECT_MISMATCH', receipt: null };
    if (!['COMPLETED', 'FAILED'].includes(job.status)) return { verified: false, reason: 'EVIDENCE_NOT_SETTLED', receipt: receipt(job) };
    return { verified: true, reason: null, receipt: receipt(job) };
  }

  // The job's executor: run the command and describe what happened.
  async #execute({ jobId, input, runProcess, setPhase }) {
    setPhase('running');
    const hash = createHash('sha256');
    let bytes = 0;
    let tail = Buffer.alloc(0);
    const startedAt = new Date().toISOString();
    const outcome = await runProcess({ command: input.command, args: input.args, cwd: input.cwd ?? undefined,
      env: this.#environments.get(jobId), stdio: ['ignore', 'pipe', 'pipe'],
      onOutput: chunk => {
        hash.update(chunk);
        bytes += chunk.length;
        tail = Buffer.concat([tail, chunk]).subarray(-TAIL_BYTES);
      } });
    const text = outputTail(tail, bytes);
    return {
      // What decides the job's outcome, as runProcess reported it (a cancellation's receipt decides whether it is confirmed).
      // Results are stored as plain JSON only: absent values are left out, never undefined.
      status: outcome.status, ...(outcome.termination ? { termination: outcome.termination } : {}),
      ...(outcome.effectAbsent === undefined ? {} : { effectAbsent: outcome.effectAbsent }),
      // The evidence.
      schema: TEST_EVIDENCE_SCHEMA, category: input.category, subject: input.subject, command: input.command, args: input.args,
      cwd: input.cwd, expectTestSummary: input.expectTestSummary, exitCode: outcome.exitCode ?? null, exitSignal: outcome.exitSignal ?? null,
      spawnError: outcome.spawnError ?? null, startedAt, finishedAt: new Date().toISOString(),
      output: { sha256: hash.digest('hex'), bytes, complete: outcome.outputComplete === true, tail: text },
      summary: testSummary(text)
    };
  }
}

// The last bytes of the output, from a line start (a secret cut in half could escape
// redaction), with known credential patterns redacted.
function outputTail(tail, bytes) {
  let text = tail.toString('utf8');
  if (bytes > tail.length) text = text.slice(text.indexOf('\n') + 1);
  return redactSecrets(text);
}

// The `node --test` summary in the output's tail, or null when there is none.
function testSummary(text) {
  const summary = {};
  for (const [, name, count] of text.matchAll(SUMMARY)) summary[name] = Number(count);
  return Number.isInteger(summary.tests) ? summary : null;
}

/**
 * A check's receipt from its job record: { category, subject, evidenceId,
 * status, completed, passed, exitCode, summary, output: { sha256, bytes,
 * complete } }. completed: the check ran to an end (COMPLETED or FAILED).
 * passed: it exited 0 with all its output, and its test summary shows at least
 * one test and no failure. Without a summary it passes only if none was
 * expected. (`node --test` exits 0 when it finds no test at all, and when it
 * runs under another test process's NODE_TEST_CONTEXT.)
 */
function receipt(job) {
  const result = job.result?.schema === TEST_EVIDENCE_SCHEMA ? job.result : null;
  const summary = result?.summary ?? null;
  const completed = result !== null && ['COMPLETED', 'FAILED'].includes(job.status);
  const summaryShowsPass = summary !== null && summary.tests > 0 && (summary.fail ?? 0) === 0 && (summary.cancelled ?? 0) === 0;
  const passed = completed && job.status === 'COMPLETED' && result.exitCode === 0 && result.output.complete === true &&
    (summary === null ? result.expectTestSummary === false : summaryShowsPass);
  return { category: result?.category ?? null, subject: result?.subject ?? null, evidenceId: job.jobId, status: job.status,
    reason: job.reason, completed, passed, exitCode: result?.exitCode ?? null, summary,
    output: result ? { sha256: result.output.sha256, bytes: result.output.bytes, complete: result.output.complete } : null };
}
