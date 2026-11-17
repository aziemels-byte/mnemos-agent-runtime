import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import { FileIdempotencyRegistry, FilePhaseJobRegistry, FileRateLimiter, ProviderCaller } from '../src/index.mjs';
import { temporaryDirectory, until } from './helpers.mjs';

// A provider on this host that answers each request as scripted: { status, headers }, 'reset'
// (closes the connection) or 'hang' (never answers); the last entry repeats. It records when
// each request arrived: that is what actually reached the provider.
async function provider(t, script) {
  const arrivals = [];
  const arrivedAt = []; // the same, in Date.now() terms (the limiter's clock)
  const server = http.createServer((request, response) => {
    const step = script[Math.min(arrivals.length, script.length - 1)];
    arrivals.push(performance.now());
    arrivedAt.push(Date.now());
    if (step === 'reset') return request.socket.destroy();
    if (step === 'hang') return;
    response.writeHead(step.status, step.headers ?? {});
    response.end(step.body ?? 'ok');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { server.closeAllConnections(); server.close(); });
  return { url: `http://127.0.0.1:${server.address().port}/`, arrivals, arrivedAt };
}

// A port nothing listens on: connections are refused, so the request is never sent.
async function closedPort() {
  const server = http.createServer();
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  await new Promise(resolve => server.close(resolve));
  return `http://127.0.0.1:${port}/`;
}

// How a caller turns fetch into an operation: a non-2xx response is an error with its status and headers.
const fetchText = (url, signal) => fetch(url, { signal }).then(async response => {
  if (!response.ok) throw Object.assign(new Error(`HTTP ${response.status}`), { status: response.status, headers: response.headers });
  return response.text();
});
const caller = options => new ProviderCaller({ baseDelayMs: 20, maxDelayMs: 2_000, random: () => 0.999, ...options });

test('a refused connection is retried with backoff until the provider answers', async t => {
  const [down, up] = [await closedPort(), await provider(t, [{ status: 200, body: 'answer' }])];
  const outcome = await caller().call({ provider: 'example', operation: ({ attempt, signal }) => fetchText(attempt < 3 ? down : up.url, signal) });
  assert.deepEqual([outcome.status, outcome.result, outcome.attempts.length], ['SUCCEEDED', 'answer', 3]);
  assert.deepEqual(outcome.attempts.slice(0, 2).map(attempt => [attempt.requestOutcome, attempt.delayMs]), [['NOT_SENT', 19], ['NOT_SENT', 39]]);
  assert.equal(up.arrivals.length, 1);
});

test('an ambiguous failure is never repeated unless the operation is idempotent', async t => {
  for (const step of ['reset', { status: 503 }]) {
    const once = await provider(t, [step]);
    const uncertain = await caller().call({ provider: 'example', operation: ({ signal }) => fetchText(once.url, signal) });
    assert.deepEqual([uncertain.status, uncertain.reconciliationRequired, uncertain.failure.requestOutcome], ['UNCERTAIN', true, 'AMBIGUOUS']);
    assert.equal(once.arrivals.length, 1, 'the request may have had its effect: it is not sent again');
    const repeated = await provider(t, [step]);
    const failed = await caller().call({ provider: 'example', idempotent: true, operation: ({ signal }) => fetchText(repeated.url, signal) });
    assert.deepEqual([failed.status, failed.effectAbsent, repeated.arrivals.length], ['FAILED', false, 3]);
  }
});

test('Retry-After is honoured: the next attempt waits at least that long, and so does every other caller', async t => {
  const state = await temporaryDirectory(t);
  const limited = await provider(t, [{ status: 429, headers: { 'Retry-After': '1' } }, { status: 200, body: 'answer' }]);
  const limiter = new FileRateLimiter({ directory: state });
  const first = caller({ limiter });
  const call = first.call({ provider: 'example', operation: ({ signal }) => fetchText(limited.url, signal) });
  // Another caller, with its own limiter on the same state (as in another process), starts during the cooldown.
  const { cooldownUntil } = await until(async () => {
    const current = await limiter.state('example');
    return current.cooldownUntil > Date.now() && current;
  }, 'the cooldown to be recorded');
  const other = await provider(t, [{ status: 200 }]);
  const second = caller({ limiter: new FileRateLimiter({ directory: state }) })
    .call({ provider: 'example', operation: ({ signal }) => fetchText(other.url, signal) });
  const outcome = await call;
  assert.deepEqual([outcome.status, outcome.attempts[0].kind, outcome.attempts[0].retryAfterMs], ['SUCCEEDED', 'quota_exhausted', 1000]);
  assert.ok(limited.arrivals[1] - limited.arrivals[0] >= 1000, `retried after ${limited.arrivals[1] - limited.arrivals[0]} ms`);
  assert.equal((await second).status, 'SUCCEEDED');
  assert.ok(other.arrivedAt[0] >= cooldownUntil, 'the other caller waited for the shared cooldown');
});

test('failures that cannot succeed, or would need a longer wait than allowed, end the call without effect', async t => {
  const unauthorized = await provider(t, [{ status: 401 }]);
  const denied = await caller().call({ provider: 'example', operation: ({ signal }) => fetchText(unauthorized.url, signal) });
  assert.deepEqual([denied.status, denied.effectAbsent, denied.failure.kind, unauthorized.arrivals.length],
    ['FAILED', true, 'authentication_required', 1]);
  const later = await provider(t, [{ status: 429, headers: { 'Retry-After': '60' } }]);
  const tooLong = await caller().call({ provider: 'example', operation: ({ signal }) => fetchText(later.url, signal) });
  assert.deepEqual([tooLong.status, tooLong.reason, tooLong.retryAfterMs, tooLong.effectAbsent, later.arrivals.length],
    ['FAILED', 'RETRY_AFTER_TOO_LONG', 60_000, true, 1]);
  const down = await closedPort();
  const exhausted = await caller({ maxAttempts: 4 }).call({ provider: 'example', operation: ({ signal }) => fetchText(down, signal) });
  assert.deepEqual([exhausted.status, exhausted.effectAbsent, exhausted.attempts.length], ['FAILED', true, 4]);
});

test('cancellation stops a wait at once and never starts another attempt; a request in flight stays uncertain', async t => {
  const down = await closedPort();
  const controller = new AbortController();
  let calls = 0;
  const started = performance.now();
  setTimeout(() => controller.abort(), 100);
  const waiting = await caller({ baseDelayMs: 5_000, maxDelayMs: 10_000 }).call({ provider: 'example', signal: controller.signal,
    operation: ({ signal }) => { calls++; return fetchText(down, signal); } });
  assert.deepEqual([waiting.status, waiting.effectAbsent, calls], ['CANCELLED', true, 1]);
  assert.ok(performance.now() - started < 4_000, 'the 5 s backoff wait ended at the abort');
  const never = await caller().call({ provider: 'example', signal: AbortSignal.abort(), operation: () => { calls++; } });
  assert.deepEqual([never.status, never.effectAbsent, calls], ['CANCELLED', true, 1]);
  const hanging = await provider(t, ['hang']);
  const inFlight = await caller().call({ provider: 'example', signal: AbortSignal.timeout(200), operation: ({ signal }) => fetchText(hanging.url, signal) });
  assert.deepEqual([inFlight.status, inFlight.reconciliationRequired, hanging.arrivals.length], ['UNCERTAIN', true, 1]);
});

test('with an idempotency key, an uncertain call stays UNKNOWN until reconciled, and a completed call is replayed', async t => {
  const effects = new FileIdempotencyRegistry({ directory: await temporaryDirectory(t) });
  const keyed = caller({ effects });
  const flaky = await provider(t, ['reset']);
  const request = { prompt: 'summarize report 7' };
  const call = url => keyed.call({ provider: 'example', key: 'summary:7', request, operation: ({ signal }) => fetchText(url, signal) });
  assert.equal((await call(flaky.url)).status, 'UNCERTAIN');
  assert.equal((await effects.inspect('summary:7')).state, 'UNKNOWN');
  const healthy = await provider(t, [{ status: 200, body: 'summary' }]);
  assert.equal((await call(healthy.url)).status, 'UNCERTAIN', 'never run again while its outcome is unknown');
  assert.equal(healthy.arrivals.length, 0);
  await effects.reconcile({ key: 'summary:7', request, disposition: 'COMPLETED', result: 'summary (from the provider log)',
    evidence: { authoritative: true, source: 'provider-log:request/7' } });
  assert.deepEqual(await call(healthy.url), { status: 'SUCCEEDED', result: 'summary (from the provider log)', replayed: true, attempts: [] });
  assert.equal(healthy.arrivals.length, 0);

  // A call whose every attempt had no effect is recorded as such, and may be run again on request.
  const down = await closedPort();
  const other = { provider: 'example', key: 'summary:8', request: { prompt: 'summarize report 8' } };
  const failed = await keyed.call({ ...other, operation: ({ signal }) => fetchText(down, signal) });
  assert.deepEqual([failed.status, failed.effectAbsent], ['FAILED', true]);
  assert.deepEqual([(await effects.inspect('summary:8')).state, (await effects.inspect('summary:8')).effectAbsent], ['FAILED', true]);
  assert.equal((await keyed.call({ ...other, operation: ({ signal }) => fetchText(healthy.url, signal) })).status, 'FAILED');
  const retried = await keyed.call({ ...other, retryFailed: true, operation: ({ signal }) => fetchText(healthy.url, signal) });
  assert.deepEqual([retried.status, retried.result, healthy.arrivals.length], ['SUCCEEDED', 'summary', 1]);
  assert.equal((await keyed.call({ ...other, operation: () => { throw new Error('must not run'); } })).replayed, true);
});

test('the rate limiter allows at most its limit per interval, shared by every limiter on the same state', async t => {
  const state = await temporaryDirectory(t);
  const limits = { example: { requests: 2, perMs: 400 } };
  const limiters = [new FileRateLimiter({ directory: state, limits }), new FileRateLimiter({ directory: state, limits })];
  const started = performance.now();
  for (let index = 0; index < 5; index++) await limiters[index % 2].acquire('example');
  // 2 at once, then one every 200 ms: the fifth after at least 600 ms.
  assert.ok(performance.now() - started >= 590, `5 slots in ${Math.round(performance.now() - started)} ms`);
  // A bucket that cannot refill during the test (1 per minute): refusing a long wait, and an aborted wait.
  const slow = new FileRateLimiter({ directory: state, limits: { slow: { requests: 1, perMs: 60_000 } } });
  await slow.acquire('slow');
  await assert.rejects(slow.acquire('slow', { maxWaitMs: 10 }), { code: 'RATE_LIMIT_WAIT_EXCEEDED' });
  await assert.rejects(slow.acquire('slow', { signal: AbortSignal.timeout(20) }), { name: 'TimeoutError' });
  const unlimited = await new FileRateLimiter({ directory: state }).acquire('unlimited'); // no limit: no wait for a slot
  assert.ok(unlimited.waitedMs < 200, `waited ${unlimited.waitedMs} ms (the state update only)`);
  await writeFile(path.join(state, 'rate-limits.json'), '{"schema":');
  await assert.rejects(limiters[0].acquire('example'), { code: 'RATE_LIMIT_STATE_INVALID' });
});

test('options are validated', async t => {
  const directory = await temporaryDirectory(t);
  assert.throws(() => new FileRateLimiter({ directory, limits: { example: { requests: 0, perMs: 1000 } } }), /requests and perMs/);
  assert.throws(() => new FileRateLimiter({ directory, limits: { 'bad name': { requests: 1, perMs: 1 } } }), /provider/);
  assert.throws(() => new ProviderCaller({ maxAttempts: 0 }), /maxAttempts/);
  assert.throws(() => new ProviderCaller({ limiter: {} }), /limiter/);
  const calls = new ProviderCaller();
  await assert.rejects(calls.call({ provider: 'example' }), /operation/);
  await assert.rejects(calls.call({ provider: 'example', operation: () => 1, key: 'k' }), /idempotency registry/);
  await assert.rejects(calls.call({ provider: 'example', operation: () => 1, idempotent: 'yes' }), /idempotent/);
});

test('inside a job: an uncertain call makes the job UNKNOWN, and a resumed attempt does not repeat it', async t => {
  const directory = await temporaryDirectory(t);
  const effects = new FileIdempotencyRegistry({ directory });
  const calls = caller({ effects });
  const flaky = await provider(t, ['reset', { status: 200, body: 'summary' }]);
  // The executor calls the provider with the job's effect key (the same in every attempt) and its signal.
  const jobs = new FilePhaseJobRegistry({ directory, execute: async ({ effectKey, signal }) => {
    const outcome = await calls.call({ provider: 'example', key: effectKey('summary'), request: { report: 7 }, signal,
      operation: ({ signal: attemptSignal }) => fetchText(flaky.url, attemptSignal) });
    if (outcome.status === 'SUCCEEDED') return { status: 'COMPLETED', summary: outcome.result };
    return { status: outcome.status === 'UNCERTAIN' ? 'UNKNOWN' : 'FAILED', effectAbsent: outcome.effectAbsent === true };
  } });
  await jobs.start({ jobId: 'summarize', objective: 'Summarize report 7', resumable: true });
  const job = await jobs.wait('summarize');
  assert.deepEqual([job.status, job.reason, job.reconciliationRequired], ['UNKNOWN', 'EXECUTOR_REPORTED_UNKNOWN', true]);
  assert.equal((await jobs.history('summarize')).events.at(-1).status, 'UNKNOWN');
  await jobs.resume({ jobId: 'summarize', expectedSequence: job.sequence, successorJobId: 'summarize_2', explicitAuthorization: true });
  assert.equal((await jobs.wait('summarize_2')).status, 'UNKNOWN', 'the key is still UNKNOWN: the call is not repeated');
  assert.equal(flaky.arrivals.length, 1);
});
