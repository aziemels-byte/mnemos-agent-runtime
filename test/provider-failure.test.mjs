import test from 'node:test';
import assert from 'node:assert/strict';
import { classifyProviderFailure, redactSecrets } from '../src/index.mjs';

for (const [input, kind, scope, retryable] of [
  [{ status: 401, message: 'Unauthorized' }, 'authentication_required', 'provider', false],
  [{ status: 429, message: 'Too many requests' }, 'quota_exhausted', 'provider', false],
  [{ status: 503 }, 'transport_unavailable', 'provider', true],
  [{ code: 'ECONNRESET' }, 'transport_unavailable', 'provider', true],
  [{ timedOut: true }, 'timeout', 'provider', true],
  [{ status: 404, message: 'model example not found' }, 'model_unavailable', 'model', false],
  [{ started: false, code: 'ENOENT' }, 'process_start_failed', 'provider', true],
  [{ message: 'plan does not include this feature' }, 'entitlement_unavailable', 'provider', false],
  [{ message: 'please upgrade the client' }, 'client_upgrade_required', 'provider', false]
]) {
  test(`classifies ${kind} (${JSON.stringify(input)})`, () => {
    const result = classifyProviderFailure(input);
    assert.deepEqual([result.kind, result.scope, result.retryable], [kind, scope, retryable]);
  });
}

test('authentication takes precedence over a generic timeout word', () => {
  assert.equal(classifyProviderFailure({ status: 401, message: 'timeout while unauthorized' }).kind, 'authentication_required');
});

test('parses Retry-After seconds and dates without assuming a reset time', () => {
  const now = Date.parse('2026-01-01T00:00:00Z');
  assert.equal(classifyProviderFailure({ headers: { 'Retry-After': '3' } }, { now }).retryAfterMs, 3000);
  assert.equal(classifyProviderFailure({ headers: new Headers({ 'retry-after': 'Thu, 01 Jan 2026 00:00:10 GMT' }) }, { now }).retryAfterMs, 10000);
  assert.equal(classifyProviderFailure({ status: 429 }, { now }).retryAfterMs, null);
  assert.equal(classifyProviderFailure({ headers: { 'Retry-After': 'unknown' } }, { now }).retryAfterMs, null);
});

test('redacts constructed fixture credentials in diagnostics before truncation', () => {
  const fake = 'fixture-' + 'x'.repeat(24);
  const result = classifyProviderFailure({ status: 401, message: `api_key="${fake}"` });
  assert.doesNotMatch(result.message, new RegExp(fake));
  assert.match(result.message, /REDACTED/);
  assert.equal(result.kind, 'authentication_required');
});

test('redacts short assignments, quoted spaces, bearer tokens and URL credentials', () => {
  for (const value of ['password=a', '"password": "example value"', 'Bearer example', 'https://example:fixture@example.invalid/path']) {
    assert.match(redactSecrets(value), /REDACTED/);
  }
});

test('diagnostics are bounded and result metadata is immutable', () => {
  const result = classifyProviderFailure({ message: 'x'.repeat(5000) });
  assert.equal(result.message.length, 2400);
  assert.ok(Object.isFrozen(result));
});

// Guard for the B4 fix: ordinary provider diagnostics stay readable.
test('diagnostics without credentials are left unchanged', () => {
  for (const text of ['max_tokens=512', 'input_tokens: 1200, output_tokens: 80', 'Authorization failed for this request',
    'password reset required', 'status 401 Unauthorized', 'token count: 512', 'secret rotation scheduled']) {
    assert.equal(redactSecrets(text), text);
  }
});

// Redaction of credentials in headers and assignments (B4), whether a request may
// have been processed (B5), and linear redaction time (B12).

// Constructed fixture, not a credential; it avoids real token prefixes.
const fixtureSecret = 'fixture' + 'Q7'.repeat(12);

test('redacts the whole credential of an authorization header, whatever its scheme', () => {
  for (const header of [`Authorization: Basic ${fixtureSecret}`, `authorization: Token ${fixtureSecret}`,
    `Proxy-Authorization: Basic ${fixtureSecret}`, `Authorization: Digest username="u", response="${fixtureSecret}"`]) {
    assert.doesNotMatch(redactSecrets(header), new RegExp(fixtureSecret), header);
  }
});

test('redacts common credential assignments and cookies', () => {
  for (const text of [`token=${fixtureSecret}`, `"refresh_token": "${fixtureSecret}"`, `secret=${fixtureSecret}`,
    `AWS_SECRET_ACCESS_KEY=${fixtureSecret}`, `Cookie: session=${fixtureSecret}`, `Set-Cookie: id=${fixtureSecret}; Path=/`]) {
    assert.doesNotMatch(redactSecrets(text), new RegExp(fixtureSecret), text);
  }
});

// requestOutcome (name proposed in the plan): whether the provider may have processed the request.
// NOT_SENT: never reached it. REJECTED: refused before processing. AMBIGUOUS: it may have been processed,
// so a retry of a non-idempotent request is unsafe without an idempotency key.
test('says whether a failed request may have been processed', () => {
  for (const [input, requestOutcome] of [
    [{ status: 401 }, 'REJECTED'], [{ status: 429 }, 'REJECTED'], [{ status: 404, message: 'model example not found' }, 'REJECTED'],
    [{ code: 'ECONNREFUSED' }, 'NOT_SENT'], [{ code: 'ENOTFOUND' }, 'NOT_SENT'], [{ code: 'EAI_AGAIN' }, 'NOT_SENT'],
    [{ started: false, code: 'ENOENT' }, 'NOT_SENT'],
    [{ timedOut: true }, 'AMBIGUOUS'], [{ code: 'ECONNRESET' }, 'AMBIGUOUS'], [{ code: 'EPIPE' }, 'AMBIGUOUS'],
    [{ status: 500 }, 'AMBIGUOUS'], [{ status: 502 }, 'AMBIGUOUS'], [{ status: 504 }, 'AMBIGUOUS'], [{ message: 'unexpected' }, 'AMBIGUOUS']
  ]) {
    assert.equal(classifyProviderFailure(input).requestOutcome, requestOutcome, JSON.stringify(input));
  }
});

test('a missing failure value is classified, not thrown', () => {
  for (const value of [null, undefined, 'timeout']) assert.doesNotThrow(() => classifyProviderFailure(value));
  assert.equal(classifyProviderFailure(null).requestOutcome, 'AMBIGUOUS');
});

test('a message alone never proves that a request was refused', () => {
  // An agent CLI can report a usage limit after it already acted.
  const cli = classifyProviderFailure({ stderr: 'Usage limit reached. Try again later.' });
  assert.deepEqual([cli.kind, cli.requestOutcome], ['quota_exhausted', 'AMBIGUOUS']);
  assert.equal(classifyProviderFailure({ status: 408 }).requestOutcome, 'AMBIGUOUS');
  assert.equal(classifyProviderFailure({ code: 'ECONNREFUSED', status: 502 }).requestOutcome, 'AMBIGUOUS', 'a response came back');
  assert.equal(classifyProviderFailure({ code: 'ENOENT', syscall: 'spawn provider-cli' }).requestOutcome, 'NOT_SENT');
  assert.equal(classifyProviderFailure({ code: 'ENOENT', syscall: 'open' }).requestOutcome, 'AMBIGUOUS');
});

test('redacts a private key of any type, including one that was cut off', () => {
  const body = 'MIIE' + 'Qk9'.repeat(20);
  for (const text of [`-----BEGIN RSA PRIVATE KEY-----\n${body}\n-----END RSA PRIVATE KEY-----`,
    `-----BEGIN ENCRYPTED PRIVATE KEY-----\n${body}\n-----END ENCRYPTED PRIVATE KEY-----`,
    `stderr: -----BEGIN PRIVATE KEY-----\n${body}\n(output truncated)`]) {
    assert.doesNotMatch(redactSecrets(text), new RegExp(body), text);
  }
  assert.equal(redactSecrets(`-----BEGIN OPENSSH PRIVATE KEY-----\n${body}\n-----END OPENSSH PRIVATE KEY-----\nnext line`),
    '[REDACTED:private_key]\nnext line');
});

test('redaction takes linear time on hostile input', () => {
  // Formerly quadratic: minutes for 1 MB of "a-a-a-..." and 20 s for repeated key headers.
  for (const unit of ['a-', 'a.', 'token-', 'a://', '-----BEGIN PRIVATE KEY-----']) {
    const text = unit.repeat(Math.ceil(1_000_000 / unit.length));
    const started = performance.now();
    redactSecrets(text);
    const elapsed = performance.now() - started;
    assert.ok(elapsed < 3_000, `${JSON.stringify(unit)}: ${elapsed.toFixed(0)} ms for 1 MB`);
  }
});
