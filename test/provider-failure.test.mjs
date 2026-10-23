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
