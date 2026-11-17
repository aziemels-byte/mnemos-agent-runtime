import { redactSecrets } from './secret-filter.mjs';

// Diagnostics are redacted before they are truncated, so that truncation cannot
// cut a secret and leave its head unredacted.
const MAX_MESSAGE = 2400;
// Failures before a connection existed: the request never reached the provider.
const NOT_SENT_CODES = new Set(['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN']);

function statusCodeOf(value) {
  for (const candidate of [value.statusCode, value.status, value.response?.status, value.cause?.status]) {
    const code = Number(candidate);
    if (Number.isInteger(code) && code >= 100 && code <= 599) return code;
  }
  const match = String(value.message ?? value.error ?? '').match(/\b(?:status|http)\s*[:=]?\s*(\d{3})\b/i);
  return match ? Number(match[1]) : null;
}

function retryAfterMs(headers, now) {
  const raw = typeof headers?.get === 'function'
    ? headers.get('retry-after')
    : Object.entries(headers ?? {}).find(([key]) => key.toLowerCase() === 'retry-after')?.[1];
  if (raw == null) return null;
  const value = String(raw).trim();
  if (/^\d+(?:\.\d+)?$/.test(value)) {
    const milliseconds = Number(value) * 1000;
    return Number.isSafeInteger(milliseconds) ? milliseconds : null;
  }
  const time = Date.parse(value);
  return Number.isFinite(time) ? Math.max(0, time - now) : null;
}

/**
 * Whether the provider may have processed the failed request:
 * - NOT_SENT: it never reached the provider (no connection, or the local process never started).
 * - REJECTED: the provider answered with a client error (HTTP 4xx other than 408): refused before processing.
 * - AMBIGUOUS: anything else, including timeouts, dropped connections, 5xx and
 *   unrecognized failures. The request may have had its effect.
 * A message alone never proves a refusal: a CLI can report a usage limit after it already acted.
 */
function requestOutcomeOf(value, statusCode, code) {
  if (value.started === false) return 'NOT_SENT';
  if (statusCode === null && NOT_SENT_CODES.has(code)) return 'NOT_SENT';
  if (code === 'ENOENT' && statusCode === null && /^spawn\b/.test(String(value.syscall ?? ''))) return 'NOT_SENT';
  if (statusCode !== null && statusCode >= 400 && statusCode < 500 && statusCode !== 408) return 'REJECTED';
  return 'AMBIGUOUS';
}

/**
 * Classify a failed call; never retry, switch accounts or select another model.
 *
 * `retryable` says whether trying again may succeed, not whether it is safe:
 * only a request whose `requestOutcome` is NOT_SENT or REJECTED certainly had no
 * effect. Retry an AMBIGUOUS request only if it is idempotent, or through an
 * idempotency key (FileIdempotencyRegistry).
 */
export function classifyProviderFailure(value = {}, { now = Date.now() } = {}) {
  if (value === null || typeof value !== 'object') value = { message: typeof value === 'string' ? value : undefined };
  const statusCode = statusCodeOf(value);
  const diagnostics = [value.message, value.error, value.stdout, value.stderr].filter(item => typeof item === 'string').join('\n');
  const message = redactSecrets(diagnostics).trim().slice(0, MAX_MESSAGE);
  const code = String(value.code ?? value.cause?.code ?? '').toUpperCase();
  let kind = 'provider_unavailable';
  let scope = 'execution';
  let retryable = false;

  if (statusCode === 401 || /not authenticated|authentication (?:required|failed)|login required|unauthori[sz]ed/i.test(message)) {
    kind = 'authentication_required'; scope = 'provider';
  } else if (statusCode === 429 || /rate[ -]?limit|usage[ -]?limit|quota|too many requests/i.test(message)) {
    kind = 'quota_exhausted'; scope = 'provider';
  } else if (/requires? a newer version|please upgrade|unsupported client version/i.test(message)) {
    kind = 'client_upgrade_required'; scope = 'provider';
  } else if (/unsupported entitlement|not entitled|subscription required|plan does not include/i.test(message)) {
    kind = 'entitlement_unavailable'; scope = 'provider';
  } else if (/model .*not found|unknown model|model .*unavailable|unsupported model/i.test(message)) {
    kind = 'model_unavailable'; scope = 'model';
  } else if (value.timedOut === true || ['ETIMEDOUT', 'TIMEOUT'].includes(code) || /\btimeout|timed out/i.test(message)) {
    kind = 'timeout'; scope = 'provider'; retryable = true;
  } else if ([500, 502, 503, 504].includes(statusCode) ||
      // UND_ERR_SOCKET: Node's fetch (undici) when the connection closes mid-request (measured).
      ['ECONNREFUSED', 'ECONNRESET', 'ENOTFOUND', 'EAI_AGAIN', 'EPIPE', 'UND_ERR_SOCKET'].includes(code) ||
      /service unavailable|temporarily unavailable|connection (?:reset|refused)|network error/i.test(message)) {
    kind = 'transport_unavailable'; scope = 'provider'; retryable = true;
  } else if (value.started === false || code === 'ENOENT') {
    kind = 'process_start_failed'; scope = 'provider'; retryable = true;
  }
  return Object.freeze({
    kind, scope, retryable, requestOutcome: requestOutcomeOf(value, statusCode, code), statusCode, message,
    retryAfterMs: retryAfterMs(value.headers ?? value.response?.headers, now)
  });
}
