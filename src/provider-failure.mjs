import { redactSecrets } from './secret-filter.mjs';

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

/** Classify a failed call; never retry, switch accounts or select another model. */
export function classifyProviderFailure(value = {}, { now = Date.now() } = {}) {
  const statusCode = statusCodeOf(value);
  const message = redactSecrets([value.message, value.error, value.stdout, value.stderr]
    .filter(item => typeof item === 'string').join('\n')).trim().slice(0, 2400);
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
      ['ECONNREFUSED', 'ECONNRESET', 'ENOTFOUND', 'EAI_AGAIN', 'EPIPE'].includes(code) ||
      /service unavailable|temporarily unavailable|connection (?:reset|refused)|network error/i.test(message)) {
    kind = 'transport_unavailable'; scope = 'provider'; retryable = true;
  } else if (value.started === false || code === 'ENOENT') {
    kind = 'process_start_failed'; scope = 'provider'; retryable = true;
  }
  return Object.freeze({
    kind, scope, retryable, statusCode, message,
    retryAfterMs: retryAfterMs(value.headers ?? value.response?.headers, now)
  });
}
