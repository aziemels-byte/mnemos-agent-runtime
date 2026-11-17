// Best-effort diagnostic redaction, not a general data-loss-prevention system.

// Keys whose value is a credential, optionally after up to four prefix words
// (AWS_SECRET_ACCESS_KEY, x-goog-api-key). A key must be followed directly by
// ":" or "=", so "max_tokens=512" or "token count: 512" are left alone.
const CREDENTIAL_KEY = '(?:[a-z0-9]+[_-]){0,4}(?:api[_-]?key|(?:access|refresh|id|auth|session)[_-]?token|token|' +
  'client[_-]?secret|secret(?:[_-]?access)?[_-]?key|secret|password|passwd)';
// Headers whose whole value is a credential, whatever its scheme (Basic, Token,
// Digest ...) or structure (cookies): the value is redacted to the end of the line.
const CREDENTIAL_HEADER = '(?:proxy-)?authorization|(?:set-)?cookie';
const QUOTED = `"[^"\\n]*"|'[^'\\n]*'`;

const RULES = [
  // The body runs to the next "-----": its END line, or the end of the text when
  // the key was cut off (redacted too). Each BEGIN scans only up to the next
  // marker, so repeated markers take linear time.
  ['private_key', /-----BEGIN (?:[A-Z0-9]+ )?PRIVATE KEY-----(?:(?!-----)[\s\S])*(?:-----END (?:[A-Z0-9]+ )?PRIVATE KEY-----)?/g],
  ['openai_key', /\bsk-(?:proj-)?[A-Za-z0-9_-]{16,}\b/g],
  ['github_token', /\b(?:ghp|github_pat)_[A-Za-z0-9_]{16,}\b/g],
  ['bearer_token', /\bBearer\s+[A-Za-z0-9._~+\/-]+={0,2}/gi],
  ['credential_header', new RegExp(`["']?\\b(?:${CREDENTIAL_HEADER})["']?\\s*[:=]\\s*(?:${QUOTED}|[^\\n]+)`, 'gi')],
  ['credential_assignment', new RegExp(`["']?\\b${CREDENTIAL_KEY}["']?\\s*[:=]\\s*(?:${QUOTED}|[^\\s,;}"']+)`, 'gi')],
  // A scheme starts only where no scheme character precedes it: with \b alone,
  // text such as "a-a-a-..." made every word a new start, in quadratic time.
  ['url_credentials', /(?<![a-z0-9+.-])[a-z][a-z0-9+.-]*:\/\/[^\s/:@]+:[^\s/@]+@/gi]
];

export function redactSecrets(value) {
  let text = String(value ?? '');
  for (const [kind, pattern] of RULES) {
    pattern.lastIndex = 0;
    text = text.replace(pattern, `[REDACTED:${kind}]`);
  }
  return text;
}
