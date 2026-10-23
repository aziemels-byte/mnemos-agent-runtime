// Best-effort diagnostic redaction, not a general data-loss-prevention system.
const RULES = [
  ['private_key', /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----[\s\S]*?-----END (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/g],
  ['openai_key', /\bsk-(?:proj-)?[A-Za-z0-9_-]{16,}\b/g],
  ['github_token', /\b(?:ghp|github_pat)_[A-Za-z0-9_]{16,}\b/g],
  ['bearer_token', /\bBearer\s+[A-Za-z0-9._~+\/-]+={0,2}/gi],
  ['credential_assignment', /["']?\b(?:api[_-]?key|access[_-]?token|client[_-]?secret|password|authorization)["']?\s*[:=]\s*(?:"[^"\n]*"|'[^'\n]*'|[^\s,;}"']+)/gi],
  ['url_credentials', /\b[a-z][a-z0-9+.-]*:\/\/[^\s/:@]+:[^\s/@]+@/gi]
];

export function redactSecrets(value) {
  let text = String(value ?? '');
  for (const [kind, pattern] of RULES) {
    pattern.lastIndex = 0;
    text = text.replace(pattern, `[REDACTED:${kind}]`);
  }
  return text;
}
