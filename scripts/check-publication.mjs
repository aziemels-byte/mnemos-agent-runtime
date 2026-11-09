import { lstat, readFile, readdir } from 'node:fs/promises';
import { isBuiltin } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const allowed = new Set([
  '.gitattributes', '.gitignore', '.github/workflows/ci.yml',
  'LICENSE', 'README.md', 'PROVENANCE.md', 'SECURITY.md', 'package.json', 'package-lock.json',
  'src/index.mjs', 'src/candidate-verifier.mjs', 'src/phase-job-registry.mjs',
  'src/provider-failure.mjs', 'src/secret-filter.mjs', 'src/checkpoint-recovery.mjs',
  'src/idempotency-registry.mjs', 'src/resource-lease-manager.mjs',
  'test/helpers.mjs', 'test/candidate-verifier.test.mjs', 'test/phase-job-registry.test.mjs',
  'test/provider-failure.test.mjs', 'test/checkpoint-recovery.test.mjs', 'test/examples.test.mjs',
  'test/idempotency-registry.test.mjs', 'test/resource-lease-manager.test.mjs',
  'examples/run.mjs', 'examples/verify-candidate.mjs', 'examples/recoverable-task.mjs',
  'examples/lease-and-idempotency.mjs',
  'scripts/check-publication.mjs'
]);
const problems = [];
const found = new Set();
let bytes = 0;

const privatePatterns = [
  ['machine profile path', /[A-Za-z]:[\\/]{1,2}Users[\\/]/i],
  ['private workspace path', /\b[A-Za-z]:[\\/]{1,2}(?:Projects|MNEMOS|Codex-storage)[\\/]/i],
  ['credential-looking token', /\b(?:gh[pousr]_|github_pat_|sk-(?:proj-)?)[A-Za-z0-9_-]{20,}/],
  ['private key payload', /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----\s+[A-Za-z0-9+/=]{20,}/]
];

async function visit(directory, relative = '') {
  for (const name of await readdir(directory)) {
    if (!relative && name === '.git') continue;
    const file = path.join(directory, name);
    const entry = relative ? `${relative}/${name}` : name;
    const stat = await lstat(file);
    if (stat.isSymbolicLink()) { problems.push(`${entry}: symbolic link`); continue; }
    if (stat.isDirectory()) { await visit(file, entry); continue; }
    if (!stat.isFile() || !allowed.has(entry)) { problems.push(`${entry}: not in publication allowlist`); continue; }
    found.add(entry);
    bytes += stat.size;
    if (stat.size > 96 * 1024) problems.push(`${entry}: unexpectedly large file`);
    const text = await readFile(file, 'utf8');
    if (text.includes('\0')) problems.push(`${entry}: binary data`);
    for (const [label, pattern] of privatePatterns) {
      if (pattern.test(text)) problems.push(`${entry}: ${label}`);
    }
    for (const match of text.matchAll(/\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/g)) {
      if (!match[0].endsWith('.invalid')) problems.push(`${entry}: non-fixture email address`);
    }
    if (entry.endsWith('.mjs')) {
      const specifiers = [...text.matchAll(/(?:\bfrom\s*|\bimport\s*)['"]([^'"]+)['"]/g)].map(match => match[1]);
      for (const specifier of specifiers) {
        if (specifier.startsWith('node:') && isBuiltin(specifier)) continue;
        if (!specifier.startsWith('.')) { problems.push(`${entry}: external import`); continue; }
        const resolved = path.resolve(path.dirname(file), specifier);
        const relativeImport = path.relative(root, resolved);
        if (relativeImport.startsWith('..') || path.isAbsolute(relativeImport)) {
          problems.push(`${entry}: import escapes repository`); continue;
        }
        if (!(await lstat(resolved)).isFile()) problems.push(`${entry}: invalid local import`);
      }
    }
  }
}

await visit(root);
for (const file of allowed) if (!found.has(file)) problems.push(`${file}: missing from publication`);
const manifest = JSON.parse(await readFile(path.join(root, 'package.json'), 'utf8'));
if (Object.keys(manifest.dependencies ?? {}).length || Object.keys(manifest.devDependencies ?? {}).length) {
  problems.push('External dependencies require a new review');
}
if (problems.length) {
  console.error(JSON.stringify({ status: 'FAIL', problems }, null, 2));
  process.exitCode = 1;
} else {
  console.log(JSON.stringify({ status: 'PASS', files: found.size, bytes, externalDependencies: 0 }));
}
