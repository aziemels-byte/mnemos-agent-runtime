// Full verification: tests, publication gate and, once it exists, the
// verify:reliability script. Runs from any working directory: every path is
// resolved from this file, never from the caller's directory (a wrong directory
// used to match no test files, and `node --test` then passes with zero tests).
//
//   node scripts/verify.mjs      (or: npm run verify)
//
// Tests run one file at a time. The full test log is written to
// <tmp>/mnemos-test.log. Exit code 0 only if every step passed and at least one
// test ran.
import { spawn } from 'node:child_process';
import { createWriteStream } from 'node:fs';
import { readdir, readFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const logFile = path.join(os.tmpdir(), 'mnemos-test.log');

// Run a command in the repository root; resolve to its exit code (1 if it cannot start).
function run(command, args, { output } = {}) {
  return new Promise(resolve => {
    const child = spawn(command, args, { cwd: root, stdio: ['ignore', output ? 'pipe' : 'inherit', output ? 'pipe' : 'inherit'],
      windowsHide: true, shell: process.platform === 'win32' && command === 'npm' });
    if (output) { child.stdout.pipe(output, { end: false }); child.stderr.pipe(output, { end: false }); }
    child.once('error', error => { console.error(`${command}: ${error.message}`); resolve(1); });
    child.once('close', code => resolve(code ?? 1));
  });
}

async function tests() {
  const files = (await readdir(path.join(root, 'test'))).filter(name => name.endsWith('.test.mjs')).sort()
    .map(name => path.join('test', name));
  if (files.length === 0) {
    console.error(`No test files found in ${path.join(root, 'test')}`);
    return 1;
  }
  const log = createWriteStream(logFile);
  const code = await run(process.execPath, ['--test', '--test-concurrency=1', '--test-timeout=120000', ...files],
    { output: log });
  await new Promise(resolve => log.end(resolve));
  const text = await readFile(logFile, 'utf8');
  const count = name => Number(text.match(new RegExp(`^# ${name} (\\d+)$`, 'm'))?.[1] ?? NaN);
  const summary = Object.fromEntries(['tests', 'pass', 'fail', 'cancelled', 'skipped', 'todo'].map(name => [name, count(name)]));
  const failures = text.split('\n').filter(line => /^not ok \d+ - /.test(line) && !/ # (TODO|SKIP)\b/.test(line));
  console.log(`Tests (${files.length} files): ${JSON.stringify(summary)}; log: ${logFile}`);
  for (const line of failures) console.log(`  ${line}`);
  const failed = code !== 0 || !(summary.tests > 0) || summary.fail > 0 || summary.cancelled > 0;
  if (failed) console.log(text.trimEnd().split('\n').slice(-40).join('\n')); // the end of the log, with the error details
  if (!(summary.tests > 0)) console.error('No tests ran: this is a failure, not a pass.');
  return failed ? code || 1 : 0;
}

async function reliability() {
  const manifest = JSON.parse(await readFile(path.join(root, 'package.json'), 'utf8'));
  if (!manifest.scripts?.['verify:reliability']) return null;
  return run('npm', ['run', 'verify:reliability']);
}

const testExit = await tests();
console.log(`TEST_EXIT=${testExit}`);
const publicationExit = await run(process.execPath, [path.join('scripts', 'check-publication.mjs')]);
console.log(`PUBLICATION_EXIT=${publicationExit}`);
const reliabilityExit = await reliability();
console.log(reliabilityExit === null ? 'RELIABILITY_EXIT=skipped (no verify:reliability script yet)' : `RELIABILITY_EXIT=${reliabilityExit}`);
const failed = testExit !== 0 || publicationExit !== 0 || (reliabilityExit ?? 0) !== 0;
console.log(`VERIFY_EXIT=${failed ? 1 : 0}`);
process.exitCode = failed ? 1 : 0;
