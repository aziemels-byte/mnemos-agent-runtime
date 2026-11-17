// Reliability verification: the checks that found real failures in this
// repository, repeated so that rare failures show up.
//
//   node scripts/verify-reliability.mjs      (or: npm run verify:reliability)
//
// 1. The process-tree termination tests that caught escapes (a process of a
//    stopped tree still running after a confirmed receipt), repeated under CPU
//    load and constant process start-up. Linux only, like those tests.
// 2. Full test suites running side by side, which exposed timing races.
//
// Every run must execute exactly the expected tests. The full output of every
// failing run is kept in <tmp>/mnemos-reliability-<time>/. Exit code 0 only if
// every run passed. Runs from any working directory.
import { spawn } from 'node:child_process';
import { mkdir, readdir, writeFile } from 'node:fs/promises';
import { availableParallelism, tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSupervised } from '../src/process-tree.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const TERMINATION_TESTS = [
  'a process started by a SIGTERM handler during termination is found and stopped',
  'processes started while the tree is being terminated cannot escape: the tree is frozen first'
];
const TERMINATION_RUNS = 30;
const TERMINATION_WORKERS = 3;
const SUITE_ROUNDS = 2;
const SUITES_SIDE_BY_SIDE = 2;

const escapeRegExp = text => text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
let artifacts = null;
async function keep(name, output) {
  artifacts ??= path.join(tmpdir(), `mnemos-reliability-${new Date().toISOString().replace(/[:.]/g, '-')}`);
  await mkdir(artifacts, { recursive: true });
  await writeFile(path.join(artifacts, name), output);
}

// Run `node --test` in the repository root; resolve to its exit code, output and summary counts.
function nodeTest(args) {
  return new Promise(resolve => {
    const child = spawn(process.execPath, ['--test', '--test-timeout=120000', ...args],
      { cwd: root, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
    let output = '';
    child.stdout.on('data', chunk => { output += chunk; });
    child.stderr.on('data', chunk => { output += chunk; });
    child.once('error', error => resolve({ code: 1, output: String(error), counts: {} }));
    child.once('close', code => {
      const count = name => Number(output.match(new RegExp(`^# ${name} (\\d+)$`, 'm'))?.[1] ?? NaN);
      resolve({ code: code ?? 1, output, counts: { tests: count('tests'), pass: count('pass'), fail: count('fail'), skipped: count('skipped') } });
    });
  });
}

// A run passes only if it exited 0, ran exactly `expected` tests (when given) and none failed.
const passed = ({ code, counts }, expected) => code === 0 && counts.fail === 0 && counts.tests > 0 &&
  (expected === undefined || (counts.tests === expected && counts.pass === expected));

// CPU load and process start-up churn, as supervised trees: they never outlive this script.
async function startLoad() {
  const busy = Math.max(1, availableParallelism() - 1);
  const trees = [];
  for (let index = 0; index < busy; index++) trees.push(await spawnSupervised(process.execPath, ['-e', 'for (;;) {}']));
  trees.push(await spawnSupervised(process.execPath, ['-e', "const { spawnSync } = require('node:child_process'); for (;;) spawnSync('/bin/true');"]));
  return { description: `${busy} busy loops and a process start-up loop`, stop: () => Promise.all(trees.map(tree => tree.terminate({ graceMs: 1000 }))) };
}

async function terminationRuns() {
  if (process.platform !== 'linux') return { skipped: 'Linux only, like the tests it repeats' };
  const load = await startLoad();
  const patterns = TERMINATION_TESTS.flatMap(name => ['--test-name-pattern', `^${escapeRegExp(name)}$`]);
  let next = 0;
  let failures = 0;
  try {
    await Promise.all(Array.from({ length: TERMINATION_WORKERS }, async () => {
      while (next < TERMINATION_RUNS) {
        const run = ++next;
        const result = await nodeTest(['--test-concurrency=1', ...patterns, path.join('test', 'process-tree.test.mjs')]);
        if (!passed(result, TERMINATION_TESTS.length)) {
          failures++;
          await keep(`termination-run-${run}.log`, result.output);
        }
      }
    }));
  } finally {
    await load.stop();
  }
  return { runs: TERMINATION_RUNS, failures, load: load.description };
}

async function suiteRounds() {
  const files = (await readdir(path.join(root, 'test'))).filter(name => name.endsWith('.test.mjs')).sort()
    .map(name => path.join('test', name));
  let failures = 0;
  for (let round = 1; round <= SUITE_ROUNDS; round++) {
    const results = await Promise.all(Array.from({ length: SUITES_SIDE_BY_SIDE }, () => nodeTest(files)));
    for (const [index, result] of results.entries()) {
      if (passed(result)) continue;
      failures++;
      await keep(`suite-round-${round}-${index + 1}.log`, result.output);
    }
  }
  return { runs: SUITE_ROUNDS * SUITES_SIDE_BY_SIDE, failures, sideBySide: SUITES_SIDE_BY_SIDE };
}

const started = Date.now();
const termination = await terminationRuns();
console.log(`Termination tests repeated under load: ${JSON.stringify(termination)}`);
const suites = await suiteRounds();
console.log(`Full suites side by side: ${JSON.stringify(suites)}`);
const failed = (termination.failures ?? 0) > 0 || suites.failures > 0;
if (artifacts) console.log(`Logs of the failing runs: ${artifacts}`);
console.log(`Reliability: ${failed ? 'FAILED' : 'passed'} in ${Math.round((Date.now() - started) / 1000)} s`);
process.exitCode = failed ? 1 : 0;
