import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { identityState, processIdentity } from '../src/process-identity.mjs';
import { runSupervised, spawnSupervised } from '../src/process-tree.mjs';
import { FilePhaseJobRegistry, PhaseJobRegistry } from '../src/index.mjs';
import { FileJobStore } from '../src/job-store.mjs';
import { readFile, writeFile } from 'node:fs/promises';
import { promisify } from 'node:util';
import { hostname } from 'node:os';
import { temporaryDirectory, until, withTimeout } from './helpers.mjs';

const posix = process.platform !== 'win32';
const linux = process.platform === 'linux';
const forever = 'setInterval(() => {}, 1000);';
const ignoreTerm = "process.on('SIGTERM', () => {});";
const moduleUrl = new URL('../src/process-tree.mjs', import.meta.url).href;

// A command that starts a grandchild (optionally ignoring SIGTERM, optionally in
// its own session). The grandchild prints its PID once its handlers are installed.
const withGrandchild = ({ leaderIgnoresTerm = false, grandchildIgnoresTerm = false, escape = false } = {}) => `
  ${leaderIgnoresTerm ? ignoreTerm : ''}
  require('node:child_process').spawn(process.execPath,
    ['-e', ${JSON.stringify((grandchildIgnoresTerm ? ignoreTerm : '') + 'console.log(process.pid);' + forever)}],
    { stdio: ['ignore', 'inherit', 'ignore'], detached: ${escape} });
  ${forever}`;

// Reads newline-delimited output; every wait is bounded so a failure can never hang the run.
function lineReader(stream, label) {
  let text = '';
  const waiters = [];
  stream.setEncoding('utf8');
  stream.on('data', chunk => { text += chunk; flush(); });
  function flush() {
    while (waiters.length && text.includes('\n')) {
      const index = text.indexOf('\n');
      const line = text.slice(0, index);
      text = text.slice(index + 1);
      waiters.shift().resolve(line);
    }
  }
  return (timeoutMs = 10_000) => new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timed out waiting for output from ${label}`)), timeoutMs);
    waiters.push({ resolve: line => { clearTimeout(timer); resolve(line); } });
    flush();
  });
}

const kill = pid => { try { process.kill(pid, 'SIGKILL'); } catch {} };
const killGroup = pgid => { try { process.kill(-pgid, 'SIGKILL'); } catch {} };

async function startTree(t, source) {
  // stderr is not inherited: no test tree may hold the test runner's pipes.
  const tree = await spawnSupervised(process.execPath, ['-e', source], { stdio: ['ignore', 'pipe', 'ignore'] });
  t.after(() => tree.terminate({ graceMs: 0, killWaitMs: 1000 }).catch(() => {}));
  const grandchildPid = Number(await lineReader(tree.stdout, 'the tree')());
  // Cleanup never relies on the code under test: a broken terminate() must not leak processes.
  t.after(() => { for (const pid of [grandchildPid, tree.pid, tree.supervisor.pid]) kill(pid); if (posix) killGroup(tree.pid); });
  return { tree, grandchild: processIdentity(grandchildPid) };
}

// Linux: running processes that carry a tree's environment marker.
function markedProcesses(marker) {
  const needle = `${marker}=1`;
  const found = [];
  for (const name of readdirSync('/proc')) {
    if (!/^\d+$/.test(name)) continue;
    try {
      const stat = readFileSync(`/proc/${name}/stat`, 'utf8');
      if (['Z', 'X'].includes(stat[stat.lastIndexOf(')') + 2])) continue;
      if (readFileSync(`/proc/${name}/environ`, 'latin1').split('\0').includes(needle)) found.push(Number(name));
    } catch { /* exited, or another user's */ }
  }
  return found;
}

const gone = identity => identityState(identity) === 'DEAD';
// Linux: the parent PID of a running process (null once it is gone).
const parentOf = pid => { try { const stat = readFileSync(`/proc/${pid}/stat`, 'utf8'); return Number(stat.slice(stat.lastIndexOf(')') + 2).split(' ')[1]); } catch { return null; } };

// An owning process that starts a supervised tree, reports the PIDs and then
// runs `then` (by default: stays alive until killed).
function startOwner(t, { source, then = forever, beforeStart = '', detached = false }) {
  const owner = spawn(process.execPath, ['--input-type=module', '-e', `
    import { spawnSupervised } from ${JSON.stringify(moduleUrl)};
    const report = value => process.stdout.write(JSON.stringify(value) + '\\n');
    const tree = await spawnSupervised(process.execPath, ['-e', ${JSON.stringify(source)}], {
      stdio: ['ignore', 'pipe', 'inherit'], orphanGraceMs: 300,
      beforeStart: async ({ supervisor }) => { report({ supervisor: supervisor.pid }); ${beforeStart} } });
    tree.stdout.once('data', data => { report({ leader: tree.pid, grandchild: Number(String(data).trim()) }); ${then} });`],
  { stdio: ['ignore', 'pipe', 'pipe'], detached });
  const next = lineReader(owner.stdout, 'the owner');
  owner.output = async () => JSON.parse(await next());
  owner.stderrEnded = new Promise(resolve => { owner.stderr.on('end', resolve); owner.stderr.resume(); });
  const closed = new Promise(resolve => owner.once('close', resolve));
  owner.closed = () => withTimeout(closed, 10_000, 'the owner did not exit');
  t.after(() => { kill(owner.pid); if (detached) killGroup(owner.pid); });
  return owner;
}

async function ownerTree(t, owner) {
  const { supervisor } = await owner.output();
  const { leader, grandchild } = await owner.output();
  t.after(() => { for (const pid of [supervisor, leader, grandchild]) kill(pid); killGroup(leader); });
  return { supervisor: processIdentity(supervisor), leader: processIdentity(leader), grandchild: processIdentity(grandchild) };
}

test('terminating the group stops the leader and its grandchild, with a confirmed receipt', { skip: !posix }, async t => {
  const { tree, grandchild } = await startTree(t, withGrandchild());
  assert.equal(identityState(grandchild), process.platform === 'linux' ? 'ALIVE' : 'UNVERIFIED');
  const receipt = await tree.terminate({ graceMs: 5000 });
  assert.deepEqual([receipt.confirmed, receipt.method, receipt.coverage, receipt.signals, receipt.pgid],
    [true, 'process-group', linux ? 'process-group+descendants' : 'process-group', ['SIGTERM'], tree.pid]);
  assert.deepEqual(receipt.leaderExit, { code: null, signal: 'SIGTERM' });
  if (process.platform === 'linux') assert.deepEqual(receipt.survivors, []);
  assert.equal(identityState(grandchild), 'DEAD', 'the grandchild is stopped, not only the direct child');
  assert.equal(identityState(tree.identity), 'DEAD');
  await until(() => gone(tree.supervisor), 'the released supervisor to exit');
});

test('members that ignore SIGTERM are killed after the grace period', { skip: !posix }, async t => {
  const { tree, grandchild } = await startTree(t, withGrandchild({ leaderIgnoresTerm: true, grandchildIgnoresTerm: true }));
  const started = performance.now();
  const receipt = await tree.terminate({ graceMs: 300 });
  assert.ok(performance.now() - started >= 290, 'the grace period is honoured');
  assert.deepEqual([receipt.confirmed, receipt.signals], [true, ['SIGTERM', 'SIGKILL']]);
  assert.equal(receipt.leaderExit.signal, 'SIGKILL');
  assert.equal(identityState(grandchild), 'DEAD');
});

test('the receipt waits for every member, not only the leader', { skip: !posix }, async t => {
  const { tree, grandchild } = await startTree(t, withGrandchild({ grandchildIgnoresTerm: true }));
  const receipt = await tree.terminate({ graceMs: 300 });
  assert.deepEqual([receipt.confirmed, receipt.signals, receipt.leaderExit.signal], [true, ['SIGTERM', 'SIGKILL'], 'SIGTERM']);
  assert.equal(identityState(grandchild), 'DEAD');
});

test('a group that already exited is confirmed without sending signals', { skip: !posix }, async t => {
  const tree = await spawnSupervised(process.execPath, ['-e', 'process.exitCode = 3']);
  t.after(() => { kill(tree.supervisor.pid); });
  assert.deepEqual(await tree.exited, { code: 3, signal: null });
  const receipt = await tree.terminate();
  assert.deepEqual([receipt.confirmed, receipt.signals, receipt.leaderExit], [true, [], { code: 3, signal: null }]);
  assert.equal(await tree.terminate(), receipt, 'terminate is idempotent');
});

test('limit (not Linux): a descendant that started its own session is not covered', { skip: !posix || linux }, async t => {
  const { tree, grandchild } = await startTree(t, withGrandchild({ escape: true }));
  const receipt = await tree.terminate({ graceMs: 5000 });
  assert.deepEqual([receipt.confirmed, receipt.coverage], [true, 'process-group']);
  assert.equal(identityState(grandchild), 'UNVERIFIED', 'escaped the group, and only Linux can track it');
});

// Owner death (A3b): the tree never outlives its owner.

test('when the owner is killed, the supervisor stops the tree and releases the owner\'s pipes', { skip: !posix }, async t => {
  // The tree inherits the owner's stderr. Without a supervisor it would keep that
  // pipe open forever after the owner died: whoever reads it (a test runner) hangs.
  const owner = startOwner(t, { source: withGrandchild({ leaderIgnoresTerm: true, grandchildIgnoresTerm: true }) });
  const tree = await ownerTree(t, owner);
  assert.ok(!gone(tree.leader) && !gone(tree.grandchild));
  owner.kill('SIGKILL');
  await until(() => gone(tree.leader) && gone(tree.grandchild), 'the tree to be stopped');
  await until(() => gone(tree.supervisor), 'the supervisor to exit');
  await withTimeout(owner.stderrEnded, 5000, 'the owner\'s stderr pipe is still held');
});

test('when the owner exits normally, the tree is stopped too', { skip: !posix }, async t => {
  const owner = startOwner(t, { source: withGrandchild(), then: 'process.exit(0);' });
  const tree = await ownerTree(t, owner);
  assert.equal(await owner.closed(), 0);
  await until(() => gone(tree.leader) && gone(tree.grandchild) && gone(tree.supervisor), 'the tree and supervisor to be gone');
});

test('killing the owner\'s whole process group does not take down the supervisor first', { skip: !posix }, async t => {
  const owner = startOwner(t, { source: withGrandchild(), detached: true });
  const tree = await ownerTree(t, owner);
  killGroup(owner.pid); // for example a test runner or service manager killing the owner's group
  await until(() => gone(tree.leader) && gone(tree.grandchild), 'the tree to be stopped');
  await until(() => gone(tree.supervisor), 'the supervisor to exit');
});

test('members left in the group after the command ended are stopped when the owner exits', { skip: !posix }, async t => {
  // The leader exits at once; its grandchild stays in the group.
  const source = `require('node:child_process').spawn(process.execPath, ['-e', ${JSON.stringify('console.log(process.pid);' + forever)}],
    { stdio: ['ignore', 'inherit', 'ignore'] }); setTimeout(() => process.exit(0), 200);`;
  // Like any piped child stream, tree.stdout keeps the owner alive until it is closed.
  // The owner checks the grandchild itself before it closes the stream and exits: once
  // it exits, its supervisor stops the grandchild, so a later check here would race.
  const owner = startOwner(t, { source, then: `tree.exited.then(() => {
    let grandchildRuns = true;
    try { process.kill(Number(String(data).trim()), 0); } catch { grandchildRuns = false; }
    report({ exited: true, grandchildRuns });
    tree.stdout.destroy();
  });` });
  const tree = await ownerTree(t, owner);
  assert.deepEqual(await owner.output(), { exited: true, grandchildRuns: true }, 'the leader ended, its grandchild runs on');
  assert.ok(gone(tree.leader));
  assert.equal(await owner.closed(), 0, 'the owner can exit normally once the command ended');
  await until(() => gone(tree.grandchild) && gone(tree.supervisor), 'the leftover member to be stopped');
});

test('after terminate, nothing keeps the owner alive', { skip: !posix }, async t => {
  // The owner reports, then has nothing left to do: it must exit at once, not
  // after a leftover timer of the library fires.
  const owner = startOwner(t, { source: withGrandchild(),
    then: 'tree.stdout.destroy(); tree.terminate({ graceMs: 5000 }).then(receipt => report({ confirmed: receipt.confirmed }));' });
  await ownerTree(t, owner);
  assert.deepEqual(await owner.output(), { confirmed: true });
  const reported = performance.now();
  assert.equal(await owner.closed(), 0);
  assert.ok(performance.now() - reported < 700, `the owner exited ${Math.round(performance.now() - reported)} ms after terminating`);
});

// Go-ahead (A3c): the command starts only after beforeStart.

test('beforeStart runs while the supervisor waits; the command starts only afterwards', async t => {
  const directory = await temporaryDirectory(t);
  const marker = path.join(directory, 'started');
  let seen;
  const tree = await spawnSupervised(process.execPath, ['-e', `require('node:fs').writeFileSync(${JSON.stringify(marker)}, ''); ${forever}`], {
    beforeStart: async ({ supervisor }) => {
      await sleep(200);
      seen = { markerExists: existsSync(marker), supervisor: identityState(supervisor) };
    } });
  t.after(() => tree.terminate({ graceMs: 0 }));
  t.after(() => { kill(tree.pid); kill(tree.supervisor.pid); });
  assert.deepEqual(seen, { markerExists: false, supervisor: process.platform === 'linux' ? 'ALIVE' : 'UNVERIFIED' });
  await until(() => existsSync(marker), 'the command to start');
});

test('if beforeStart fails, the command never starts and the supervisor exits', async t => {
  const directory = await temporaryDirectory(t);
  const marker = path.join(directory, 'started');
  let supervisor;
  await assert.rejects(spawnSupervised(process.execPath, ['-e', `require('node:fs').writeFileSync(${JSON.stringify(marker)}, '')`], {
    beforeStart: async context => { supervisor = context.supervisor; throw new Error('could not record the process'); } }),
  /could not record the process/);
  t.after(() => kill(supervisor.pid));
  await until(() => gone(supervisor), 'the supervisor to exit');
  await sleep(300);
  assert.equal(existsSync(marker), false);
});

test('if the owner dies before the go-ahead, the command never starts', { skip: !posix }, async t => {
  const directory = await temporaryDirectory(t);
  const marker = path.join(directory, 'started');
  const owner = startOwner(t, { source: `require('node:fs').writeFileSync(${JSON.stringify(marker)}, ''); ${forever}`,
    beforeStart: 'await new Promise(() => setInterval(() => {}, 1000));' });
  const { supervisor } = await owner.output();
  const supervisorIdentity = processIdentity(supervisor);
  t.after(() => kill(supervisor));
  owner.kill('SIGKILL');
  await until(() => gone(supervisorIdentity), 'the supervisor to exit');
  await sleep(300);
  assert.equal(existsSync(marker), false);
});

test('on Windows the tree is stopped with taskkill and the receipt is never confirmed', { skip: posix }, async t => {
  const tree = await spawnSupervised(process.execPath, ['-e', forever]);
  t.after(() => tree.terminate({ graceMs: 0 }));
  const receipt = await tree.terminate({ graceMs: 2000 });
  assert.deepEqual([receipt.confirmed, receipt.method], [false, 'taskkill-tree']);
  assert.notEqual(receipt.leaderExit, null, 'the leader exited');
});

test('spawn failures and invalid options are reported', async () => {
  // ENOENT, or EACCES when an inaccessible directory is on PATH.
  await assert.rejects(spawnSupervised('mnemos-command-that-does-not-exist'), error => ['ENOENT', 'EACCES'].includes(error.code));
  await assert.rejects(spawnSupervised(''), /command/);
  await assert.rejects(spawnSupervised(process.execPath, [1]), /args/);
  await assert.rejects(spawnSupervised(process.execPath, [], { stdio: 'overlapped' }), /stdio/);
  await assert.rejects(spawnSupervised(process.execPath, [], { beforeStart: 'yes' }), /beforeStart/);
  await assert.rejects(spawnSupervised(process.execPath, [], { orphanGraceMs: -1 }), /orphanGraceMs/);
  await assert.rejects(spawnSupervised(process.execPath, [], { cwd: 5 }), /cwd/);
  await assert.rejects(spawnSupervised(process.execPath, [], { env: 'X=1' }), /env/);
  const tree = await spawnSupervised(process.execPath, ['-e', '']);
  await tree.exited;
  for (const graceMs of [-1, 1.5, 60_001]) assert.throws(() => tree.terminate({ graceMs }), /graceMs/);
  assert.throws(() => tree.terminate({ killWaitMs: -1 }), /killWaitMs/);
  await tree.terminate();
});

// Executors (A4): jobs that run a supervised command report what is known.

// A command that starts a grandchild and records both PIDs once the grandchild runs.
const recordingTree = (file, then = forever) => `
  const { spawn } = require('node:child_process');
  const grandchild = spawn(process.execPath, ['-e', ${JSON.stringify(forever)}], { stdio: 'ignore' });
  grandchild.once('spawn', () => {
    require('node:fs').writeFileSync(${JSON.stringify(file)}, JSON.stringify({ leader: process.pid, grandchild: grandchild.pid }));
    ${then}
  });
  ${forever}`;

async function recordedPids(t, file) {
  let pids;
  await until(async () => { try { pids = JSON.parse(await readFile(file, 'utf8')); return true; } catch { return false; } }, 'the PIDs');
  t.after(() => { kill(pids.leader); kill(pids.grandchild); if (posix) killGroup(pids.leader); });
  // A process that already exited has no identity: judge it by its PID.
  const identify = pid => processIdentity(pid) ?? { pid, host: hostname() };
  return { leader: identify(pids.leader), grandchild: identify(pids.grandchild) };
}

test('cancelling a job stops its whole process tree and records the confirmed receipt', { skip: !posix }, async t => {
  const directory = await temporaryDirectory(t);
  const pidFile = path.join(directory, 'pids.json');
  const execute = ({ signal }) => runSupervised({ command: process.execPath, args: ['-e', recordingTree(pidFile)], signal, graceMs: 2000 });
  const jobs = new FilePhaseJobRegistry({ directory, execute });
  await jobs.start({ jobId: 'build', objective: 'Run a build' });
  const tree = await recordedPids(t, pidFile);
  const cancelled = await jobs.requestCancellation('build', { timeoutMs: 10_000 });
  assert.deepEqual([cancelled.status, cancelled.cancellationConfirmed, cancelled.reason], ['CANCELLED', true, 'CANCELLED_BY_REQUEST']);
  assert.deepEqual([cancelled.result.termination.confirmed, cancelled.result.termination.method], [true, 'process-group']);
  assert.ok(gone(tree.leader) && gone(tree.grandchild), 'the grandchild is stopped too');
  const stored = await new FileJobStore({ directory }).read('build');
  assert.deepEqual([stored.status, stored.result.termination.confirmed], ['CANCELLED', true], 'the receipt is recorded');
});

test('a command that ends by itself completes or fails by its exit code', async () => {
  const jobs = new PhaseJobRegistry({ execute: ({ input, signal }) =>
    runSupervised({ command: process.execPath, args: ['-e', `process.exitCode = ${input.code}`], signal }) });
  for (const [jobId, code] of [['succeeds', 0], ['fails', 3]]) jobs.start({ jobId, objective: 'Exit', input: { code } });
  const succeeded = await jobs.wait('succeeds');
  assert.deepEqual([succeeded.status, succeeded.result.exitCode], ['COMPLETED', 0]);
  const failed = await jobs.wait('fails');
  assert.deepEqual([failed.status, failed.effectAbsent, failed.result.exitCode], ['FAILED', false, 3]);
});

test('processes a completed command left in its group are stopped with the job', { skip: !posix }, async t => {
  const directory = await temporaryDirectory(t);
  const pidFile = path.join(directory, 'pids.json');
  const jobs = new PhaseJobRegistry({ execute: ({ signal }) =>
    runSupervised({ command: process.execPath, args: ['-e', recordingTree(pidFile, 'process.exit(0);')], signal, graceMs: 2000 }) });
  jobs.start({ jobId: 'daemonizes', objective: 'Leave a process behind' });
  const done = await jobs.wait('daemonizes');
  const tree = await recordedPids(t, pidFile);
  assert.deepEqual([done.status, done.result.cleanup.confirmed, done.result.cleanup.signals], ['COMPLETED', true, ['SIGTERM']]);
  assert.ok(gone(tree.grandchild), 'no work outlives the job');
});

test('a command that never started is reported as effect-free', async () => {
  const missing = await runSupervised({ command: 'mnemos-command-that-does-not-exist' });
  assert.deepEqual([missing.status, missing.effectAbsent], ['FAILED', true]);
  assert.ok(['ENOENT', 'EACCES'].includes(missing.spawnError));
  const controller = new AbortController();
  controller.abort();
  const early = await runSupervised({ command: process.execPath, args: ['-e', forever], signal: controller.signal });
  assert.deepEqual([early.status, early.termination.confirmed, early.effectAbsent], ['CANCELLED', true, true]);
  await assert.rejects(runSupervised({ command: process.execPath, signal: 'abort' }), /AbortSignal/);
});

test('on Windows a cancelled job stays UNKNOWN: the receipt is not confirmed', { skip: posix }, async () => {
  const jobs = new PhaseJobRegistry({ execute: ({ signal }) => runSupervised({ command: process.execPath, args: ['-e', forever], signal }) });
  jobs.start({ jobId: 'windows_job', objective: 'Run on Windows' });
  await sleep(1000);
  const result = await jobs.requestCancellation('windows_job', { timeoutMs: 20_000 });
  assert.deepEqual([result.status, result.reconciliationRequired], ['UNKNOWN', true]);
});

// Descendants that leave the group (A5, Linux): tracked through their parent links.

// The command starts `parent` (in the group), which starts an escapee in its own
// session, which starts a child of its own. Every process records its PID once
// running. The parent exits after parentLivesMs (or never).
const escapingTree = (file, { parentLivesMs = null, escapeeIgnoresTerm = false, escapeeClearsEnvironment = false } = {}) => {
  const record = name => `require('node:fs').appendFileSync(${JSON.stringify(file)}, JSON.stringify({ ${name}: process.pid }) + '\\n');`;
  const escapeeChild = record('escapeeChild') + forever;
  const escapee = (escapeeIgnoresTerm ? ignoreTerm : '') + `require('node:child_process').spawn(process.execPath, ['-e', ${JSON.stringify(escapeeChild)}], { stdio: 'ignore' });`
    + record('escapee') + forever;
  const parent = `require('node:child_process').spawn(process.execPath, ['-e', ${JSON.stringify(escapee)}], { stdio: 'ignore', detached: true${escapeeClearsEnvironment ? ', env: {}' : ''} });`
    + record('parent') + (parentLivesMs === null ? forever : `setTimeout(() => process.exit(0), ${parentLivesMs});`);
  return `require('node:child_process').spawn(process.execPath, ['-e', ${JSON.stringify(parent)}], { stdio: 'ignore' }); ${forever}`;
};

async function escapingPids(t, file) {
  let pids = {};
  await until(async () => {
    try { pids = Object.assign({}, ...(await readFile(file, 'utf8')).trim().split('\n').map(line => JSON.parse(line))); }
    catch { return false; }
    return ['parent', 'escapee', 'escapeeChild'].every(name => pids[name]);
  }, 'the escaping tree to run');
  t.after(() => { for (const pid of Object.values(pids)) { kill(pid); killGroup(pid); } });
  const identify = pid => processIdentity(pid) ?? { pid, host: hostname() };
  return Object.fromEntries(Object.entries(pids).map(([name, pid]) => [name, identify(pid)]));
}

async function startEscapingTree(t, options, treeOptions = {}) {
  const directory = await temporaryDirectory(t);
  const file = path.join(directory, 'pids.jsonl');
  const tree = await spawnSupervised(process.execPath, ['-e', escapingTree(file, options)], treeOptions);
  t.after(() => { kill(tree.pid); killGroup(tree.pid); kill(tree.supervisor.pid); });
  return { tree, pids: await escapingPids(t, file) };
}

test('a descendant that started its own session, and its child, are found by the last scan and stopped', { skip: !linux }, async t => {
  // No periodic scan happens during the test: only the scan at terminate() can find them.
  const { tree, pids } = await startEscapingTree(t, {}, { trackIntervalMs: 60_000 });
  assert.ok(!gone(pids.escapee) && !gone(pids.escapeeChild));
  const receipt = await tree.terminate({ graceMs: 2000 });
  assert.deepEqual([receipt.confirmed, receipt.coverage], [true, 'process-group+descendants']);
  assert.deepEqual(receipt.tracked.map(entry => entry.pid).sort(), [pids.escapee.pid, pids.escapeeChild.pid].sort());
  assert.ok(gone(pids.escapee) && gone(pids.escapeeChild) && gone(pids.parent), 'nothing escaped the termination');
});

test('the receipt waits for tracked descendants and escalates for them too', { skip: !linux }, async t => {
  const { tree, pids } = await startEscapingTree(t, { escapeeIgnoresTerm: true }, { trackIntervalMs: 60_000 });
  const receipt = await tree.terminate({ graceMs: 300 });
  assert.deepEqual([receipt.confirmed, receipt.signals], [true, ['SIGTERM', 'SIGKILL']]);
  assert.ok(gone(pids.escapee), 'the receipt is confirmed only once the escaped descendant is gone');
});

test('sampling keeps track of an escaped descendant after its parent exits', { skip: !linux }, async t => {
  // It clears its environment too: only its parent link, while the parent lives, connects it to the tree.
  const { tree, pids } = await startEscapingTree(t, { parentLivesMs: 400, escapeeClearsEnvironment: true }, { trackIntervalMs: 50 });
  await until(() => gone(pids.parent), 'the parent to exit'); // the escapee's parent link is gone now
  const receipt = await tree.terminate({ graceMs: 2000 });
  assert.equal(receipt.confirmed, true);
  assert.ok(gone(pids.escapee) && gone(pids.escapeeChild), 'tracked while its parent lived, stopped after');
});

test('a detached descendant whose parent exited before any scan is found by its environment marker', { skip: !linux }, async t => {
  const { tree, pids } = await startEscapingTree(t, { parentLivesMs: 100 }, { trackIntervalMs: 60_000 });
  await until(() => gone(pids.parent), 'the parent to exit'); // no parent link is left to follow
  const receipt = await tree.terminate({ graceMs: 2000 });
  assert.equal(receipt.confirmed, true);
  assert.deepEqual(receipt.tracked.map(entry => entry.pid).sort(), [pids.escapee.pid, pids.escapeeChild.pid].sort());
  assert.ok(gone(pids.escapee) && gone(pids.escapeeChild));
  assert.deepEqual(markedProcesses(tree.marker), []);
});

test('limit: a descendant that clears its environment and detaches before any scan is not found', { skip: !linux }, async t => {
  const { tree, pids } = await startEscapingTree(t, { parentLivesMs: 100, escapeeClearsEnvironment: true }, { trackIntervalMs: 60_000 });
  // The limit applies once the escapee has lost its parent link, which can happen after its parent
  // already reports as exited (measured under load): wait for the link itself.
  await until(() => gone(pids.parent) && parentOf(pids.escapee.pid) !== pids.parent.pid, 'the escapee to lose its parent link');
  const receipt = await tree.terminate({ graceMs: 2000 });
  assert.deepEqual([receipt.confirmed, receipt.tracked], [true, []], 'the receipt covers what could be found');
  assert.ok(!gone(pids.escapee), 'no parent link and no marker: nothing connects it to the tree');
});

// Linux: running processes whose command line is exactly `sleep <duration>`.
function sleepers(duration) {
  const found = [];
  for (const name of readdirSync('/proc')) {
    if (!/^\d+$/.test(name)) continue;
    try {
      const stat = readFileSync(`/proc/${name}/stat`, 'utf8');
      if (['Z', 'X'].includes(stat[stat.lastIndexOf(')') + 2])) continue;
      const argv = readFileSync(`/proc/${name}/cmdline`, 'utf8').split('\0');
      if (argv[0].endsWith('sleep') && argv[1] === duration) found.push(Number(name));
    } catch { /* exited, or another user's */ }
  }
  return found;
}

// Linux: raw facts about processes that survived a termination, for the failure
// message, so that a rare failure (see verify:reliability) leaves data behind.
// Start times are in clock ticks since boot; uptime in seconds.
const uptime = () => Number(readFileSync('/proc/uptime', 'utf8').split(' ')[0]);
function survivorDiagnostics(pids, tree, receipt, uptimeAtStart) {
  const processes = pids.map(pid => {
    try {
      const stat = readFileSync(`/proc/${pid}/stat`, 'utf8');
      const fields = stat.slice(stat.lastIndexOf(')') + 2).split(' ');
      let marker;
      try { marker = readFileSync(`/proc/${pid}/environ`, 'latin1').split('\0').includes(`${receipt.marker}=1`); }
      catch (error) { marker = error.code; }
      return { pid, state: fields[0], ppid: Number(fields[1]), pgrp: Number(fields[2]), session: Number(fields[3]),
        startTicks: fields[19], marker, inReceipt: receipt.tracked.some(entry => entry.pid === pid) };
    } catch (error) { return { pid, error: error.code }; }
  });
  return JSON.stringify({ processes, leader: tree.pid, leaderStartTicks: tree.identity?.startTicks ?? null,
    uptimeAtTerminate: uptimeAtStart, uptimeAtCheck: uptime(), receipt });
}

test('a process started by a SIGTERM handler during termination is found and stopped', { skip: !linux }, async t => {
  // The command ignores nothing, but on SIGTERM it starts a detached process, then exits.
  const duration = `29.${Date.now() % 100000}1`;
  const source = `process.on('SIGTERM', () => {
      require('node:child_process').spawn('sleep', [${JSON.stringify(duration)}], { stdio: 'ignore', detached: true });
      setTimeout(() => process.exit(0), 50);
    }); ${forever}`;
  const tree = await spawnSupervised(process.execPath, ['-e', source], { trackIntervalMs: 60_000 });
  t.after(() => { for (const pid of sleepers(duration)) kill(pid); kill(tree.pid); kill(tree.supervisor.pid); });
  await sleep(300); // the handler is installed
  const uptimeAtStart = uptime();
  const receipt = await tree.terminate({ graceMs: 3000 });
  assert.equal(receipt.confirmed, true);
  await sleep(200);
  const survivors = sleepers(duration);
  assert.deepEqual(survivors, [], survivors.length === 0 ? undefined
    : `started after the last scan before the signals, and still stopped; ${survivorDiagnostics(survivors, tree, receipt, uptimeAtStart)}`);
});

test('processes started while the tree is being terminated cannot escape: the tree is frozen first', { skip: !linux }, async t => {
  // The command keeps starting detached processes that also clear their environment:
  // only their parent link connects them to the tree, and only while the parent runs.
  // It ignores SIGTERM, so it keeps starting them through the grace period; the tree
  // is frozen again before SIGKILL, so none can appear unseen before the kill.
  // (A parent that exits during the grace period, after starting such a process,
  // leaves it unreachable: the documented limit. Measured: a Node process sent
  // SIGTERM and SIGCONT may still start children before it exits.)
  const duration = `28.${Date.now() % 100000}1`;
  const spawner = `const { spawn } = require('node:child_process'); let count = 0; ${ignoreTerm}
    const timer = setInterval(() => {
      if (count++ > 1500) return clearInterval(timer);
      spawn('sleep', [${JSON.stringify(duration)}], { stdio: 'ignore', detached: true, env: {} });
    }, 2); ${forever}`;
  const tree = await spawnSupervised(process.execPath, ['-e', spawner], { trackIntervalMs: 60_000 });
  t.after(() => { for (const pid of sleepers(duration)) kill(pid); kill(tree.pid); killGroup(tree.pid); kill(tree.supervisor.pid); });
  await until(() => sleepers(duration).length > 5, 'the spawner to start processes');
  const uptimeAtStart = uptime();
  const receipt = await tree.terminate({ graceMs: 1000 });
  assert.deepEqual([receipt.confirmed, receipt.frozen, receipt.signals], [true, true, ['SIGTERM', 'SIGKILL']]);
  await sleep(200);
  const survivors = sleepers(duration);
  assert.deepEqual(survivors, [], survivors.length === 0 ? undefined
    : `no process of the tree survived; ${survivorDiagnostics(survivors, tree, receipt, uptimeAtStart)}`);
});

test('when the owner dies, descendants found only by their marker are stopped too', { skip: !linux }, async t => {
  const directory = await temporaryDirectory(t);
  const file = path.join(directory, 'pids.jsonl');
  // The escapee's parent exits at once, long before the first periodic scan.
  const owner = startOwner(t, { source: escapingTree(file, { parentLivesMs: 50 }) });
  await owner.output();
  const pids = await escapingPids(t, file);
  await until(() => gone(pids.parent), 'the parent to exit');
  owner.kill('SIGKILL');
  await until(() => gone(pids.escapee) && gone(pids.escapeeChild), 'the supervisor to stop them');
});

test('when the owner dies, tracked descendants are stopped too', { skip: !linux }, async t => {
  const directory = await temporaryDirectory(t);
  const file = path.join(directory, 'pids.jsonl');
  const owner = startOwner(t, { source: escapingTree(file) });
  const { supervisor } = await owner.output();
  const pids = await escapingPids(t, file);
  await sleep(600); // sampled at least once (every 250 ms by default)
  owner.kill('SIGKILL');
  await until(() => gone(pids.escapee) && gone(pids.escapeeChild) && gone(pids.parent), 'the whole tree to be stopped');
  await until(() => gone(processIdentity(supervisor) ?? { pid: supervisor, host: hostname() }), 'the supervisor to exit');
});

test('a cancelled job stops descendants that left its group', { skip: !linux }, async t => {
  const directory = await temporaryDirectory(t);
  const file = path.join(directory, 'pids.jsonl');
  const jobs = new PhaseJobRegistry({ execute: ({ signal }) =>
    runSupervised({ command: process.execPath, args: ['-e', escapingTree(file)], signal, graceMs: 2000 }) });
  jobs.start({ jobId: 'deploy', objective: 'Start a daemon' });
  const pids = await escapingPids(t, file);
  const cancelled = await jobs.requestCancellation('deploy', { timeoutMs: 10_000 });
  assert.deepEqual([cancelled.status, cancelled.cancellationConfirmed, cancelled.result.termination.coverage],
    ['CANCELLED', true, 'process-group+descendants']);
  assert.ok(gone(pids.escapee) && gone(pids.escapeeChild));
});

// An unconfirmed start (B1) and a supervisor that cannot be spawned (B2).

test('a command whose start was not confirmed is UNKNOWN, never effect-free, and is stopped', { skip: !posix }, async t => {
  const directory = await temporaryDirectory(t);
  const effect = path.join(directory, 'effect');
  const pidFile = path.join(directory, 'command.pid');
  // Preloaded into the supervisor only (it inherits NODE_OPTIONS): it starts the
  // command, then dies before it can report that the command started.
  const preload = path.join(directory, 'die-after-start.cjs');
  await writeFile(preload, `const send = process.send?.bind(process);
if (send) process.send = (message, ...rest) => {
  if (message?.type === 'started') process.kill(process.pid, 'SIGKILL');
  return send(message, ...rest);
};`);
  t.after(async () => { try { kill(Number(await readFile(pidFile, 'utf8'))); } catch { /* never started */ } });
  const jobs = new FilePhaseJobRegistry({ directory, execute: ({ runProcess }) => runProcess({ command: '/bin/sh',
    args: ['-c', `echo $$ > '${pidFile}'; sleep 1; echo charged >> '${effect}'`] }) });
  const previous = process.env.NODE_OPTIONS;
  process.env.NODE_OPTIONS = `--require "${preload}"`;
  let job;
  try {
    await jobs.start({ jobId: 'charge_1', objective: 'Charge once' });
    job = await withTimeout(jobs.wait('charge_1'), 20_000, 'the job did not settle');
  } finally {
    if (previous === undefined) delete process.env.NODE_OPTIONS;
    else process.env.NODE_OPTIONS = previous;
  }
  const [{ marker }] = job.processes; // recorded before the go-ahead
  t.after(() => { if (linux) for (const pid of markedProcesses(marker)) kill(pid); });
  // The command may have started, so its effect may happen: a retry is not known to be safe.
  assert.deepEqual([job.status, job.reason, job.effectAbsent, job.reconciliationRequired],
    ['UNKNOWN', 'EXECUTOR_REPORTED_UNKNOWN', false, true]);
  assert.equal(job.result.startUnconfirmed, true);
  assert.equal(job.result.marker, marker);
  // Linux can find the unsupervised command by its marker; it must not run on.
  if (linux) {
    assert.equal(job.result.termination.confirmed, true);
    assert.deepEqual(markedProcesses(marker), [], 'the command was stopped');
  }
});

test('a supervisor that cannot be spawned is reported as a start failure, never crashing its owner', { skip: !posix }, async () => {
  // With few file descriptors, spawning the supervisor (pipes and IPC) fails with EMFILE.
  const script = `
    import { openSync } from 'node:fs';
    import { runSupervised } from ${JSON.stringify(moduleUrl)};
    const held = [];
    try { for (;;) held.push(openSync('/dev/null', 'r')); } catch { /* no descriptor left */ }
    const result = await runSupervised({ command: 'true' });
    process.stdout.write(JSON.stringify(result));`;
  const { stdout } = await promisify(execFile)('/bin/sh', ['-c', 'ulimit -n 64 && exec "$0" --input-type=module -e "$1"',
    process.execPath, script], { timeout: 20_000 });
  const result = JSON.parse(stdout);
  // Nothing was started: this one is effect-free.
  assert.deepEqual([result.status, result.effectAbsent], ['FAILED', true]);
  assert.ok(['EMFILE', 'EAGAIN', 'ENFILE'].includes(result.spawnError), result.spawnError);
});

test('env is passed as child_process.spawn() would: undefined values left out, others as strings', async () => {
  const check = 'process.exit(process.env.COUNT === "2" && process.env.BIG === "1" && !("GONE" in process.env) ? 0 : 7)';
  const result = await runSupervised({ command: process.execPath, args: ['-e', check],
    env: { PATH: process.env.PATH, COUNT: 2, BIG: 1n, GONE: undefined } }); // a BigInt could not cross the supervisor's channel
  assert.deepEqual([result.status, result.exitCode], ['COMPLETED', 0]);
});

test('piped output is always read: a command writing more than a pipe holds finishes, and onOutput gets all of it', async () => {
  // Before, nothing read the pipes: a command writing 1 MiB blocked until it was stopped (measured).
  const writeMiB = ['-e', 'process.stdout.write("x".repeat(1024 * 1024)); process.stderr.write("done")'];
  const chunks = { stdout: 0, stderr: 0 };
  const result = await withTimeout(runSupervised({ command: process.execPath, args: writeMiB, stdio: ['ignore', 'pipe', 'pipe'],
    onOutput: (chunk, stream) => { chunks[stream] += chunk.length; } }), 30_000, 'the command did not finish');
  assert.deepEqual([result.status, result.outputComplete, chunks], ['COMPLETED', true, { stdout: 1024 * 1024, stderr: 4 }]);
  const drained = await withTimeout(runSupervised({ command: process.execPath, args: writeMiB, stdio: ['ignore', 'pipe', 'pipe'] }),
    30_000, 'the command did not finish without onOutput');
  assert.deepEqual([drained.status, drained.outputComplete], ['COMPLETED', true]);
  // A failing onOutput is not called again; the output is still drained and reported incomplete.
  const failing = await runSupervised({ command: process.execPath, args: writeMiB, stdio: ['ignore', 'pipe', 'pipe'],
    onOutput: () => { throw new Error('consumer failed'); } });
  assert.deepEqual([failing.status, failing.outputComplete], ['COMPLETED', false]);
  assert.equal('outputComplete' in await runSupervised({ command: process.execPath, args: ['-e', ''] }), false, 'nothing piped');
  await assert.rejects(runSupervised({ command: process.execPath, onOutput: 'log' }), /onOutput/);
});
