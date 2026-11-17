import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { readdirSync, readFileSync } from 'node:fs';
import { hostname } from 'node:os';
import { processIdentity } from './process-identity.mjs';

// Supervised process trees. spawnSupervised and runSupervised are exported from
// index.mjs; the scanning and stopping helpers are internal.

const MAX_WAIT_MS = 60_000;
const posix = process.platform !== 'win32';
const STDIO = new Set(['ignore', 'pipe', 'inherit']);

// Wait for a promise for at most ms. The timer is cleared as soon as the
// promise settles, so it never keeps the process alive on its own.
function within(promise, ms) {
  let timer;
  return Promise.race([promise, new Promise(resolve => { timer = setTimeout(resolve, ms); })])
    .finally(() => clearTimeout(timer));
}

function boundedMs(value, label, min = 0) {
  if (!Number.isInteger(value) || value < min || value > MAX_WAIT_MS) {
    throw new TypeError(`${label} must be an integer between ${min} and ${MAX_WAIT_MS}`);
  }
  return value;
}

/**
 * Linux: every process as { state, ppid, pgrp, start } by PID, from /proc/<pid>/stat.
 * The shared functions below run in the owner and, through their source, inside
 * the supervisor, so they may use only readdirSync, readFileSync and process.
 */
function procTable() {
  const table = new Map();
  let names;
  try { names = readdirSync('/proc'); } catch { return table; }
  for (const name of names) {
    if (!/^\d+$/.test(name)) continue;
    let stat;
    try { stat = readFileSync(`/proc/${name}/stat`, 'utf8'); } catch { continue; } // exited meanwhile
    const fields = stat.slice(stat.lastIndexOf(')') + 2).split(' ');
    table.set(Number(name), { state: fields[0], ppid: Number(fields[1]), pgrp: Number(fields[2]), start: fields[19] });
  }
  return table;
}

/**
 * Members of a process group that have not exited. On Linux, from /proc:
 * members that exited and await reaping (zombies) are not counted. Elsewhere
 * on POSIX, kill(-pgid, 0) can only tell whether any member exists: null means
 * "some member remains, PIDs unknown".
 */
function liveGroupMembers(pgid) {
  if (pgid === null) return []; // no group: a tree known only by its marker
  if (process.platform === 'linux') {
    const members = [];
    for (const [pid, entry] of procTable()) {
      if (entry.pgrp === pgid && entry.state !== 'Z' && entry.state !== 'X') members.push(pid);
    }
    return members;
  }
  try { process.kill(-pgid, 0); return null; }
  catch (error) { return error.code === 'ESRCH' ? [] : null; }
}

/**
 * Linux: the running processes of the tree led by `pgid` that are outside its
 * process group, as [{ pid, start }]:
 * - descendants, by parent links, of the leader and of `roots` ([pid, start]
 *   pairs found earlier, while they are still the same process);
 * - with withEnvironment, every process carrying the tree's environment marker
 *   (`${marker}=1`, inherited by descendants), however it detached, and its
 *   descendants. Only processes of the same user can be read.
 */
export function scanTree(pgid, marker, roots, withEnvironment) {
  const table = procTable();
  const running = entry => entry !== undefined && entry.state !== 'Z' && entry.state !== 'X';
  const found = new Map();
  const add = pid => {
    const entry = table.get(pid);
    if (running(entry) && entry.pgrp !== pgid) found.set(pid, entry.start);
  };
  const starts = pgid === null ? [] : [pgid];
  for (const [pid, start] of roots) {
    if (table.get(pid)?.start === start) { add(pid); starts.push(pid); }
  }
  if (withEnvironment && marker) {
    const needle = `${marker}=1`;
    for (const [pid, entry] of table) {
      if (!running(entry) || entry.pgrp === pgid || found.has(pid)) continue;
      let environ;
      try { environ = readFileSync(`/proc/${pid}/environ`, 'latin1'); } catch { continue; }
      if (environ.split('\0').includes(needle)) { add(pid); starts.push(pid); }
    }
  }
  const children = new Map();
  for (const [pid, entry] of table) {
    if (!children.has(entry.ppid)) children.set(entry.ppid, []);
    children.get(entry.ppid).push(pid);
  }
  const seen = new Set(starts);
  const queue = [...starts];
  while (queue.length) {
    for (const pid of children.get(queue.shift()) ?? []) {
      if (seen.has(pid)) continue;
      seen.add(pid);
      queue.push(pid);
      add(pid);
    }
  }
  return [...found].map(([pid, start]) => ({ pid, start }));
}

/** Linux: whether `pid` is still the running process that started at `start`. */
function sameProcess(pid, start) {
  let stat;
  try { stat = readFileSync(`/proc/${pid}/stat`, 'utf8'); } catch { return false; }
  const fields = stat.slice(stat.lastIndexOf(')') + 2).split(' ');
  return fields[0] !== 'Z' && fields[0] !== 'X' && fields[19] === start;
}

/**
 * Send `signal` to the group (while it has live members, so that its ID still
 * names it) and to each process in `processes` ([{ pid, start }]) that is still
 * the same process. Never signals a reused PID. Returns whether anything was sent.
 */
function signalTree(pgid, processes, signal) {
  let sent = false;
  const members = liveGroupMembers(pgid);
  if (members === null || members.length > 0) {
    try { process.kill(-pgid, signal); sent = true; } catch { /* no member left */ }
  }
  for (const { pid, start } of processes) {
    if (!sameProcess(pid, start)) continue;
    try { process.kill(pid, signal); sent = true; } catch { /* exited meanwhile */ }
  }
  return sent;
}

/**
 * Linux: stop (SIGSTOP) the group and every process of the tree that a full
 * scan finds, again and again until a scan finds nothing new. Stopped processes
 * cannot start others, so nothing can appear between the last scan and the
 * termination signals. Returns every process found outside the group.
 */
function freezeTree(pgid, marker, roots) {
  const known = new Map(roots);
  for (let round = 0; round < 50; round++) {
    signalTree(pgid, [], 'SIGSTOP');
    let added = 0;
    for (const { pid, start } of scanTree(pgid, marker, [...known], true)) {
      if (known.get(pid) === start) continue;
      known.set(pid, start);
      if (sameProcess(pid, start)) { try { process.kill(pid, 'SIGSTOP'); } catch { /* exited */ } }
      added++;
    }
    if (added === 0) break;
  }
  return [...known].map(([pid, start]) => ({ pid, start }));
}

/**
 * Stop a tree and verify it: the procedure shared by terminate() and by the
 * supervisor when its owner is gone. On Linux the tree is frozen first, then
 * SIGTERM (and SIGCONT, so handlers run), a grace period, a second freeze (so
 * nothing a SIGTERM handler started can start more), SIGKILL. While waiting it
 * rescans: processes started meanwhile, for example by a SIGTERM handler, get
 * the current signal too. `confirmed` requires the group to be empty, every
 * known process to be gone and, on Linux, a fresh scan to find nothing running.
 * Elsewhere on POSIX only the group is covered.
 */
export async function stopTree(pgid, marker, roots, graceMs, killWaitMs) {
  const linux = process.platform === 'linux';
  const known = new Map(roots);
  const signals = [];
  let frozen = false;
  const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
  const list = () => [...known].map(([pid, start]) => ({ pid, start }));
  const learn = found => {
    const added = [];
    for (const { pid, start } of found) {
      if (known.get(pid) !== start) { known.set(pid, start); added.push({ pid, start }); }
    }
    return added;
  };
  const freeze = () => { if (linux) { learn(freezeTree(pgid, marker, [...known])); frozen = true; } };
  const state = () => {
    const members = liveGroupMembers(pgid);
    const escaped = linux ? list().filter(({ pid, start }) => sameProcess(pid, start)).map(({ pid }) => pid) : [];
    return { empty: members !== null && members.length === 0 && escaped.length === 0,
      survivors: members === null ? null : [...members, ...escaped] };
  };
  async function settle(waitMs, signal) {
    const deadline = performance.now() + waitMs;
    let nextScan = 0;
    for (;;) {
      let current = state();
      if (linux && (current.empty || performance.now() >= nextScan)) {
        const added = learn(scanTree(pgid, marker, [...known], true));
        nextScan = performance.now() + 100;
        if (added.length > 0) {
          if (signal) {
            signalTree(pgid, added, signal);
            if (signal === 'SIGTERM' && frozen) signalTree(pgid, added, 'SIGCONT');
          }
          // What was just found (or signalled) may have started others meanwhile: only a
          // later scan that finds nothing new can show the tree empty.
          current = { ...state(), empty: false };
        }
      }
      if (current.empty || performance.now() >= deadline) return current;
      await sleep(20);
    }
  }
  let current = await settle(0);
  if (!current.empty) freeze();
  for (const [signal, waitMs] of [['SIGTERM', graceMs], ['SIGKILL', killWaitMs]]) {
    if (current.empty) break;
    if (signal === 'SIGKILL') freeze();
    if (signalTree(pgid, list(), signal)) signals.push(signal);
    if (signal === 'SIGTERM' && frozen) signalTree(pgid, list(), 'SIGCONT');
    current = await settle(waitMs, signal);
  }
  return { confirmed: current.empty, signals, frozen, survivors: current.survivors, processes: list() };
}

/**
 * Linux: whether a live member of the group carries the tree's marker, that is,
 * whether the group ID still names that tree's group after its leader is gone.
 */
export function groupCarriesMarker(pgid, marker) {
  if (process.platform !== 'linux' || !Number.isSafeInteger(pgid) || pgid <= 0) return false;
  const needle = `${marker}=1`;
  for (const pid of liveGroupMembers(pgid)) {
    try { if (readFileSync(`/proc/${pid}/environ`, 'latin1').split('\0').includes(needle)) return true; }
    catch { /* exited, or another user's */ }
  }
  return false;
}

// Their source, without the module's export keywords, runs inside the supervisor.
const SHARED = [procTable, liveGroupMembers, scanTree, sameProcess, signalTree, freezeTree, stopTree].map(String).join('\n');

/**
 * The supervisor: a small Node.js process in its own session, connected to the
 * owner by the IPC channel. It starts the command only on the owner's "go", as
 * the leader of a new process group carrying the tree's environment marker, and
 * reports its start and exit. On Linux it also follows, every trackIntervalMs,
 * the parent links of descendants that leave the group and reports them. When
 * the owner disconnects without releasing it (the owner exited, crashed or was
 * killed, even with SIGKILL), it stops whatever remains of the tree: freeze,
 * SIGTERM, a grace period, SIGKILL. It ignores terminal and termination signals
 * so that only the owner, or the owner's death, decides.
 */
const SUPERVISOR = `
'use strict';
const { spawn } = require('node:child_process');
const { readdirSync, readFileSync } = require('node:fs');
${SHARED}
for (const signal of ['SIGINT', 'SIGTERM', 'SIGHUP']) process.on(signal, () => {});
const windows = process.platform === 'win32';
const linux = process.platform === 'linux';
let child = null;
let exited = false;
let released = false;
let spec = null;
const tracked = new Map(); // pid -> start: descendants outside the group
const send = message => { try { if (process.connected) process.send(message); } catch {} };
const list = () => [...tracked].map(([pid, start]) => ({ pid, start }));
function track() {
  if (!linux || !child) return;
  let changed = false;
  for (const { pid, start } of scanTree(child.pid, spec.marker, [...tracked], false)) {
    if (tracked.get(pid) !== start) { tracked.set(pid, start); changed = true; }
  }
  if (changed) send({ type: 'tracked', processes: list() });
}
async function stopOrphanedTree() {
  if (windows) {
    if (exited) return;
    await new Promise(resolve => spawn('taskkill', ['/PID', String(child.pid), '/T', '/F'], { stdio: 'ignore', windowsHide: true })
      .once('close', resolve).once('error', resolve));
    return;
  }
  await stopTree(child.pid, spec.marker, [...tracked], spec.graceMs, spec.killWaitMs);
}
let cleaning = false;
async function ownerGone() {
  if (cleaning) return;
  cleaning = true;
  if (child && !released) await stopOrphanedTree();
  process.exit(0);
}
process.on('disconnect', ownerGone);
process.on('message', message => {
  if (message?.type === 'release') { released = true; process.disconnect(); return; }
  if (message?.type === 'scan') { track(); send({ type: 'scanned', processes: list() }); return; }
  if (message?.type !== 'go' || child || cleaning) return;
  spec = message;
  child = spawn(spec.command, spec.args, { cwd: spec.cwd, env: { ...(spec.env ?? process.env), [spec.marker]: '1' },
    windowsHide: true, stdio: ['inherit', 'inherit', 'inherit', 'ignore'], detached: !windows });
  child.once('spawn', () => {
    send({ type: 'started', pid: child.pid });
    if (linux) setInterval(track, spec.trackIntervalMs);
  });
  child.once('error', error => { send({ type: 'error', code: error.code, message: String(error.message) }); child = null; });
  child.once('exit', (code, signal) => { exited = true; send({ type: 'exit', code, signal }); });
});
send({ type: 'ready' });
`;

function normalizeStdio(stdio) {
  const list = typeof stdio === 'string' ? [stdio, stdio, stdio] : stdio;
  if (!Array.isArray(list) || list.length !== 3 || !list.every(entry => STDIO.has(entry))) {
    throw new TypeError("stdio must be 'ignore', 'pipe' or 'inherit', or an array of three of them");
  }
  return list;
}

function nextMessage(supervisor, types, timeoutMs, label) {
  return new Promise((resolve, reject) => {
    const onMessage = message => { if (types.includes(message?.type)) { done(); resolve(message); } };
    const onExit = () => { done(); reject(new Error(`The process supervisor exited before it reported ${label}`)); };
    // Spawning the supervisor itself can fail (EMFILE, EAGAIN): 'error' then comes without 'exit'.
    const onError = error => { done(); reject(error); };
    const timer = setTimeout(() => { done(); reject(Object.assign(new Error(`The process supervisor did not report ${label}`),
      { code: 'SUPERVISOR_TIMEOUT' })); }, timeoutMs);
    function done() {
      clearTimeout(timer);
      supervisor.off('message', onMessage); supervisor.off('exit', onExit); supervisor.off('error', onError);
    }
    supervisor.on('message', onMessage);
    supervisor.once('exit', onExit);
    supervisor.once('error', onError);
  });
}

function taskkill(pid, force) {
  return new Promise(resolve => {
    const args = ['/PID', String(pid), '/T', ...(force ? ['/F'] : [])];
    const killer = spawn('taskkill', args, { stdio: 'ignore', windowsHide: true });
    killer.once('error', () => resolve(false));
    killer.once('close', code => resolve(code === 0));
  });
}

/**
 * Start a command under a supervisor, as the leader of a new process group
 * (POSIX), so that it and every descendant that stays in the group can be
 * stopped together, including when this (owning) process dies.
 *
 * - beforeStart({ supervisor, marker }): awaited after the supervisor is
 *   running and before the command starts, for example to record the
 *   supervisor and the tree's environment marker durably. If it throws, or this
 *   process dies first, the command never starts.
 * - onStart({ pid, identity, marker }): awaited once the command has started,
 *   for example to record its identity. Its failure is ignored.
 *   If the go-ahead was sent but the start is never reported (the supervisor
 *   died or timed out), it rejects with code START_UNCONFIRMED and the receipt
 *   of stopping whatever may have started (see unconfirmedStart()).
 * - terminate({ graceMs, killWaitMs }): freeze the tree (Linux), SIGTERM, up
 *   to graceMs, freeze again, SIGKILL, up to killWaitMs (see stopTree()).
 *   Resolves to a receipt whose `confirmed` is true only when the tree is
 *   verifiably gone (members that exited and await reaping count as gone).
 *   Coverage: on Linux the process group and every descendant, including those
 *   that started their own session or group, found by parent links and by the
 *   tree's environment marker; elsewhere on POSIX the process group only.
 * - If this process exits or dies without terminating the tree, the supervisor
 *   stops it the same way (orphanGraceMs, then SIGKILL): nothing it covers
 *   outlives its owner.
 * - Windows has no process groups: the tree is stopped with taskkill /T, which
 *   cannot reach descendants whose parent already exited, so the receipt is
 *   never confirmed there.
 */
export async function spawnSupervised(command, args = [], { cwd, env, stdio = 'ignore', beforeStart, onStart,
  orphanGraceMs = 2_000, startTimeoutMs = 10_000, trackIntervalMs = 250 } = {}) {
  if (typeof command !== 'string' || !command) throw new TypeError('command is required');
  if (!Array.isArray(args) || !args.every(arg => typeof arg === 'string')) throw new TypeError('args must be strings');
  if (beforeStart !== undefined && typeof beforeStart !== 'function') throw new TypeError('beforeStart must be a function');
  if (onStart !== undefined && typeof onStart !== 'function') throw new TypeError('onStart must be a function');
  boundedMs(orphanGraceMs, 'orphanGraceMs');
  boundedMs(startTimeoutMs, 'startTimeoutMs', 1);
  boundedMs(trackIntervalMs, 'trackIntervalMs', 10);
  const streams = normalizeStdio(stdio);
  if (cwd !== undefined && typeof cwd !== 'string') throw new TypeError('cwd must be a string');
  // As child_process.spawn() treats it: undefined values are left out, others become strings.
  // The supervisor receives it as JSON, which could not carry every value.
  if (env !== undefined && (env === null || typeof env !== 'object')) throw new TypeError('env must be an object');
  const environment = env === undefined ? undefined
    : Object.fromEntries(Object.entries(env).filter(([, value]) => value !== undefined).map(([name, value]) => [name, String(value)]));

  // The supervisor is in its own session: a signal to the owner's process group
  // (Ctrl-C, a runner killing the group) does not take it down with the owner.
  const supervisor = spawn(process.execPath, ['-e', SUPERVISOR],
    { stdio: [...streams, 'ipc'], detached: posix, windowsHide: true });
  // A ChildProcess 'error' without a listener would crash this (owning) process:
  // spawning can fail (EMFILE, EAGAIN), and so can a message to a supervisor
  // that just exited. Startup failures are reported through nextMessage();
  // later ones are covered by the exit report and termination receipts.
  supervisor.on('error', () => {});
  const abandon = () => { if (supervisor.connected) supervisor.disconnect(); };
  // Listen for the exit report before the command can start: "started" and
  // "exit" of a short command can arrive in the same tick.
  let leaderExit = null;
  const exited = new Promise(resolve => {
    const onMessage = message => {
      if (message?.type !== 'exit') return;
      supervisor.off('message', onMessage);
      leaderExit = { code: message.code, signal: message.signal };
      // The command ended: let this process exit normally. The supervisor stays
      // until then, to stop any member left in the group when it does.
      supervisor.unref();
      supervisor.channel?.unref();
      resolve(leaderExit);
    };
    supervisor.on('message', onMessage);
  });
  // Descendants that left the group, as reported by the supervisor (Linux), kept
  // as identities so that a reused PID is never signalled.
  const ownBootId = processIdentity(process.pid)?.bootId;
  const tracking = process.platform === 'linux';
  const tracked = new Map();
  const addTracked = processes => {
    for (const { pid, start } of processes) {
      if (!Number.isSafeInteger(pid) || typeof start !== 'string') continue;
      tracked.set(`${pid}:${start}`, { pid, host: hostname(), bootId: ownBootId, startTicks: start });
    }
  };
  supervisor.on('message', message => {
    if (tracking && ['tracked', 'scanned'].includes(message?.type) && Array.isArray(message.processes)) addTracked(message.processes);
  });
  // Inherited by every descendant: finds them even after they detached and lost their parent.
  const marker = `MNEMOS_TREE_${randomUUID().replaceAll('-', '')}`;
  let started;
  let supervisorIdentity = null;
  // Until the go-ahead is sent, a failure proves that the command never started.
  // Afterwards only the supervisor's report can tell: without it, the command
  // may have started (and may still run) although its start was never confirmed.
  let goSent = false;
  try {
    await nextMessage(supervisor, ['ready'], startTimeoutMs, 'that it is ready');
    supervisorIdentity = processIdentity(supervisor.pid);
    if (beforeStart) await beforeStart({ supervisor: supervisorIdentity, marker });
    const reply = nextMessage(supervisor, ['started', 'error'], startTimeoutMs, 'the start of the command');
    // On a closed channel send() does not throw (it returns false); the reply then
    // fails, and the start counts as unconfirmed.
    supervisor.send({ type: 'go', command, args, cwd, env: environment, marker, graceMs: orphanGraceMs, killWaitMs: 2_000, trackIntervalMs });
    goSent = true;
    started = await reply;
  } catch (error) {
    abandon(); // a supervisor still connected stops whatever it started, then exits
    if (!goSent) throw error;
    throw await unconfirmedStart(error);
  }
  if (started.type === 'error') {
    // The supervisor reports that spawning the command failed: it never started.
    abandon();
    throw Object.assign(new Error(`spawn ${command} ${started.code}: ${started.message}`), { code: started.code });
  }

  /**
   * The go-ahead was sent but the start was never reported: the supervisor died
   * or did not answer in time. The command may have started. On Linux, stop the
   * supervisor (so it can start nothing more) and everything it started or that
   * carries the tree's marker, with a receipt. Elsewhere nothing here can find
   * the command; a supervisor that is still connected stops it on disconnect.
   */
  async function unconfirmedStart(cause) {
    let termination = { confirmed: false, method: 'none', coverage: 'none', marker, verifiedAt: new Date().toISOString() };
    if (process.platform === 'linux') {
      const roots = supervisorIdentity?.startTicks ? [[supervisor.pid, supervisorIdentity.startTicks]] : [];
      const result = await stopTree(null, marker, roots, orphanGraceMs, 2_000);
      termination = { confirmed: result.confirmed, method: 'marker', coverage: 'supervisor-descendants+marker', marker,
        signals: result.signals, frozen: result.frozen, survivors: result.survivors, verifiedAt: new Date().toISOString() };
    }
    return Object.assign(new Error(`The start of ${command} was not confirmed: ${cause.message}`, { cause }),
      { code: 'START_UNCONFIRMED', startUnconfirmed: true, marker, termination });
  }

  const pid = started.pid;
  const identity = processIdentity(pid); // null if it already exited
  // The command runs: a failure to record it must not hide it from the caller.
  if (onStart) { try { await onStart({ pid, identity, marker }); } catch { /* recorded by marker already */ } }
  let termination;

  // A last sync with the supervisor's periodic scans before anything is signalled.
  async function finalScan() {
    if (!tracking || !supervisor.connected) return;
    let onMessage;
    const scanned = new Promise(resolve => {
      onMessage = message => { if (message?.type === 'scanned') resolve(); };
      supervisor.on('message', onMessage);
    });
    try { supervisor.send({ type: 'scan' }); await within(scanned, 1_000); }
    catch { /* the supervisor is gone; the scans below still apply */ }
    finally { supervisor.off('message', onMessage); }
  }

  const release = () => { if (supervisor.connected) supervisor.send({ type: 'release' }); };

  async function terminatePosix(graceMs, killWaitMs) {
    await finalScan();
    const roots = [...tracked.values()].map(({ pid: trackedPid, startTicks }) => [trackedPid, startTicks]);
    const result = await stopTree(pid, tracking ? marker : null, tracking ? roots : [], graceMs, killWaitMs);
    addTracked(result.processes);
    if (result.confirmed) {
      await within(exited, 1_000); // the supervisor's report of the exit status
      release();
    }
    return { confirmed: result.confirmed, method: 'process-group', coverage: tracking ? 'process-group+descendants' : 'process-group',
      pid, pgid: pid, supervisorPid: supervisor.pid, signals: result.signals, frozen: result.frozen, survivors: result.survivors,
      marker, tracked: [...tracked.values()].map(({ pid: trackedPid, startTicks }) => ({ pid: trackedPid, startTicks })),
      leaderExit, verifiedAt: new Date().toISOString() };
  }

  async function terminateWindows(graceMs, killWaitMs) {
    const signals = [];
    if (!leaderExit) {
      if (await taskkill(pid, false)) signals.push('taskkill /T');
      await within(exited, graceMs);
    }
    if (!leaderExit) {
      if (await taskkill(pid, true)) signals.push('taskkill /T /F');
      await within(exited, killWaitMs);
    }
    if (leaderExit) release();
    // Descendants whose parent already exited are not reachable: never confirmed.
    return { confirmed: false, method: 'taskkill-tree', coverage: 'live-descendants', pid, pgid: null,
      supervisorPid: supervisor.pid, signals, survivors: null, leaderExit, verifiedAt: new Date().toISOString() };
  }

  return {
    pid,
    identity,
    supervisor: supervisorIdentity,
    marker,
    stdin: supervisor.stdin,
    stdout: supervisor.stdout,
    stderr: supervisor.stderr,
    exited,
    terminate({ graceMs = 5_000, killWaitMs = 5_000 } = {}) {
      boundedMs(graceMs, 'graceMs');
      boundedMs(killWaitMs, 'killWaitMs');
      termination ??= posix ? terminatePosix(graceMs, killWaitMs) : terminateWindows(graceMs, killWaitMs);
      return termination;
    }
  };
}

/**
 * An executor body for PhaseJobRegistry and FilePhaseJobRegistry that runs a
 * command under supervision and reports what is known:
 *
 *   execute: ({ signal }) => runSupervised({ command, args, signal })
 *
 * - Abort: the tree is terminated; { status: 'CANCELLED', termination: receipt }.
 *   The registry counts the cancellation as confirmed only if the receipt is.
 * - The command ends by itself: COMPLETED for exit code 0, FAILED otherwise.
 *   Members it left in its group are stopped too (cleanup: receipt), so no work
 *   outlives the job.
 * - The command never started (it cannot be spawned, or the job was aborted
 *   first): FAILED or CANCELLED with effectAbsent: true.
 * - Piped stdout and stderr are always read (a pipe nobody reads fills up and
 *   blocks the command): each chunk goes to onOutput(chunk, 'stdout' | 'stderr')
 *   if given, and is discarded otherwise. Once the command has ended, the result
 *   says whether all output arrived (outputComplete: the pipes closed within 2 s).
 * - Its start was not confirmed (the go-ahead was sent, but the supervisor
 *   died or did not report in time): UNKNOWN with startUnconfirmed: true and
 *   the receipt of stopping whatever may have started. Never effect-free.
 */
export async function runSupervised({ command, args = [], signal, cwd, env, stdio = 'ignore', beforeStart, onStart,
  onOutput, graceMs = 5_000, killWaitMs = 5_000, trackIntervalMs } = {}) {
  if (signal !== undefined && !(signal instanceof AbortSignal)) throw new TypeError('signal must be an AbortSignal');
  if (onOutput !== undefined && typeof onOutput !== 'function') throw new TypeError('onOutput must be a function');
  boundedMs(graceMs, 'graceMs');
  boundedMs(killWaitMs, 'killWaitMs');
  const notStarted = { status: 'CANCELLED', termination: { confirmed: true, method: 'not-started' }, effectAbsent: true };
  if (signal?.aborted) return notStarted;
  let tree;
  try {
    tree = await spawnSupervised(command, args, { cwd, env, stdio, beforeStart, onStart, trackIntervalMs });
  } catch (error) {
    if (error instanceof TypeError) throw error;
    // The go-ahead was sent but the start never confirmed: the command may have run.
    if (error.code === 'START_UNCONFIRMED') {
      return { status: 'UNKNOWN', startUnconfirmed: true, marker: error.marker, termination: error.termination };
    }
    // Nothing was started: the supervisor reported the spawn failure, or never got the go-ahead.
    return { status: 'FAILED', effectAbsent: true, spawnError: typeof error.code === 'string' ? error.code : 'SPAWN_FAILED' };
  }
  const output = readOutput(tree, onOutput);
  let onAbort;
  const aborted = new Promise(resolve => {
    onAbort = resolve;
    if (signal?.aborted) resolve();
    signal?.addEventListener('abort', onAbort, { once: true });
  });
  const first = await Promise.race([tree.exited.then(() => 'exited'), aborted.then(() => 'aborted')]);
  signal?.removeEventListener('abort', onAbort);
  const receipt = await tree.terminate({ graceMs, killWaitMs });
  const outputComplete = await output.closed(OUTPUT_CLOSE_WAIT_MS);
  const reported = outputComplete === null ? {} : { outputComplete };
  if (first === 'aborted') return { status: 'CANCELLED', termination: receipt, ...reported };
  const { code, signal: exitSignal } = receipt.leaderExit ?? await tree.exited;
  return { status: code === 0 ? 'COMPLETED' : 'FAILED', exitCode: code, exitSignal, cleanup: receipt, ...reported };
}

const OUTPUT_CLOSE_WAIT_MS = 2_000;

// Read the tree's piped stdout and stderr from the start. closed(ms) resolves to
// whether every pipe closed (all output arrived, and onOutput never failed)
// within ms, or null when nothing is piped. A failing onOutput is not called
// again; its stream is still drained, so the command is never blocked.
function readOutput(tree, onOutput) {
  const streams = [['stdout', tree.stdout], ['stderr', tree.stderr]].filter(([, stream]) => stream);
  if (streams.length === 0) return { closed: async () => null };
  let failed = false;
  const closed = Promise.all(streams.map(([name, stream]) => new Promise(resolve => {
    stream.on('data', chunk => {
      if (!onOutput || failed) return;
      try { onOutput(chunk, name); } catch { failed = true; }
    });
    stream.once('close', resolve);
  })));
  return {
    async closed(waitMs) {
      let timer;
      const done = await Promise.race([closed.then(() => true), new Promise(resolve => { timer = setTimeout(() => resolve(false), waitMs); })]);
      clearTimeout(timer);
      return done && !failed;
    }
  };
}
