import { mkdtemp, rm } from 'node:fs/promises';
import { createRequire, syncBuiltinESMExports } from 'node:module';
import { Worker } from 'node:worker_threads';
import os from 'node:os';
import path from 'node:path';

export async function temporaryDirectory(t) {
  const parent = path.resolve(os.tmpdir());
  const directory = await mkdtemp(path.join(parent, 'mnemos-public-test-'));
  t.after(async () => {
    if (path.dirname(path.resolve(directory)) !== parent) throw new Error('Unsafe cleanup target');
    await rm(directory, { recursive: true, force: true });
  });
  return directory;
}

/**
 * Poll `predicate` every 10 ms until it returns a truthy value, and resolve with
 * that value; reject after timeoutMs, naming what was awaited.
 */
export async function until(predicate, label, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await predicate();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}`);
    await new Promise(resolve => setTimeout(resolve, 10));
  }
}

/**
 * Resolve with the promise, or reject with `message` after ms. The timer is
 * cleared as soon as the promise settles, so it never keeps a test file alive.
 */
export function withTimeout(promise, ms, message) {
  let timer;
  return Promise.race([promise, new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms);
  })]).finally(() => clearTimeout(timer));
}

export function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

/**
 * Linux only: a process that has exited but is never reaped (a zombie) while
 * the test runs. Its parent (`sleep`) never waits for children.
 */
export async function zombieProcess(t) {
  const { spawn } = await import('node:child_process');
  const { readFile } = await import('node:fs/promises');
  const parent = spawn('/bin/sh', ['-c', 'sleep 0.05 & echo $!; exec sleep 60'], { stdio: ['ignore', 'pipe', 'ignore'] });
  t.after(() => parent.kill('SIGKILL'));
  const pid = Number(await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('the zombie helper did not report its PID')), 10_000);
    parent.stdout.once('data', data => { clearTimeout(timer); resolve(String(data).trim()); });
    parent.once('error', error => { clearTimeout(timer); reject(error); });
    parent.once('close', () => { clearTimeout(timer); reject(new Error('the zombie helper exited early')); });
  }));
  const deadline = Date.now() + 5_000;
  for (;;) {
    const stat = await readFile(`/proc/${pid}/stat`, 'utf8');
    if (stat[stat.lastIndexOf(')') + 2] === 'Z') return { pid, parentPid: parent.pid };
    if (Date.now() > deadline) throw new Error('the process did not become a zombie');
    await new Promise(resolve => setTimeout(resolve, 10));
  }
}

/**
 * Fault injection: the next `count` calls of a node:fs/promises function whose
 * arguments satisfy `applies` fail with `code`. Modules import these functions
 * by name, and syncBuiltinESMExports() updates those bindings too, so this
 * reaches the code under test without changing it. Restored when the test ends.
 */
export function failFsPromises(t, name, applies, { code = 'EBUSY', count = 1 } = {}) {
  const fsPromises = createRequire(import.meta.url)('node:fs/promises');
  const original = fsPromises[name];
  let remaining = count;
  fsPromises[name] = async (...args) => {
    if (remaining > 0 && applies(...args)) {
      remaining--;
      throw Object.assign(new Error(`injected ${code} (${name})`), { code });
    }
    return original(...args);
  };
  syncBuiltinESMExports();
  t.after(() => { fsPromises[name] = original; syncBuiltinESMExports(); });
}

/**
 * A worker thread of this process running ES module `source` (parentPort and
 * workerData are in scope). next() resolves with its next message, send()
 * posts one to it, exited resolves when it ends. Terminated when the test ends.
 */
export function workerThread(t, source, workerData) {
  const worker = new Worker(`import { parentPort, workerData } from 'node:worker_threads';\n${source}`,
    { eval: true, type: 'module', workerData });
  t.after(() => worker.terminate());
  let failure = null;
  const failed = new Promise((resolve, reject) => worker.once('error', error => { failure = error; reject(error); }));
  failed.catch(() => {});
  const exited = new Promise(resolve => worker.once('exit', resolve));
  return {
    next: () => failure ? Promise.reject(failure) : Promise.race([new Promise(resolve => worker.once('message', resolve)), failed]),
    send: message => worker.postMessage(message),
    exited
  };
}
