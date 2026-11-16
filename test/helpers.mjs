import { mkdtemp, rm } from 'node:fs/promises';
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
