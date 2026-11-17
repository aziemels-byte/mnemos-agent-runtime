import { createHash, randomUUID } from 'node:crypto';
import { link, mkdir, open, readdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { hostname } from 'node:os';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { ownIdentityFields, recordedProcessState } from './process-identity.mjs';

// Internal module shared by the file-backed stores. Not exported from index.mjs.

const UUID_PATTERN = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';
const TOKEN = new RegExp(`^${UUID_PATTERN}$`);
const LOCK_NAME = /^[a-z0-9][a-z0-9._-]*\.lock$/;
const TRANSIENT_RENAME = new Set(['EPERM', 'EACCES', 'EBUSY']);
// Filesystems that cannot flush a directory report one of these; anything else is a real error.
const DIRECTORY_SYNC_UNSUPPORTED = new Set(['EINVAL', 'ENOTSUP', 'EISDIR', 'EBADF']);
// Lock waiters' temporaries carry the PID in their name, so a file left empty by a
// process killed between creating and writing it can still be attributed.
const WAITER_TEMPORARY = new RegExp(`^${UUID_PATTERN}(?:\\.(\\d+))?\\.lock\\.tmp$`);
const sha256 = text => createHash('sha256').update(text).digest('hex');
const lockError = (code, message, details = {}) => Object.assign(new Error(message), { code, ...details });
// Lock records, break claims and waiters' temporaries name their writer. On
// Linux they also carry its boot ID and start time, so that a record left by an
// earlier process that had the same PID (a restarted container's PID 1, or any
// reused PID) is recognized as dead; see recordedProcessState().
const writerRecord = fields => JSON.stringify({ ...fields, pid: process.pid, host: hostname(), ...ownIdentityFields() });
const ownerRecord = () => writerRecord({ at: new Date().toISOString() });
const validToken = token => typeof token === 'string' && TOKEN.test(token);

/**
 * Flush a directory's entries (creations, renames) to stable storage, so that a
 * completed rename survives power loss. Windows cannot open a directory for
 * flushing; there this relies on NTFS metadata journaling and is best effort.
 */
export async function syncDirectory(directory) {
  if (process.platform === 'win32') return false;
  let handle;
  try {
    handle = await open(directory, 'r');
    await handle.sync();
    return true;
  } catch (error) {
    if (DIRECTORY_SYNC_UNSUPPORTED.has(error.code)) return false;
    throw error;
  } finally {
    await handle?.close();
  }
}

/**
 * Write a flushed temporary file in the target's directory, then rename it over
 * the target. Readers see either the old or the new content, never a mix.
 * With syncDirectory, the rename itself is flushed before this resolves.
 */
export async function replaceFile(target, text, suffix, { syncDirectory: flushDirectory = false } = {}) {
  const temporary = path.join(path.dirname(target), `${randomUUID()}${suffix}`);
  let file;
  try {
    file = await open(temporary, 'wx', 0o600);
    await file.writeFile(text, 'utf8');
    await file.sync();
    await file.close();
    file = null;
    const deadline = performance.now() + 2_000;
    for (let delay = 10; ; delay = Math.min(100, delay * 2)) {
      try { await rename(temporary, target); break; }
      catch (error) {
        // Windows readers and scanners can hold the replacement target open.
        if (!TRANSIENT_RENAME.has(error.code) || performance.now() >= deadline) throw error;
        await sleep(delay + Math.random() * 10);
      }
    }
    if (flushDirectory) await syncDirectory(path.dirname(target));
  } finally {
    await file?.close();
    await rm(temporary, { force: true });
  }
}

/**
 * Call `retry` in the background after the backoff for `attempt` (25 ms,
 * doubling, at most 2 s), without keeping the process alive. The one retry
 * policy of the file-backed stores, and only for rewriting state under a
 * condition that makes a repeat harmless: a record still owned by this
 * execution (compare-and-swap), a lock still carrying this token. Never use it
 * to repeat an effect: an effect whose outcome is unknown is UNKNOWN.
 */
export function retryLater(attempt, retry) {
  const timer = setTimeout(retry, Math.min(25 * 2 ** attempt, 2_000));
  timer.unref?.();
}

/**
 * The bytes of a state file, or null when it does not exist. A path that is not
 * a regular file, or a file larger than maxBytes (also one that grows while it
 * is read), is rejected with the error invalid(reason) returns, reason being
 * 'NOT_A_FILE' or 'TOO_LARGE': state is never read without bound, and never
 * treated as absent unless it is.
 */
export async function readBoundedFile(file, maxBytes, invalid) {
  let handle;
  try { handle = await open(file, 'r'); }
  catch (error) {
    if (error.code === 'ENOENT') return null;
    throw error;
  }
  try {
    const stat = await handle.stat();
    if (!stat.isFile()) throw invalid('NOT_A_FILE');
    if (stat.size > maxBytes) throw invalid('TOO_LARGE');
    const bytes = await handle.readFile();
    if (bytes.length > maxBytes) throw invalid('TOO_LARGE');
    return bytes;
  } finally {
    await handle.close();
  }
}

/**
 * Same-host mutual exclusion through one lock file in a directory.
 * - link() publishes a fully written lock record atomically and fails if a lock exists.
 * - A lock is broken automatically only when its holder ran on this host and its
 *   PID is gone, and only by the process holding the per-token break claim.
 * - Anything else waits and then fails with `timeoutCode`; operators can use
 *   inspect() and forceUnlock().
 */
export class DirectoryLock {
  #claim;
  #parentsFlushed = false;

  constructor({ directory, name, timeoutMs = 5_000, timeoutCode = 'LOCK_TIMEOUT', ownedTemporary = () => false } = {}) {
    if (typeof directory !== 'string' || !directory.trim()) throw new TypeError('A lock directory is required');
    if (typeof name !== 'string' || !LOCK_NAME.test(name)) throw new TypeError('Invalid lock name');
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) throw new RangeError('Invalid lock timeout');
    this.directory = path.resolve(directory);
    this.name = name;
    this.lockFile = path.join(this.directory, name);
    this.timeoutMs = timeoutMs;
    this.timeoutCode = timeoutCode;
    this.ownedTemporary = ownedTemporary;
    const escaped = name.replace(/[.]/g, '\\.');
    this.#claim = new RegExp(`^${escaped}\\.(${UUID_PATTERN})\\.break$`);
  }

  /**
   * Run `operation` while holding the lock; housekeeping runs first. Resolves or
   * rejects with the operation's own outcome: a failure to unlock afterwards
   * never replaces it (see #release).
   */
  async run(operation) {
    const token = await this.#acquire();
    try {
      await this.#sweep(token);
      return await operation();
    } finally {
      await this.#release(token);
    }
  }

  /** Read-only view for operators; never takes the lock. */
  async inspect() {
    const pendingBreaks = [];
    let names = [];
    try { names = await readdir(this.directory); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    for (const name of names) {
      const match = this.#claim.exec(name);
      if (!match) continue;
      let claimant = null;
      try { claimant = JSON.parse(await readFile(path.join(this.directory, name), 'utf8')); } catch { /* unreadable */ }
      pendingBreaks.push({ token: match[1], claimant, claimantState: recordedProcessState(claimant) });
    }
    let text;
    try { text = await readFile(this.lockFile); }
    catch (error) {
      if (error.code === 'ENOENT') return { status: 'UNLOCKED', lockFile: this.lockFile, pendingBreaks };
      throw error;
    }
    let holder = null;
    try {
      const record = JSON.parse(text);
      if (validToken(record?.token)) {
        holder = { token: record.token, pid: record.pid, host: record.host, lockedAt: record.lockedAt };
        for (const field of ['bootId', 'startTicks']) if (record[field] !== undefined) holder[field] = record[field];
      }
    } catch { /* reported as UNREADABLE */ }
    return { status: 'LOCKED', lockFile: this.lockFile, lockId: sha256(text),
      holder, holderState: holder ? recordedProcessState(holder) : 'UNREADABLE', pendingBreaks };
  }

  /**
   * Operator override for a lock the automatic rules refuse to break (another
   * host, unreadable record, or a breaker that died). Removes the lock only if
   * it is byte-for-byte the one inspected, and never while its holder is alive.
   */
  async forceUnlock({ expectedLockId, explicitAuthorization = false } = {}) {
    if (explicitAuthorization !== true) throw new Error('Explicit authorization required');
    if (typeof expectedLockId !== 'string' || !/^[0-9a-f]{64}$/.test(expectedLockId)) throw new TypeError('Invalid expectedLockId');
    const inspected = await this.inspect();
    if (inspected.status === 'UNLOCKED') return { status: 'NOT_LOCKED' };
    if (inspected.lockId !== expectedLockId) {
      throw lockError('LOCK_CHANGED', 'The lock changed since it was inspected; inspect it again', { lockFile: this.lockFile });
    }
    if (inspected.holderState === 'ALIVE') {
      throw lockError('LOCK_HOLDER_ALIVE', 'The lock holder is still running', { holder: inspected.holder });
    }
    const token = inspected.holder?.token;
    const claim = token ? this.#claimFile(token) : null;
    if (claim) {
      // Holding the claim stops automatic breakers from replacing the lock under us.
      // An existing claim from a live breaker means recovery is already under way.
      const existing = inspected.pendingBreaks.find(entry => entry.token === token);
      if (existing?.claimantState === 'ALIVE') {
        throw lockError('LOCK_BREAK_IN_PROGRESS', 'Another process is breaking this lock', { claimant: existing.claimant });
      }
      if (!existing) {
        try { await this.#publishClaim(claim); }
        catch (error) {
          if (error.code === 'EEXIST') throw lockError('LOCK_BREAK_IN_PROGRESS', 'Another process is breaking this lock');
          throw error;
        }
      }
    }
    try {
      let text = null;
      try { text = await readFile(this.lockFile); } catch (error) { if (error.code !== 'ENOENT') throw error; }
      if (text === null) return { status: 'NOT_LOCKED' };
      if (sha256(text) !== expectedLockId) {
        throw lockError('LOCK_CHANGED', 'The lock changed since it was inspected; inspect it again', { lockFile: this.lockFile });
      }
      await rm(this.lockFile);
      return { status: 'LOCK_FORCE_RELEASED', holder: inspected.holder, holderState: inspected.holderState };
    } finally {
      if (claim) await rm(claim, { force: true });
    }
  }

  // Say who holds the lock and what to do, using the same read-only view as inspect().
  async #timeoutError() {
    const base = `Timed out after ${this.timeoutMs} ms waiting for ${this.name}`;
    let view;
    try { view = await this.inspect(); }
    catch { return lockError(this.timeoutCode, `${base}. Inspect it with inspectLock().`, { lockFile: this.lockFile }); }
    if (view.status === 'UNLOCKED') {
      return lockError(this.timeoutCode, `${base}; it was released just now, so retrying may succeed.`,
        { lockFile: this.lockFile, holder: null, holderState: null, pendingBreaks: view.pendingBreaks });
    }
    const who = view.holder ? `held by PID ${view.holder.pid} on ${view.holder.host} since ${view.holder.lockedAt}` : 'its lock record is unreadable';
    const next = {
      ALIVE: 'the holder is still running (possibly paused): wait, or stop that process',
      DEAD: view.pendingBreaks.some(entry => entry.token === view.holder?.token)
        ? 'the holder exited and a recovery attempt is pending: if it does not clear, use forceUnlock()'
        : 'the holder exited: a retry recovers the lock automatically',
      OTHER_HOST: 'the holder cannot be checked from this host: confirm it is gone, then use forceUnlock()',
      UNREADABLE: 'confirm no operation is running, then use forceUnlock()'
    }[view.holderState];
    return lockError(this.timeoutCode, `${base}: ${who} (${view.holderState}); ${next}. See inspectLock() and the README runbook.`,
      { lockFile: this.lockFile, lockId: view.lockId, holder: view.holder, holderState: view.holderState, pendingBreaks: view.pendingBreaks });
  }

  #claimFile(token) {
    return path.join(this.directory, `${this.name}.${token}.break`);
  }

  async #readHolder() {
    try {
      const holder = JSON.parse(await readFile(this.lockFile, 'utf8'));
      return validToken(holder?.token) ? holder : { unreadable: true };
    } catch (error) {
      if (error.code === 'ENOENT') return null;
      return { unreadable: true };
    }
  }

  // Create the directory if needed. When this instance first sees it, or has just
  // created it, flush its entry (and those of created parents) into their parents,
  // so a power loss cannot drop the state directory itself.
  async #ensureDirectory() {
    const created = await mkdir(this.directory, { recursive: true, mode: 0o700 });
    if (this.#parentsFlushed && !created) return;
    const parents = [];
    for (let directory = this.directory; ; directory = path.dirname(directory)) {
      parents.push(path.dirname(directory));
      if (!created || directory === path.resolve(created) || path.dirname(directory) === directory) break;
    }
    for (const parent of parents.reverse()) await syncDirectory(parent);
    this.#parentsFlushed = true;
  }

  async #acquire() {
    await this.#ensureDirectory();
    const token = randomUUID();
    const record = writerRecord({ token, lockedAt: new Date().toISOString() });
    const temporary = this.#temporary(token);
    const deadline = Date.now() + this.timeoutMs;
    let delay = 2;
    try {
      await writeFile(temporary, record, { flag: 'wx', mode: 0o600 });
      for (;;) {
        try { await link(temporary, this.lockFile); return token; }
        catch (error) { if (error.code !== 'EEXIST') throw error; }
        const holder = await this.#readHolder();
        if (holder === null) continue; // released between link() and read; retry now
        if (!holder.unreadable && recordedProcessState(holder) === 'DEAD' && await this.#breakStale(holder.token)) continue;
        if (Date.now() >= deadline) throw await this.#timeoutError();
        await sleep(delay);
        delay = Math.min(delay * 2, 50);
      }
    } finally {
      await rm(temporary, { force: true }).catch(() => {});
    }
  }

  #temporary(token) {
    return path.join(this.directory, `${token}.${process.pid}.lock.tmp`);
  }

  // Publish a complete claimant record so inspection never mistakes a live
  // breaker's partially written claim for an abandoned legacy claim.
  async #publishClaim(claim) {
    const temporary = this.#temporary(randomUUID());
    try {
      await writeFile(temporary, ownerRecord(), { flag: 'wx', mode: 0o600 });
      await link(temporary, claim);
    } finally {
      await rm(temporary, { force: true }).catch(() => {});
    }
  }

  // Only the process holding the per-token claim may remove a lock carrying that
  // token, and the holder is dead, so re-reading before removal cannot remove a
  // newer lock that another process created after the stale one was observed.
  async #breakStale(staleToken) {
    const claim = this.#claimFile(staleToken);
    try { await this.#publishClaim(claim); }
    catch (error) { if (error.code === 'EEXIST') return false; throw error; }
    try {
      if ((await this.#readHolder())?.token === staleToken) await rm(this.lockFile);
      return true;
    } finally {
      await rm(claim, { force: true });
    }
  }

  // Only an owner removes its own lock; a live owner's lock is never broken.
  // Never throws: the operation has already committed (or failed) under the
  // lock, and reporting a failed unlock instead would misstate that outcome. A
  // lock that cannot be removed now (for example a scanner holding the file on
  // Windows) is removed in the background while this process runs; meanwhile it
  // blocks other callers as a live holder's lock does. Should this process exit
  // first, its lock is stale and the next caller removes it.
  async #release(token, attempt = 0) {
    try {
      if ((await this.#readHolder())?.token === token) await rm(this.lockFile, { force: true });
    } catch {
      retryLater(attempt, () => this.#release(token, attempt + 1));
    }
  }

  // Runs while holding the lock, so a leftover temporary owned by this lock's
  // writer is from a crash. Files of processes that may still run are kept.
  async #sweep(ownToken) {
    try {
      for (const name of await readdir(this.directory)) {
        const file = path.join(this.directory, name);
        if (this.ownedTemporary(name)) { await rm(file, { force: true }); continue; }
        // We hold the lock, so a claim names a lock that is gone. Lock tokens are
        // random and never reissued, so such a claim can no longer protect anything.
        const claim = this.#claim.exec(name);
        if (claim && claim[1] !== ownToken) { await rm(file, { force: true }); continue; }
        // Lock waiters' temporaries may belong to any lock in this directory;
        // only those of a provably dead process are removed.
        const waiterName = WAITER_TEMPORARY.exec(name);
        if (!waiterName) continue;
        let waiter = null;
        try { waiter = JSON.parse(await readFile(file, 'utf8')); } catch { /* empty or torn record */ }
        // Without a readable record, fall back to the PID in the name on this host.
        // If that guess is wrong, the live waiter's link() fails with ENOENT: an error, never a double grant.
        if (!waiter && waiterName[1]) waiter = { pid: Number(waiterName[1]), host: hostname() };
        if (recordedProcessState(waiter) === 'DEAD') await rm(file, { force: true });
      }
    } catch { /* Housekeeping never fails an otherwise valid operation. */ }
  }
}
