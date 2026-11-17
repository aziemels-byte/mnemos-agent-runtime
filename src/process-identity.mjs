import { readFileSync } from 'node:fs';
import { hostname } from 'node:os';
import { threadId } from 'node:worker_threads';

// Internal module: who a process is, whether it still runs, and which recorded
// executions this process owns. Shared by the directory lock, the file-backed
// registries and the process supervisor. Not exported from index.mjs.

// Linux: a process that has exited but was not yet reaped by its parent (a
// zombie, state Z; or X while being reaped) still answers kill(pid, 0). Under an
// init that reaps slowly or never (for example a container without --init) it
// would look alive indefinitely. True only for a definite zombie; any doubt
// (no procfs, unreadable, unexpected format) keeps the process alive.
function exitedButUnreaped(pid) {
  const state = procStat(pid)?.[0];
  return state === 'Z' || state === 'X';
}

// Linux: the fields of /proc/<pid>/stat after "pid (comm)", starting with the
// state (field 3); comm may contain spaces and parentheses. Null when unavailable.
function procStat(pid) {
  if (process.platform !== 'linux') return null;
  let stat;
  try { stat = readFileSync(`/proc/${pid}/stat`, 'utf8'); }
  catch { return null; }
  const end = stat.lastIndexOf(')');
  return end < 0 ? null : stat.slice(end + 2).trim().split(' ');
}

const BOOT_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const TICKS = /^\d{1,20}$/;
let cachedBootId;
function bootId() {
  if (process.platform !== 'linux') return null;
  if (cachedBootId === undefined) {
    try {
      const id = readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim();
      cachedBootId = BOOT_ID.test(id) ? id : null;
    } catch { cachedBootId = null; }
  }
  return cachedBootId;
}

/**
 * Who a process is, so that a later check can tell it from an unrelated
 * process that reused its PID: { pid, host } and, on Linux, the boot ID and the
 * process start time in clock ticks since boot (field 22 of /proc/<pid>/stat).
 * Elsewhere only { pid, host }. Null when no such process is running.
 */
export function processIdentity(pid = process.pid) {
  if (!Number.isSafeInteger(pid) || pid <= 0) throw new TypeError('Invalid PID');
  if (pid !== process.pid) {
    try { process.kill(pid, 0); }
    catch (error) { if (error.code === 'ESRCH') return null; }
  }
  const identity = { pid, host: hostname() };
  const fields = procStat(pid);
  if (fields && ['Z', 'X'].includes(fields[0])) return null; // exited, awaiting reaping
  const boot = bootId();
  if (fields && boot && TICKS.test(fields[19] ?? '')) {
    identity.bootId = boot;
    identity.startTicks = fields[19];
  }
  return identity;
}

/**
 * Whether the process an identity describes is still running:
 * - ALIVE: the same process, verified by boot ID and start time.
 * - DEAD: provably gone (PID gone or a zombie, the host rebooted, or the PID now
 *   belongs to a process that started at another time).
 * - UNVERIFIED: a process with this PID runs, but the identity has no start time
 *   to compare (recorded elsewhere than Linux, or by an older version). It may be
 *   the same process or an unrelated one: never signal it on this evidence.
 * - OTHER_HOST, UNREADABLE: as processState().
 */
export function identityState(identity) {
  const basic = processState(identity);
  if (basic === 'OTHER_HOST' || basic === 'UNREADABLE') return basic;
  const recorded = identity.bootId !== undefined || identity.startTicks !== undefined;
  if (recorded && !(typeof identity.bootId === 'string' && BOOT_ID.test(identity.bootId) &&
    typeof identity.startTicks === 'string' && TICKS.test(identity.startTicks))) return 'UNREADABLE';
  if (recorded && bootId() !== null && identity.bootId !== bootId()) return 'DEAD'; // rebooted since
  if (basic === 'DEAD') return 'DEAD';
  const current = processIdentity(identity.pid);
  if (current === null) return 'DEAD';
  if (!recorded || current.startTicks === undefined) return 'UNVERIFIED';
  return current.startTicks === identity.startTicks ? 'ALIVE' : 'DEAD';
}

// A process is provably gone only when it ran on this host and its PID no
// longer exists, or it has exited and only awaits reaping. Anything else
// (other host, unreadable record, live or unknown PID) is treated as live: a
// stuck lock is recoverable, a double grant is not.
function processState(record) {
  if (!Number.isSafeInteger(record?.pid) || record.pid <= 0 || typeof record.host !== 'string') return 'UNREADABLE';
  if (record.host !== hostname()) return 'OTHER_HOST';
  if (record.pid === process.pid) return 'ALIVE';
  try { process.kill(record.pid, 0); }
  catch (error) { return error.code === 'ESRCH' ? 'DEAD' : 'ALIVE'; }
  return exitedButUnreaped(record.pid) ? 'DEAD' : 'ALIVE';
}

/**
 * The state of the process a record names (a lock holder, a job or effect
 * owner, a process recorded by a job): ALIVE, DEAD, OTHER_HOST or UNREADABLE.
 * A record with a start time (written on Linux by this version) is judged by
 * identityState(): DEAD also when its PID now belongs to another process,
 * including this one, or the host rebooted; all threads of one process share
 * its start time. A record without one (other platforms, earlier versions) is
 * judged by its PID alone: this process's own PID then always counts as ALIVE.
 * A start time that cannot be compared counts as ALIVE: never act on it.
 */
export function recordedProcessState(record) {
  if (record?.bootId === undefined && record?.startTicks === undefined) return processState(record);
  const state = identityState(record);
  return state === 'UNVERIFIED' ? 'ALIVE' : state;
}

let ownIdentity;
/**
 * This process's boot ID and start time ({} where unavailable), recorded with
 * every lock, job and process record it writes so that a later reader can tell
 * it from an unrelated process that reuses its PID.
 */
export function ownIdentityFields() {
  if (ownIdentity === undefined) {
    const { bootId: boot, startTicks } = processIdentity(process.pid) ?? {};
    ownIdentity = boot === undefined || startTicks === undefined ? {} : { bootId: boot, startTicks };
  }
  return { ...ownIdentity };
}

/**
 * A process identity as stored in a record: { pid, host } and, on Linux,
 * bootId and startTicks. The latter are optional (other platforms, earlier
 * versions) but never partial.
 */
export function validIdentity(identity) {
  const identified = identity?.bootId !== undefined || identity?.startTicks !== undefined;
  return Number.isSafeInteger(identity?.pid) && identity.pid > 0 &&
    typeof identity.host === 'string' && identity.host.length > 0 && identity.host.length <= 255 &&
    (!identified || (typeof identity.bootId === 'string' && BOOT_ID.test(identity.bootId) &&
      typeof identity.startTicks === 'string' && TICKS.test(identity.startTicks)));
}

// Executions (jobs, effects) running in this thread, listed before their record
// becomes visible and removed once they can no longer write it. Module state is
// per thread: other threads of this process keep their own lists.
const liveExecutions = new Set();

/** List an execution as running here; call before its record becomes visible. */
export function markExecutionLive(executionId) { liveExecutions.add(executionId); }

/** The execution will not write its record again. */
export function markExecutionEnded(executionId) { liveExecutions.delete(executionId); }

/** Whether the execution runs in this thread (not merely in this process). */
export function isExecutionLive(executionId) { return liveExecutions.has(executionId); }

/**
 * The owner to record for an execution started here: this process's identity
 * (on Linux with its boot ID and start time) and this thread's ID.
 */
export function executionOwner(executionId) {
  return { pid: process.pid, host: hostname(), executionId, threadId, ...ownIdentityFields() };
}

/**
 * The state of a recorded execution's owner (see executionOwner(); records of
 * earlier versions lack threadId, and bootId and startTicks off Linux): ALIVE,
 * DEAD (provably gone, so its work is not running), OTHER_HOST or UNREADABLE
 * (cannot be verified). Only DEAD permits reclassifying the owner's work.
 *
 * An owner with this process's PID whose execution is not listed in this thread:
 * - with another start time: an earlier process that had this PID: DEAD;
 * - in another thread of this process: ALIVE, for it may run there (a thread
 *   that ended while running it leaves it ALIVE until this process exits);
 * - in this thread, or recorded by an earlier version: it ended, or its final
 *   write failed: DEAD.
 * Off Linux there is no start time to compare, so a record of another thread
 * counts as ALIVE even if an earlier process with this PID wrote it.
 */
export function ownerState(owner) {
  if (owner?.pid === process.pid && owner.host === hostname()) {
    if (liveExecutions.has(owner.executionId)) return 'ALIVE';
    if (owner.bootId !== undefined || owner.startTicks !== undefined) {
      const state = recordedProcessState(owner);
      if (state !== 'ALIVE') return state; // an earlier process with this PID, or an unreadable identity
    }
    return owner.threadId !== undefined && owner.threadId !== threadId ? 'ALIVE' : 'DEAD';
  }
  return recordedProcessState(owner);
}
