import { createHash } from 'node:crypto';
import { open, readdir } from 'node:fs/promises';
import path from 'node:path';
import { readBoundedFile, syncDirectory } from './directory-lock.mjs';

// Internal module: the event history of the file-backed job store. Not exported
// from index.mjs; see FilePhaseJobRegistry.history() and searchHistory().
//
// Every write of a job record appends one event to that job's history file,
// <stem>.history.jsonl, under the same lock and after the record itself is
// written. The record stays the source of truth: an event never describes a
// change that was not stored. Each event carries the record's sequence, which
// grows by exactly one per write, so an event that could not be appended (a
// disk error, a full history) shows as a gap: a history is `complete` only when
// every sequence up to the record's is present.

export const JOB_EVENT_SCHEMA = 'mnemos.job-event.v1';
const MAX_HISTORY_BYTES = 1024 * 1024;
const HISTORY_FILE = /^[0-9a-f]{64}\.history\.jsonl$/;
const MAX_SEARCH_LIMIT = 10_000;

/** The file name stem shared by a job's record and its history. */
export const jobFileStem = jobId => createHash('sha256').update(jobId).digest('hex');
const historyFile = (directory, jobId) => path.join(directory, `${jobFileStem(jobId)}.history.jsonl`);

// The most significant change of a write names its event; `changed` lists every field that changed.
const TYPES = [
  ['RECONCILED', (previous, next) => !previous.reconciliation && Boolean(next.reconciliation)],
  ['ORPHANS_TERMINATED', (previous, next) => !previous.orphanTermination && Boolean(next.orphanTermination)],
  ['RESUMED', (previous, next) => previous.resumedBy === undefined && next.resumedBy !== undefined],
  ['STATUS_CHANGED', (previous, next) => previous.status !== next.status],
  ['CANCELLATION_REQUESTED', (previous, next) => !previous.cancellationRequested && next.cancellationRequested],
  ['DISPATCHED', (previous, next) => !previous.dispatched && next.dispatched],
  ['PROCESS_RECORDED', (previous, next) => JSON.stringify(previous.processes ?? []) !== JSON.stringify(next.processes ?? [])],
  ['PROGRESS_SAVED', (previous, next) => previous.progress?.sequence !== next.progress?.sequence],
  ['PHASE_CHANGED', (previous, next) => previous.phase !== next.phase]
];

/**
 * The event for a record write: previous is null when the record was created.
 * Identifying details only: results, progress state and evidence contents stay
 * in the record.
 */
export function jobEvent(previous, next) {
  const event = { schema: JOB_EVENT_SCHEMA, jobId: next.jobId, rootJobId: next.resumeOf?.rootJobId ?? next.jobId,
    sequence: next.sequence, at: next.updatedAt };
  if (previous === null) {
    return { ...event, type: 'CREATED', status: next.status, phase: next.phase, reason: next.reason, objective: next.objective,
      owner: { pid: next.owner.pid, host: next.owner.host }, ...(next.resumeOf ? { resumeOf: next.resumeOf } : {}) };
  }
  const changed = Object.keys({ ...previous, ...next })
    .filter(field => !['sequence', 'updatedAt'].includes(field) && JSON.stringify(previous[field]) !== JSON.stringify(next[field])).sort();
  const type = TYPES.find(([, applies]) => applies(previous, next))?.[0] ?? 'UPDATED';
  const details = {};
  if (previous.status !== next.status) details.previousStatus = previous.status;
  if (previous.phase !== next.phase) details.previousPhase = previous.phase;
  if (type === 'RECONCILED') {
    Object.assign(details, { disposition: next.reconciliation.disposition, evidenceSource: next.reconciliation.evidence.source,
      override: next.reconciliation.override !== null });
  }
  if (type === 'ORPHANS_TERMINATED') details.treesStopped = next.orphanTermination.receipts.length;
  if (type === 'RESUMED') details.resumedBy = next.resumedBy;
  if (type === 'PROGRESS_SAVED') details.progressSequence = next.progress.sequence;
  return { ...event, type, changed, status: next.status, phase: next.phase, reason: next.reason,
    effectAbsent: next.effectAbsent, ...details };
}

/**
 * Append an event to its job's history and flush it. Never throws: the record
 * write it follows has already committed, and a missing event shows as a gap.
 * Resolves to whether it was appended.
 */
export async function appendJobEvent(directory, event) {
  let line = JSON.stringify(event) + '\n';
  let handle;
  try {
    handle = await open(historyFile(directory, event.jobId), 'a+', 0o600);
    const { size } = await handle.stat();
    if (size > 0) {
      // A line cut off by a crash would swallow this event too: start a new line.
      const last = Buffer.alloc(1);
      await handle.read(last, 0, 1, size - 1);
      if (last[0] !== 0x0a) line = '\n' + line;
    }
    if (size + Buffer.byteLength(line) > MAX_HISTORY_BYTES) return false;
    await handle.write(line);
    await handle.sync();
    if (size === 0) await syncDirectory(directory); // a new file: flush its directory entry too
    return true;
  } catch {
    return false;
  } finally {
    await handle?.close().catch(() => {});
  }
}

// The events of one history file, in sequence order, and how many lines could not be read
// (a line cut off by a crash, or damaged). Missing file: no events.
async function readHistoryFile(file, jobId) {
  const bytes = await readBoundedFile(file, MAX_HISTORY_BYTES,
    () => Object.assign(new Error('Job history is not a file of at most 1 MiB'), { code: 'JOB_HISTORY_INVALID', file }));
  if (bytes === null) return { events: [], unreadableLines: 0 };
  const events = [];
  let unreadableLines = 0;
  for (const line of bytes.toString('utf8').split('\n')) {
    if (line === '') continue;
    let event;
    try { event = JSON.parse(line); } catch { unreadableLines++; continue; }
    if (event?.schema !== JOB_EVENT_SCHEMA || typeof event.jobId !== 'string' || (jobId !== undefined && event.jobId !== jobId) ||
      !Number.isSafeInteger(event.sequence) || event.sequence < 1 || typeof event.type !== 'string') { unreadableLines++; continue; }
    events.push(event);
  }
  events.sort((a, b) => a.sequence - b.sequence);
  return { events, unreadableLines };
}

/**
 * One job's history: { jobId, events (by sequence), complete, missing, unreadableLines }.
 * missing lists the ranges [from, to] of sequences up to `lastSequence` (the
 * record's) that have no event.
 */
export async function readJobHistory(directory, jobId, lastSequence) {
  const { events, unreadableLines } = await readHistoryFile(historyFile(directory, jobId), jobId);
  const present = new Set(events.map(event => event.sequence));
  const last = Math.max(lastSequence ?? 0, ...present);
  const missing = [];
  for (let sequence = 1; sequence <= last; sequence++) {
    if (present.has(sequence)) continue;
    const range = missing.at(-1);
    if (range && range[1] === sequence - 1) range[1] = sequence;
    else missing.push([sequence, sequence]);
  }
  return { jobId, events, complete: missing.length === 0 && unreadableLines === 0, missing, unreadableLines };
}

function searchFilters({ jobId, rootJobId, type, status, reason, since, until, limit = 1000 } = {}) {
  for (const [name, value] of Object.entries({ jobId, rootJobId, type, status, reason })) {
    if (value !== undefined && (typeof value !== 'string' || !value)) throw new TypeError(`${name} must be a non-empty string`);
  }
  for (const [name, value] of Object.entries({ since, until })) {
    if (value !== undefined && !(typeof value === 'string' && Number.isFinite(Date.parse(value)))) {
      throw new TypeError(`${name} must be a timestamp`);
    }
  }
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_SEARCH_LIMIT) throw new TypeError(`limit must be an integer between 1 and ${MAX_SEARCH_LIMIT}`);
  return { jobId, rootJobId, type, status, reason, since: since && Date.parse(since), until: until && Date.parse(until), limit };
}

/**
 * Events of every job matching all given filters (exact jobId, rootJobId, type,
 * status, reason; `at` within [since, until]), oldest first. With more than
 * `limit` (default 1000) matches, the most recent `limit` are returned and
 * `truncated` is true. Histories that cannot be read are listed in `invalid`.
 */
export async function searchJobHistory(directory, filters) {
  const { limit, since, until, ...exact } = searchFilters(filters);
  let names = [];
  try { names = await readdir(directory); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  if (exact.jobId !== undefined) names = names.filter(name => name === `${jobFileStem(exact.jobId)}.history.jsonl`);
  const matches = [];
  const invalid = [];
  let unreadableLines = 0;
  for (const name of names.filter(entry => HISTORY_FILE.test(entry))) {
    let history;
    try { history = await readHistoryFile(path.join(directory, name)); }
    catch (error) { invalid.push({ file: name, code: error.code ?? 'JOB_HISTORY_INVALID' }); continue; }
    unreadableLines += history.unreadableLines;
    for (const event of history.events) {
      if (Object.entries(exact).some(([field, value]) => value !== undefined && event[field] !== value)) continue;
      const at = Date.parse(event.at);
      if ((since !== undefined && at < since) || (until !== undefined && at > until)) continue;
      matches.push(event);
    }
  }
  matches.sort((a, b) => a.at.localeCompare(b.at) || a.jobId.localeCompare(b.jobId) || a.sequence - b.sequence);
  return { events: matches.slice(-limit), truncated: matches.length > limit, invalid, unreadableLines };
}
