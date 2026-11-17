import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { appendFile, readFile, writeFile } from 'node:fs/promises';
import { hostname } from 'node:os';
import path from 'node:path';
import { FilePhaseJobRegistry } from '../src/index.mjs';
import { FileJobStore } from '../src/job-store.mjs';
import { deferred, failFsPromises, temporaryDirectory, until } from './helpers.mjs';

const fingerprintOf = input => createHash('sha256').update(JSON.stringify(input)).digest('hex');
const historyFile = (directory, jobId) => path.join(directory, `${createHash('sha256').update(jobId).digest('hex')}.history.jsonl`);
const evidence = { authoritative: true, source: 'billing-api:charge/42' };
const types = history => history.events.map(event => event.type);

// A process that has exited: a job recorded under it has a provably dead owner.
async function deadOwner() {
  const child = spawn(process.execPath, ['-e', ''], { stdio: 'ignore' });
  await new Promise(resolve => child.once('exit', resolve));
  return { pid: child.pid, host: hostname(), executionId: 'gone' };
}

test('every write of a job is one event, in order, and the history is complete', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const jobs = new FilePhaseJobRegistry({ directory, execute: async ({ setPhase }) => {
    setPhase('fetching');
    await until(async () => (await store.read('report_1')).phase === 'fetching', 'the phase to be recorded');
    setPhase('writing');
    return { status: 'COMPLETED', rows: 3 };
  } });
  await jobs.start({ jobId: 'report_1', objective: 'Create the report' });
  const done = await jobs.wait('report_1');
  const history = await jobs.history('report_1');
  assert.equal(history.complete, true);
  assert.deepEqual(history.missing, []);
  assert.deepEqual(history.events.map(event => event.sequence), Array.from({ length: done.sequence }, (_, index) => index + 1));
  assert.deepEqual([types(history)[0], types(history)[1], types(history).at(-1)], ['CREATED', 'DISPATCHED', 'STATUS_CHANGED']);
  assert.ok(types(history).includes('PHASE_CHANGED'));
  const [created] = history.events;
  assert.deepEqual([created.objective, created.status, created.phase, created.owner.pid], ['Create the report', 'RUNNING', 'queued', process.pid]);
  const outcome = history.events.at(-1);
  assert.deepEqual([outcome.previousStatus, outcome.status, outcome.phase, outcome.reason], ['RUNNING', 'COMPLETED', 'writing', null]);
  assert.ok(outcome.changed.includes('result'));
  assert.equal(JSON.stringify(history).includes('rows'), false, 'results stay in the record');
  assert.equal(await jobs.history('never_started'), null);
});

test('a cancellation is recorded as requested, then as its outcome', async t => {
  const directory = await temporaryDirectory(t);
  const running = deferred(); // cancel only once the executor runs, so that the job was dispatched
  const jobs = new FilePhaseJobRegistry({ directory, execute: ({ signal }) => new Promise(resolve => {
    signal.addEventListener('abort', () => resolve({ status: 'CANCELLED', termination: { confirmed: true } }), { once: true });
    running.resolve();
  }) });
  await jobs.start({ jobId: 'export_1', objective: 'Export' });
  await running.promise;
  await jobs.requestCancellation('export_1', { timeoutMs: 5000 });
  await jobs.wait('export_1');
  const history = await jobs.history('export_1');
  assert.equal(history.complete, true);
  assert.ok(types(history).includes('CANCELLATION_REQUESTED'));
  assert.deepEqual([history.events.at(-1).type, history.events.at(-1).status, history.events.at(-1).reason],
    ['STATUS_CHANGED', 'CANCELLED', 'CANCELLED_BY_REQUEST']);
});

test('a dead owner, a resume and a reconciliation are recorded, and attempts are found by their root', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  for (const jobId of ['import', 'charge']) {
    const { record } = await store.create({ jobId, objective: jobId, fingerprint: fingerprintOf(null), owner: await deadOwner(),
      resumable: jobId === 'import' });
    await store.update(jobId, { expectedSequence: record.sequence, changes: { dispatched: true, dispatchedAt: new Date().toISOString() } });
  }
  const jobs = new FilePhaseJobRegistry({ directory, execute: async () => ({ status: 'COMPLETED' }) });
  const lost = await jobs.get('import'); // its owner died after dispatch: reclassified, and recorded
  assert.equal(lost.reason, 'OWNER_DIED_AFTER_DISPATCH');
  await jobs.resume({ jobId: 'import', expectedSequence: lost.sequence, successorJobId: 'import_2', explicitAuthorization: true });
  await jobs.wait('import_2');
  assert.deepEqual(types(await jobs.history('import')), ['CREATED', 'DISPATCHED', 'STATUS_CHANGED', 'RESUMED']);
  const reclassified = (await jobs.history('import')).events[2];
  assert.deepEqual([reclassified.status, reclassified.reason], ['UNKNOWN', 'OWNER_DIED_AFTER_DISPATCH']);
  assert.equal((await jobs.history('import')).events[3].resumedBy, 'import_2');
  const successor = await jobs.history('import_2');
  assert.deepEqual(types(successor), ['CREATED', 'DISPATCHED', 'STATUS_CHANGED']);
  assert.deepEqual(successor.events[0].resumeOf, { jobId: 'import', rootJobId: 'import', attempt: 2 });
  const attempts = await jobs.searchHistory({ rootJobId: 'import' });
  assert.deepEqual(new Set(attempts.events.map(event => event.jobId)), new Set(['import', 'import_2']));
  assert.equal(attempts.events.length, 7);

  const charge = await jobs.get('charge');
  await jobs.reconcile({ jobId: 'charge', expectedSequence: charge.sequence, disposition: 'FAILED',
    evidence: { ...evidence, effectAbsent: true }, explicitAuthorization: true });
  const reconciled = (await jobs.history('charge')).events.at(-1);
  assert.deepEqual([reconciled.type, reconciled.status, reconciled.disposition, reconciled.evidenceSource, reconciled.previousStatus],
    ['RECONCILED', 'FAILED', 'FAILED', evidence.source, 'UNKNOWN']);
});

test('search filters by job, type, status, reason and time; the most recent are kept over the limit', async t => {
  const directory = await temporaryDirectory(t);
  const outcomes = { ok_1: 'COMPLETED', ok_2: 'COMPLETED', bad_1: 'FAILED' };
  const jobs = new FilePhaseJobRegistry({ directory, execute: async ({ jobId }) => ({ status: outcomes[jobId] }) });
  for (const jobId of Object.keys(outcomes)) {
    await jobs.start({ jobId, objective: jobId });
    await jobs.wait(jobId);
  }
  const failed = await jobs.searchHistory({ type: 'STATUS_CHANGED', status: 'FAILED' });
  assert.deepEqual(failed.events.map(event => [event.jobId, event.reason]), [['bad_1', 'EXECUTOR_REPORTED_FAILURE']]);
  assert.equal((await jobs.searchHistory({ reason: 'EXECUTOR_REPORTED_FAILURE' })).events.length, 1);
  assert.deepEqual((await jobs.searchHistory({ jobId: 'ok_2' })).events.map(event => event.jobId), ['ok_2', 'ok_2', 'ok_2']);
  const all = await jobs.searchHistory({});
  assert.deepEqual([all.events.length, all.truncated], [9, false]);
  assert.deepEqual(all.events.map(event => event.at), all.events.map(event => event.at).sort(), 'oldest first');
  const recent = await jobs.searchHistory({ limit: 2 });
  assert.deepEqual([recent.events, recent.truncated], [all.events.slice(-2), true]);
  const middle = all.events[4].at;
  assert.ok((await jobs.searchHistory({ since: middle })).events.every(event => event.at >= middle));
  assert.ok((await jobs.searchHistory({ until: middle })).events.every(event => event.at <= middle));
  assert.deepEqual((await jobs.searchHistory({ until: '2000-01-01T00:00:00.000Z' })).events, []);
  for (const invalid of [{ type: 5 }, { since: 'yesterday' }, { limit: 0 }, { limit: 10_001 }, { status: '' }]) {
    await assert.rejects(jobs.searchHistory(invalid), TypeError);
  }
});

test('an event that could not be appended is a gap, never a failed write', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const { record } = await store.create({ jobId: 'job_1', objective: 'Job', fingerprint: fingerprintOf(null), owner: await deadOwner() });
  failFsPromises(t, 'open', file => String(file).endsWith('.history.jsonl'), { code: 'ENOSPC' });
  const dispatched = await store.update('job_1', { expectedSequence: record.sequence, // the record write still succeeds
    changes: { dispatched: true, dispatchedAt: new Date().toISOString() } });
  await store.update('job_1', { expectedSequence: dispatched.sequence, changes: { phase: 'working' } });
  const history = await store.history('job_1');
  assert.deepEqual([history.complete, history.missing, history.events.map(event => event.sequence)], [false, [[2, 2]], [1, 3]]);
});

test('a line cut off by a crash is reported, and the next event is still recorded whole', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const { record } = await store.create({ jobId: 'job_1', objective: 'Job', fingerprint: fingerprintOf(null), owner: await deadOwner() });
  await appendFile(historyFile(directory, 'job_1'), '{"schema":"mnemos.job-event.v1","jobId":"job_1","seq'); // a crash mid-append
  await store.update('job_1', { expectedSequence: record.sequence, changes: { dispatched: true, dispatchedAt: new Date().toISOString() } });
  const history = await store.history('job_1');
  assert.deepEqual([history.events.map(event => [event.sequence, event.type]), history.unreadableLines, history.complete],
    [[[1, 'CREATED'], [2, 'DISPATCHED']], 1, false]);
});

test('a full history stops growing; later events show as a gap', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const { record } = await store.create({ jobId: 'job_1', objective: 'Job', fingerprint: fingerprintOf(null), owner: await deadOwner() });
  const file = historyFile(directory, 'job_1');
  const first = await readFile(file, 'utf8');
  await writeFile(file, first + '\n'.repeat(1024 * 1024 - Buffer.byteLength(first) - 10)); // just under 1 MiB
  await store.update('job_1', { expectedSequence: record.sequence, changes: { dispatched: true, dispatchedAt: new Date().toISOString() } });
  const history = await store.history('job_1');
  assert.deepEqual([history.events.length, history.missing], [1, [[2, 2]]]);
  assert.equal((await readFile(file)).length, Buffer.byteLength(first) + 1024 * 1024 - Buffer.byteLength(first) - 10, 'nothing appended');
});
