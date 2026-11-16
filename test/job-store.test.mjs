import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import fs from 'node:fs';
import { readdir, readFile, writeFile, copyFile, mkdir } from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import { hostname } from 'node:os';
import path from 'node:path';
import { FileJobStore, JOB_RECORD_SCHEMA, MAX_RESULT_BYTES, boundResult } from '../src/job-store.mjs';
import { FileCheckpointStore, FileIdempotencyRegistry, FileResourceLeaseManager } from '../src/index.mjs';
import { temporaryDirectory } from './helpers.mjs';

const fingerprint = text => createHash('sha256').update(text).digest('hex');
const owner = () => ({ pid: process.pid, host: hostname(), executionId: randomUUID() });
const newJob = (jobId, extra = {}) => ({ jobId, objective: 'Produce a report', fingerprint: fingerprint(String(jobId)), owner: owner(), ...extra });
const recordFile = (directory, jobId) => path.join(directory, `${fingerprint(jobId)}.job.json`);

async function dispatched(store, jobId) {
  const { record } = await store.create(newJob(jobId));
  return store.update(jobId, { expectedSequence: record.sequence,
    changes: { dispatched: true, dispatchedAt: new Date().toISOString() } });
}

test('a created record is read back identically by a new store instance', async t => {
  const directory = await temporaryDirectory(t);
  const job = newJob('job_1');
  const { created, record } = await new FileJobStore({ directory }).create(job);
  assert.equal(created, true);
  assert.deepEqual(
    [record.status, record.phase, record.sequence, record.dispatched, record.reconciliationRequired, record.result],
    ['RUNNING', 'queued', 1, false, false, null]);
  assert.deepEqual(record.owner, job.owner);

  const restarted = new FileJobStore({ directory });
  assert.deepEqual(await restarted.read('job_1'), record);
  assert.equal(await restarted.read('missing'), null);
  const stored = JSON.parse(await readFile(recordFile(directory, 'job_1'), 'utf8'));
  assert.equal(stored.schema, JOB_RECORD_SCHEMA);
});

test('creating an existing job returns the stored record and writes nothing', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const first = await store.create(newJob('job_1'));
  const again = await new FileJobStore({ directory }).create(newJob('job_1', { objective: 'Different', fingerprint: fingerprint('other') }));
  assert.equal(again.created, false);
  assert.deepEqual(again.record, first.record); // the caller decides whether the identity conflicts
});

test('a full lifecycle survives a restart after every transition', async t => {
  const directory = await temporaryDirectory(t);
  const at = () => new Date().toISOString();
  let record = (await new FileJobStore({ directory }).create(newJob('job_1'))).record;
  const steps = [
    { dispatched: true, dispatchedAt: at() },
    { phase: 'full_regression' },
    { cancellationRequested: true, cancellationRequestedAt: at() },
    { status: 'CANCELLED', cancellationConfirmed: true, effectAbsent: false, reason: 'CANCELLED_BY_REQUEST', result: { exitCode: null } }
  ];
  for (const changes of steps) {
    const store = new FileJobStore({ directory }); // a fresh process for every step
    assert.deepEqual(await store.read('job_1'), record);
    const next = await store.update('job_1', { expectedSequence: record.sequence, changes });
    assert.equal(next.sequence, record.sequence + 1);
    record = next;
  }
  const final = await new FileJobStore({ directory }).read('job_1');
  assert.deepEqual(final, record);
  assert.deepEqual([final.status, final.phase, final.dispatched, final.cancellationRequested, final.cancellationConfirmed, final.sequence],
    ['CANCELLED', 'full_regression', true, true, true, 5]);
});

test('updates are compare-and-swap on the sequence', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const record = await dispatched(store, 'job_1');
  await store.update('job_1', { expectedSequence: record.sequence, changes: { phase: 'build' } });
  await assert.rejects(store.update('job_1', { expectedSequence: record.sequence, changes: { phase: 'stale' } }), error =>
    error.code === 'JOB_RECORD_CHANGED' && error.expectedSequence === 2 && error.currentSequence === 3);
  assert.equal((await store.read('job_1')).phase, 'build');
  await assert.rejects(store.update('missing', { expectedSequence: 1, changes: {} }), { code: 'JOB_NOT_FOUND' });
  for (const invalid of [0, 1.5, '1', undefined]) {
    await assert.rejects(store.update('job_1', { expectedSequence: invalid, changes: {} }), /expectedSequence/);
  }
});

test('the state machine refuses invalid transitions and writes nothing', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const at = new Date().toISOString();
  const refuse = async (jobId, changes, code) => {
    const before = await store.read(jobId);
    await assert.rejects(store.update(jobId, { expectedSequence: before.sequence, changes }), { code });
    assert.deepEqual(await store.read(jobId), before);
  };

  const queued = (await store.create(newJob('queued'))).record;
  await refuse('queued', { status: 'COMPLETED' }, 'JOB_TRANSITION_INVALID'); // never dispatched
  await refuse('queued', { objective: 'Rewritten' }, 'JOB_RECORD_INVALID'); // identity
  await refuse('queued', { sequence: 9 }, 'JOB_RECORD_INVALID');
  await refuse('queued', { status: 'DONE' }, 'JOB_RECORD_INVALID');
  await refuse('queued', { phase: 'bad phase' }, 'JOB_RECORD_INVALID');
  await refuse('queued', { dispatched: true }, 'JOB_RECORD_INVALID'); // no timestamp
  await refuse('queued', { status: 'UNKNOWN' }, 'JOB_RECORD_INVALID'); // must require reconciliation
  await refuse('queued', { status: 'FAILED', reconciliationRequired: true }, 'JOB_RECORD_INVALID');
  await refuse('queued', { status: 'FAILED', cancellationConfirmed: true }, 'JOB_RECORD_INVALID');
  await refuse('queued', { effectAbsent: true }, 'JOB_RECORD_INVALID'); // only for FAILED or CANCELLED
  await refuse('queued', { result: { early: true } }, 'JOB_RECORD_INVALID'); // no result while RUNNING
  await refuse('queued', { reason: 'lower case' }, 'JOB_RECORD_INVALID');
  assert.equal((await store.read('queued')).sequence, queued.sequence);

  const running = await dispatched(store, 'running');
  await refuse('running', { dispatched: false, dispatchedAt: null }, 'JOB_TRANSITION_INVALID');
  await refuse('running', { dispatchedAt: at.replace(/^\d{4}/, '2000') }, 'JOB_TRANSITION_INVALID');
  const cancelling = await store.update('running', { expectedSequence: running.sequence,
    changes: { cancellationRequested: true, cancellationRequestedAt: at } });
  await refuse('running', { cancellationRequested: false, cancellationRequestedAt: null }, 'JOB_TRANSITION_INVALID');

  // UNKNOWN leaves only to a definite outcome; definite outcomes are final.
  const unknown = await store.update('running', { expectedSequence: cancelling.sequence,
    changes: { status: 'UNKNOWN', reconciliationRequired: true, reason: 'INTERRUPTED_AFTER_DISPATCH' } });
  await refuse('running', { status: 'RUNNING', reconciliationRequired: false, reason: null }, 'JOB_TRANSITION_INVALID');
  await refuse('running', { phase: 'later' }, 'JOB_TRANSITION_INVALID');
  await refuse('running', { status: 'COMPLETED', reconciliationRequired: false, reason: 'RECONCILED' }, 'JOB_RECORD_INVALID'); // no audit
  const audit = previousStatus => ({ at, disposition: 'COMPLETED', override: null,
    evidence: { authoritative: true, source: 'ticket' }, previous: { status: previousStatus, reason: 'INTERRUPTED_AFTER_DISPATCH' } });
  await refuse('running', { status: 'COMPLETED', reconciliationRequired: false, reason: 'RECONCILED', reconciliation: audit('RUNNING') },
    'JOB_TRANSITION_INVALID'); // names the wrong previous state
  await refuse('running', { status: 'COMPLETED', reconciliationRequired: false, reason: 'RECONCILED',
    reconciliation: { ...audit('UNKNOWN'), disposition: 'FAILED' } }, 'JOB_RECORD_INVALID');
  await refuse('running', { status: 'COMPLETED', reconciliationRequired: false, reason: 'RECONCILED',
    reconciliation: { ...audit('UNKNOWN'), evidence: { source: 'ticket' } } }, 'JOB_RECORD_INVALID'); // not authoritative
  const completed = await store.update('running', { expectedSequence: unknown.sequence,
    changes: { status: 'COMPLETED', reconciliationRequired: false, reason: 'RECONCILED', result: { ok: true }, reconciliation: audit('UNKNOWN') } });
  await refuse('running', { status: 'FAILED' }, 'JOB_TRANSITION_INVALID');
  await refuse('running', { result: { ok: false } }, 'JOB_TRANSITION_INVALID');
  assert.equal((await store.read('running')).sequence, completed.sequence);
});

test('results are plain JSON of at most 64 KiB, stored canonically', async t => {
  assert.deepEqual(boundResult({ b: [1, 'x'], a: null }), { result: { a: null, b: [1, 'x'] }, resultDiscarded: false });
  assert.deepEqual(boundResult(undefined), { result: null, resultDiscarded: false });
  for (const value of [{ handle: () => {} }, new Map(), NaN, { when: new Date() }, 'x'.repeat(MAX_RESULT_BYTES)]) {
    assert.deepEqual(boundResult(value), { result: null, resultDiscarded: true });
  }
  const largest = 'x'.repeat(MAX_RESULT_BYTES - 2); // JSON quotes make it exactly 64 KiB
  assert.equal(boundResult(largest).resultDiscarded, false);

  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const running = await dispatched(store, 'job_1');
  await assert.rejects(store.update('job_1', { expectedSequence: running.sequence,
    changes: { status: 'COMPLETED', result: { handle: () => {} } } }), { code: 'JOB_RECORD_INVALID' });
  await assert.rejects(store.update('job_1', { expectedSequence: running.sequence,
    changes: { status: 'COMPLETED', result: 'x'.repeat(MAX_RESULT_BYTES) } }), { code: 'JOB_RECORD_INVALID' });
  await assert.rejects(store.update('job_1', { expectedSequence: running.sequence,
    changes: { status: 'COMPLETED', resultDiscarded: true, result: { kept: true } } }), { code: 'JOB_RECORD_INVALID' });
  const completed = await store.update('job_1', { expectedSequence: running.sequence,
    changes: { status: 'COMPLETED', result: { z: 1, a: { y: 2, b: 3 } } } });
  assert.equal(JSON.stringify(completed.result), '{"a":{"b":3,"y":2},"z":1}');
  const largestJob = await dispatched(store, 'job_2');
  await store.update('job_2', { expectedSequence: largestJob.sequence, changes: { status: 'COMPLETED', result: largest } });
  assert.equal((await new FileJobStore({ directory }).read('job_2')).result, largest);
});

test('unreadable or inconsistent records fail closed and are never overwritten', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const valid = (await store.create(newJob('valid'))).record;
  const file = recordFile(directory, 'job_1');
  const wrap = record => JSON.stringify({ schema: JOB_RECORD_SCHEMA, record });
  const cases = [
    ['{"torn', 'not valid JSON'],
    [JSON.stringify({ schema: 'other', record: { ...valid, jobId: 'job_1' } }), 'schema'],
    [wrap({ ...valid, jobId: 'job_1', status: 'DONE' }), 'status'],
    [wrap({ ...valid, jobId: 'job_1', reconciliationRequired: true }), 'reconciliationRequired'],
    [wrap({ ...valid, jobId: 'job_1', owner: { pid: 1 } }), 'owner'],
    [wrap({ ...Object.fromEntries(Object.entries(valid).filter(([key]) => key !== 'reason')), jobId: 'job_1' }), 'reason'],
    [wrap(valid), 'another job'], // a record stored under the wrong job's file name
    ['x'.repeat(128 * 1024 + 1), '128 KiB']
  ];
  for (const [text, message] of cases) {
    await writeFile(file, text);
    await assert.rejects(store.read('job_1'), error => error.code === 'JOB_STATE_INVALID' && error.message.includes(message), message);
    await assert.rejects(store.create(newJob('job_1')), { code: 'JOB_STATE_INVALID' });
    await assert.rejects(store.update('job_1', { expectedSequence: 1, changes: {} }), { code: 'JOB_STATE_INVALID' });
    assert.equal(await readFile(file, 'utf8'), text, 'an invalid record is left for reconciliation');
  }
  // A stored owner identity that is partial is as invalid as any other inconsistency.
  const partial = wrap({ ...valid, jobId: 'job_1', owner: { ...valid.owner, startTicks: '5' } });
  await writeFile(file, partial);
  await assert.rejects(store.read('job_1'), error => error.code === 'JOB_STATE_INVALID' && error.field === 'owner');
  await mkdir(recordFile(directory, 'dir_job'));
  await assert.rejects(store.read('dir_job'), { code: 'JOB_STATE_INVALID' });
});

test('list returns every record and reports unreadable ones', async t => {
  const directory = await temporaryDirectory(t);
  let tick = Date.parse('2026-01-01T00:00:00.000Z');
  const store = new FileJobStore({ directory, clock: () => new Date(tick++) });
  assert.deepEqual(await new FileJobStore({ directory: path.join(directory, 'absent') }).list(), { records: [], invalid: [] });
  for (const jobId of ['second', 'first', 'third']) await store.create(newJob(jobId));
  await writeFile(recordFile(directory, 'broken'), '{"torn');
  await copyFile(recordFile(directory, 'first'), recordFile(directory, 'misfiled'));
  await writeFile(path.join(directory, 'notes.txt'), 'ignored');
  const { records, invalid } = await store.list();
  assert.deepEqual(records.map(record => record.jobId), ['second', 'first', 'third']);
  assert.deepEqual(invalid.map(entry => entry.file).sort(),
    [`${fingerprint('broken')}.job.json`, `${fingerprint('misfiled')}.job.json`].sort());
  assert.ok(invalid.every(entry => entry.code === 'JOB_STATE_INVALID'));
});

test('inputs and options are validated', async t => {
  const directory = await temporaryDirectory(t);
  assert.throws(() => new FileJobStore({}), /directory/);
  assert.throws(() => new FileJobStore({ directory, clock: 'now' }), /clock/);
  const store = new FileJobStore({ directory });
  for (const jobId of ['', '_a', 'x'.repeat(129), 7]) {
    await assert.rejects(store.read(jobId), /Invalid job ID/);
    await assert.rejects(store.create(newJob(jobId)), /Invalid job ID/);
  }
  const boot = '00000000-0000-4000-8000-000000000000';
  for (const invalid of [{ objective: ' ' }, { objective: 'x'.repeat(4001) }, { fingerprint: 'abc' },
    { owner: { pid: 1, host: 'h' } }, { owner: { pid: -1, host: 'h', executionId: 'e' } },
    // The owner's identity is optional but never partial or malformed.
    { owner: { pid: 1, host: 'h', executionId: 'e', startTicks: '5' } },
    { owner: { pid: 1, host: 'h', executionId: 'e', bootId: boot } },
    { owner: { pid: 1, host: 'h', executionId: 'e', bootId: boot, startTicks: 5 } },
    { owner: { pid: 1, host: 'h', executionId: 'e', bootId: 'not-a-boot-id', startTicks: '5' } }]) {
    await assert.rejects(store.create(newJob('job_1', invalid)), { code: 'JOB_RECORD_INVALID' });
  }
  await assert.rejects(new FileJobStore({ directory, clock: () => new Date(NaN) }).create(newJob('job_1')), /valid Date/);
  const identified = (await store.create(newJob('identified', { owner: { ...owner(), bootId: boot, startTicks: '123' } }))).record;
  assert.deepEqual([identified.owner.bootId, identified.owner.startTicks], [boot, '123']);
  await store.create(newJob('job_1'));
  await assert.rejects(store.update('job_1', { expectedSequence: 1, changes: null }), /changes/);
  await assert.rejects(store.update('job_1', { expectedSequence: 1, changes: { owner: owner() } }), { code: 'JOB_RECORD_INVALID' });
  assert.deepEqual((await readdir(directory)).sort(), [`${fingerprint('identified')}.job.json`, `${fingerprint('job_1')}.job.json`].sort());
});

// Records renames and flushes (with the flushed path) until the test ends.
async function recordFlushes(t, directory) {
  const events = [];
  const paths = new WeakMap();
  const promises = fs.promises;
  const original = { open: promises.open, rename: promises.rename };
  const probe = await original.open(path.join(directory, 'probe'), 'w');
  const handlePrototype = Object.getPrototypeOf(probe);
  await probe.close();
  const originalSync = handlePrototype.sync;
  promises.open = async function (file, ...rest) {
    const handle = await original.open.call(this, file, ...rest);
    paths.set(handle, path.resolve(String(file)));
    return handle;
  };
  promises.rename = async (from, to) => { events.push(['rename', path.basename(to)]); return original.rename(from, to); };
  handlePrototype.sync = function () { events.push(['sync', paths.get(this)]); return originalSync.call(this); };
  syncBuiltinESMExports();
  t.after(() => {
    Object.assign(promises, original);
    handlePrototype.sync = originalSync;
    syncBuiltinESMExports();
  });
  return events;
}

const flushedAfterRename = (events, target, directory) => {
  const renamed = events.findIndex(([type, name]) => type === 'rename' && name === target);
  return renamed >= 0 && events.slice(renamed).some(([type, file]) => type === 'sync' && file === directory);
};

test('every write flushes the file, then the directory entry after the rename', { skip: process.platform === 'win32' }, async t => {
  const directory = await temporaryDirectory(t);
  const events = await recordFlushes(t, directory);
  const jobDirectory = path.join(directory, 'nested', 'jobs');
  const store = new FileJobStore({ directory: jobDirectory });
  const { record } = await store.create(newJob('job_1'));
  const target = `${fingerprint('job_1')}.job.json`;
  const renamed = events.findIndex(([type, name]) => type === 'rename' && name === target);
  assert.ok(renamed > 0, 'the record is published by rename');
  assert.ok(events.slice(0, renamed).some(([type, file]) => type === 'sync' && file?.endsWith('.job.tmp')), 'file flushed before rename');
  assert.ok(events.slice(renamed).some(([type, file]) => type === 'sync' && file === jobDirectory), 'directory flushed after rename');
  // The newly created state directory and its new parent were flushed into their parents.
  for (const parent of [directory, path.join(directory, 'nested')]) {
    assert.ok(events.some(([type, file]) => type === 'sync' && file === parent), `flushed ${parent}`);
  }

  events.length = 0;
  await store.update('job_1', { expectedSequence: record.sequence, changes: { phase: 'build' } });
  const updated = events.findIndex(([type]) => type === 'rename');
  assert.ok(events.slice(updated).some(([type, file]) => type === 'sync' && file === jobDirectory));
});

test('temporary files of a crashed job writer are swept; other stores keep theirs', async t => {
  const directory = await temporaryDirectory(t);
  const checkpoints = new FileCheckpointStore(directory);
  await checkpoints.save({ shared: true });
  const crashed = `${randomUUID()}.job.tmp`;
  const foreign = [`${randomUUID()}.record.tmp`, `${randomUUID()}.state.tmp`, 'notes.job.tmp'];
  for (const name of [crashed, ...foreign]) await writeFile(path.join(directory, name), '{"torn');
  const store = new FileJobStore({ directory });
  await store.create(newJob('job_1'));
  const names = await readdir(directory);
  assert.equal(names.includes(crashed), false);
  for (const name of foreign) assert.ok(names.includes(name), name);
  assert.deepEqual(await checkpoints.load(), { shared: true });
});

// Cross-process behavior. Children import the module directly.
const storeUrl = new URL('../src/job-store.mjs', import.meta.url).href;
const lockUrl = new URL('../src/directory-lock.mjs', import.meta.url).href;
const childPrelude = `
  import { FileJobStore } from ${JSON.stringify(storeUrl)};
  import { DirectoryLock } from ${JSON.stringify(lockUrl)};
  import { randomUUID, createHash } from 'node:crypto';
  import { hostname } from 'node:os';
  const [directory, ...args] = process.argv.slice(1);
  const store = new FileJobStore({ directory, lockTimeoutMs: 20000 });
  const owner = { pid: process.pid, host: hostname(), executionId: randomUUID() };
  const emit = value => process.stdout.write(JSON.stringify(value) + '\\n');
`;

function startChild(source, args) {
  const child = spawn(process.execPath, ['--input-type=module', '-e', childPrelude + source, '--', ...args],
    { stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', chunk => { stdout += chunk; child.emit('output', stdout); });
  child.stderr.on('data', chunk => { stderr += chunk; });
  child.exited = new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('close', (code, signal) => resolve({ code, signal, stdout, stderr }));
  });
  child.line = () => new Promise((resolve, reject) => {
    const check = () => { if (stdout.includes('\n')) resolve(JSON.parse(stdout.split('\n')[0])); };
    child.on('output', check);
    child.exited.then(({ stderr: error }) => reject(new Error(`child exited before reporting: ${error}`)));
    check();
  });
  return child;
}

async function runChildren(count, source, args) {
  const children = Array.from({ length: count }, () => startChild(source, args));
  const results = await Promise.all(children.map(child => child.exited));
  for (const { code, stderr } of results) if (code !== 0) throw new Error(`child exited ${code}: ${stderr}`);
  return results.map(({ stdout }) => JSON.parse(stdout));
}

test('processes creating the same job at once: exactly one creates it', async t => {
  const directory = await temporaryDirectory(t);
  const source = `
    const fingerprint = createHash('sha256').update('same input').digest('hex');
    const { created, record } = await store.create({ jobId: 'shared_job', objective: 'Run once', fingerprint, owner });
    emit({ created, owner: record.owner.executionId, mine: owner.executionId });`;
  const results = await runChildren(6, source, [directory]);
  const creators = results.filter(result => result.created);
  assert.equal(creators.length, 1);
  assert.ok(results.every(result => result.owner === creators[0].mine), 'every process sees the creator as owner');
  assert.equal((await new FileJobStore({ directory }).read('shared_job')).owner.executionId, creators[0].mine);
});

test('processes updating the same sequence at once: exactly one wins', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const running = await dispatched(store, 'shared_job');
  const source = `
    try {
      await store.update('shared_job', { expectedSequence: Number(args[0]), changes: { phase: 'phase_' + process.pid } });
      emit({ won: true });
    } catch (error) {
      if (error.code !== 'JOB_RECORD_CHANGED') throw error;
      emit({ won: false });
    }`;
  const results = await runChildren(6, source, [directory, String(running.sequence)]);
  assert.equal(results.filter(result => result.won).length, 1);
  assert.equal((await store.read('shared_job')).sequence, running.sequence + 1);
});

test('a process killed mid-job leaves a durable record a restarted process can read', async t => {
  const directory = await temporaryDirectory(t);
  const source = `
    const fingerprint = createHash('sha256').update('input').digest('hex');
    let { record } = await store.create({ jobId: 'killed_job', objective: 'Long task', fingerprint, owner });
    record = await store.update('killed_job', { expectedSequence: record.sequence,
      changes: { dispatched: true, dispatchedAt: new Date().toISOString() } });
    record = await store.update('killed_job', { expectedSequence: record.sequence, changes: { phase: 'execution' } });
    emit({ pid: process.pid, sequence: record.sequence });
    setInterval(() => {}, 1000); // the "work" runs until killed`;
  const child = startChild(source, [directory]);
  t.after(() => child.kill('SIGKILL'));
  const reported = await child.line();
  child.kill('SIGKILL');
  await child.exited;

  const record = await new FileJobStore({ directory }).read('killed_job');
  assert.deepEqual([record.status, record.phase, record.dispatched, record.sequence, record.owner.pid],
    ['RUNNING', 'execution', true, reported.sequence, reported.pid]);
});

test('a job lock left by a killed process is recovered; a live holder times out', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory, lockTimeoutMs: 200 });
  const record = await dispatched(store, 'job_1');
  const source = `
    const lock = new DirectoryLock({ directory, name: 'jobs.lock' });
    setInterval(() => {}, 1000); // stay alive while holding the lock
    lock.run(() => { emit({ locked: true }); return new Promise(() => {}); });`;

  const holder = startChild(source, [directory]);
  t.after(() => holder.kill('SIGKILL'));
  await holder.line();
  await assert.rejects(store.update('job_1', { expectedSequence: record.sequence, changes: { phase: 'blocked' } }),
    error => error.code === 'JOB_LOCK_TIMEOUT' && error.holder?.pid === holder.pid);
  holder.kill('SIGKILL');
  await holder.exited;
  const next = await store.update('job_1', { expectedSequence: record.sequence, changes: { phase: 'resumed' } });
  assert.equal(next.phase, 'resumed');
  assert.equal((await store.inspectLock()).status, 'UNLOCKED');
});

test('checkpoint, lease and idempotency writes also flush their directory', { skip: process.platform === 'win32' }, async t => {
  const directory = await temporaryDirectory(t);
  const events = await recordFlushes(t, directory);
  await new FileCheckpointStore(directory).save({ durable: true });
  assert.ok(flushedAfterRename(events, 'checkpoint.json', directory), 'checkpoint');

  const leases = new FileResourceLeaseManager({ directory, nodeRegistry: { require: () => ({ trust: 'VERIFIED', status: 'AVAILABLE' }) } });
  await leases.acquire({ ownerId: 'owner', nodeId: 'local', resources: ['gpu:0'], ttlMs: 1000, explicitAuthorization: true });
  assert.ok(flushedAfterRename(events, 'leases.json', directory), 'leases');

  await new FileIdempotencyRegistry({ directory }).execute({ key: 'effect', operation: async () => 1 });
  const record = `${fingerprint('effect')}.record.json`;
  assert.ok(flushedAfterRename(events, record, directory), 'idempotency record');
});

test('process records are validated and append-only while the job runs', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const at = new Date().toISOString();
  const marker = index => `MNEMOS_TREE_${String(index).padStart(32, '0')}`;
  const tree = (index, leader = null) => ({ marker: marker(index), supervisor: { pid: 100 + index, host: 'h' }, leader, recordedAt: at });
  const refuse = async (changes, code) => {
    const before = await store.read('job_1');
    await assert.rejects(store.update('job_1', { expectedSequence: before.sequence, changes }), { code });
    assert.deepEqual(await store.read('job_1'), before);
  };
  let record = await dispatched(store, 'job_1');
  assert.equal(record.processes, undefined, 'older records have none');

  for (const invalid of [[null], [{ ...tree(1), marker: 'MNEMOS_TREE_x' }], [tree(1), tree(1)],
    [{ ...tree(1), supervisor: { pid: 0, host: 'h' } }], [{ ...tree(1), leader: { pid: 5 } }],
    [{ ...tree(1), supervisor: { pid: 1, host: 'h', startTicks: '5' } }], [{ ...tree(1), recordedAt: 'never' }],
    Array.from({ length: 65 }, (_, index) => tree(index)), 'none']) {
    await refuse({ processes: invalid }, 'JOB_RECORD_INVALID');
  }

  record = await store.update('job_1', { expectedSequence: record.sequence, changes: { processes: [tree(1)] } });
  record = await store.update('job_1', { expectedSequence: record.sequence,
    changes: { processes: [tree(1, { pid: 201, host: 'h', bootId: '00000000-0000-4000-8000-000000000000', startTicks: '42' })] } });
  record = await store.update('job_1', { expectedSequence: record.sequence, changes: { processes: [...record.processes, tree(2)] } });
  assert.deepEqual(record.processes.map(entry => [entry.marker, entry.leader?.pid ?? null]), [[marker(1), 201], [marker(2), null]]);

  await refuse({ processes: [record.processes[0]] }, 'JOB_TRANSITION_INVALID'); // removal
  await refuse({ processes: [{ ...record.processes[0], leader: { pid: 202, host: 'h' } }, record.processes[1]] }, 'JOB_TRANSITION_INVALID');
  await refuse({ processes: [{ ...record.processes[0], supervisor: { pid: 999, host: 'h' } }, record.processes[1]] }, 'JOB_TRANSITION_INVALID');
  await refuse({ processes: [record.processes[1], record.processes[0]] }, 'JOB_TRANSITION_INVALID'); // reordered

  record = await store.update('job_1', { expectedSequence: record.sequence,
    changes: { status: 'UNKNOWN', reconciliationRequired: true, reason: 'OWNER_DIED_AFTER_DISPATCH' } });
  await refuse({ processes: [...record.processes, tree(3)] }, 'JOB_TRANSITION_INVALID'); // no longer RUNNING
  // Not even as part of resolving it: process records describe what ran while it was RUNNING.
  await refuse({ status: 'FAILED', reconciliationRequired: false, reason: 'X', processes: [...record.processes, tree(3)] }, 'JOB_TRANSITION_INVALID');
  await refuse({ status: 'FAILED', reconciliationRequired: false, reason: 'X',
    processes: [record.processes[0], { ...record.processes[1], leader: { pid: 7, host: 'h' } }] }, 'JOB_TRANSITION_INVALID');
  await refuse({ processes: [record.processes[0], { ...record.processes[1], leader: { pid: 7, host: 'h' } }] }, 'JOB_TRANSITION_INVALID');
  assert.equal((await new FileJobStore({ directory }).read('job_1')).processes.length, 2, 'durable');
});

test('the orphan audit is set once, when a RUNNING job becomes UNKNOWN, and kept after reconciliation', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const at = new Date().toISOString();
  const audit = { at, receipts: [{ marker: `MNEMOS_TREE_${'a'.repeat(32)}`, pgid: 42, confirmed: true, signals: ['SIGTERM'], frozen: true, stopped: [43] }] };
  const refuse = async (changes, code) => {
    const before = await store.read('job_1');
    await assert.rejects(store.update('job_1', { expectedSequence: before.sequence, changes }), { code });
    assert.deepEqual(await store.read('job_1'), before);
  };
  let record = await dispatched(store, 'job_1');
  const terminated = { status: 'UNKNOWN', reconciliationRequired: true, reason: 'ORPHANS_TERMINATED' };
  await refuse(terminated, 'JOB_RECORD_INVALID'); // the reason requires the audit
  await refuse({ orphanTermination: audit }, 'JOB_TRANSITION_INVALID'); // not while staying RUNNING
  await refuse({ status: 'UNKNOWN', reconciliationRequired: true, reason: 'OTHER', orphanTermination: audit }, 'JOB_TRANSITION_INVALID');
  for (const invalid of [{ ...audit, receipts: [] }, { ...audit, receipts: [{ ...audit.receipts[0], confirmed: false }] },
    { ...audit, receipts: [{ ...audit.receipts[0], marker: 'x' }] }, { ...audit, at: 'never' }]) {
    await refuse({ ...terminated, orphanTermination: invalid }, 'JOB_RECORD_INVALID');
  }
  record = await store.update('job_1', { expectedSequence: record.sequence, changes: { ...terminated, orphanTermination: audit } });
  assert.deepEqual(record.orphanTermination, audit);

  const reconciliation = { at, disposition: 'FAILED', override: null, evidence: { authoritative: true, source: 'ticket' },
    previous: { status: 'UNKNOWN', reason: 'ORPHANS_TERMINATED' } };
  await refuse({ status: 'FAILED', reconciliationRequired: false, reason: 'RECONCILED', reconciliation,
    orphanTermination: { ...audit, at: new Date(0).toISOString() } }, 'JOB_TRANSITION_INVALID'); // never changed
  record = await store.update('job_1', { expectedSequence: record.sequence,
    changes: { status: 'FAILED', reconciliationRequired: false, reason: 'RECONCILED', reconciliation } });
  assert.deepEqual([record.status, record.orphanTermination], ['FAILED', audit], 'kept for audit');
});

test('resumption fields: progress advances while running, identity stays, a stopped job is linked once', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const at = new Date().toISOString();
  const refuse = async (jobId, changes, code) => {
    const before = await store.read(jobId);
    await assert.rejects(store.update(jobId, { expectedSequence: before.sequence, changes }), { code });
    assert.deepEqual(await store.read(jobId), before);
  };
  const progress = (sequence, state = { n: sequence }) => ({ sequence, phase: 'work', state, savedAt: at });

  // Only resumable jobs carry resumption fields.
  await dispatched(store, 'plain');
  await refuse('plain', { progress: progress(1) }, 'JOB_RECORD_INVALID');
  await assert.rejects(store.create(newJob('bad', { resumable: 'yes' })), { code: 'JOB_RECORD_INVALID' });

  let record = (await store.create(newJob('job_1', { resumable: true }))).record;
  assert.equal(record.resumable, true);
  record = await store.update('job_1', { expectedSequence: record.sequence, changes: { dispatched: true, dispatchedAt: at } });
  for (const invalid of [{ ...progress(1), phase: 'bad phase' }, { ...progress(1), state: { f: () => {} } },
    progress(1, 'x'.repeat(70 * 1024)), { ...progress(1), savedAt: 'never' }]) {
    await refuse('job_1', { progress: invalid }, 'JOB_RECORD_INVALID');
  }
  await refuse('job_1', { progress: progress(2) }, 'JOB_TRANSITION_INVALID'); // skips a step
  record = await store.update('job_1', { expectedSequence: record.sequence, changes: { progress: progress(1) } });
  record = await store.update('job_1', { expectedSequence: record.sequence, changes: { progress: progress(2) } });
  await refuse('job_1', { progress: progress(2, { other: true }) }, 'JOB_TRANSITION_INVALID'); // rewritten
  await refuse('job_1', { resumable: false }, 'JOB_RECORD_INVALID'); // identity
  await refuse('job_1', { resumedBy: 'job_2' }, 'JOB_TRANSITION_INVALID'); // still RUNNING

  // A FAILED job: final, except for one link to its successor.
  record = await store.update('job_1', { expectedSequence: record.sequence, changes: { status: 'FAILED', reason: 'EXECUTOR_THREW' } });
  await refuse('job_1', { progress: progress(3) }, 'JOB_TRANSITION_INVALID');
  await refuse('job_1', { resumedBy: 'job_2', reason: 'OTHER' }, 'JOB_TRANSITION_INVALID'); // only the link
  await refuse('job_1', { resumedBy: 'job_1' }, 'JOB_RECORD_INVALID');
  record = await store.update('job_1', { expectedSequence: record.sequence, changes: { resumedBy: 'job_2' } });
  assert.deepEqual([record.status, record.reason, record.resumedBy], ['FAILED', 'EXECUTOR_THREW', 'job_2']);
  await refuse('job_1', { resumedBy: 'job_3' }, 'JOB_TRANSITION_INVALID'); // once

  // An UNKNOWN job becomes CANCELLED with reason RESUMED, which requires the link.
  record = (await store.create(newJob('job_u', { resumable: true }))).record;
  record = await store.update('job_u', { expectedSequence: record.sequence, changes: { dispatched: true, dispatchedAt: at } });
  record = await store.update('job_u', { expectedSequence: record.sequence,
    changes: { status: 'UNKNOWN', reconciliationRequired: true, reason: 'OWNER_DIED_AFTER_DISPATCH' } });
  const resumed = { status: 'CANCELLED', reason: 'RESUMED', reconciliationRequired: false, cancellationConfirmed: true };
  await refuse('job_u', resumed, 'JOB_RECORD_INVALID');
  record = await store.update('job_u', { expectedSequence: record.sequence, changes: { ...resumed, resumedBy: 'job_u2' } });
  assert.equal(record.reason, 'RESUMED');

  // A COMPLETED job is never linked.
  record = (await store.create(newJob('job_c', { resumable: true }))).record;
  record = await store.update('job_c', { expectedSequence: record.sequence, changes: { dispatched: true, dispatchedAt: at } });
  record = await store.update('job_c', { expectedSequence: record.sequence, changes: { status: 'COMPLETED' } });
  await refuse('job_c', { resumedBy: 'job_c2' }, 'JOB_TRANSITION_INVALID');
});

test('resume() links and creates under one lock, and never twice', async t => {
  const directory = await temporaryDirectory(t);
  const store = new FileJobStore({ directory });
  const at = new Date().toISOString();
  let record = (await store.create(newJob('job_1', { resumable: true }))).record;
  record = await store.update('job_1', { expectedSequence: record.sequence, changes: { dispatched: true, dispatchedAt: at,
    progress: { sequence: 1, phase: 'work', state: { next: 4 }, savedAt: at } } });
  record = await store.update('job_1', { expectedSequence: record.sequence, changes: { status: 'FAILED', reason: 'X' } });
  const successor = { jobId: 'job_2', owner: owner() };
  const first = await store.resume({ jobId: 'job_1', expectedSequence: record.sequence, changes: {}, successor });
  assert.equal(first.created, true);
  assert.deepEqual([first.predecessor.resumedBy, first.successor.resumeOf, first.successor.progress.state, first.successor.fingerprint],
    ['job_2', { jobId: 'job_1', rootJobId: 'job_1', attempt: 2 }, { next: 4 }, record.fingerprint]);
  const again = await store.resume({ jobId: 'job_1', expectedSequence: first.predecessor.sequence, changes: {}, successor });
  assert.deepEqual([again.created, again.successor], [false, first.successor]);
  await assert.rejects(store.resume({ jobId: 'job_1', expectedSequence: first.predecessor.sequence, changes: {},
    successor: { jobId: 'job_3', owner: owner() } }), { code: 'JOB_ALREADY_RESUMED' });
  await assert.rejects(store.resume({ jobId: 'job_1', expectedSequence: record.sequence, changes: {}, successor }),
    { code: 'JOB_RECORD_CHANGED' });
});
