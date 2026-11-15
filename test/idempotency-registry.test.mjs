import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readdir, readFile, writeFile } from 'node:fs/promises';
import { hostname } from 'node:os';
import path from 'node:path';
import { FileIdempotencyRegistry, IdempotencyRegistry } from '../src/index.mjs';
import { temporaryDirectory } from './helpers.mjs';

class Journal {
  constructor() { this.events = []; this.failType = null; }
  async appendEvent(type, key, data) {
    if (this.failType === type) throw new Error('journal write unavailable');
    this.events.push({ type, key, data: structuredClone(data) });
  }
  async listEvents({ typePrefix }) { return structuredClone(this.events.filter(x => x.type.startsWith(typePrefix))); }
}

function deferred() {
  return Promise.withResolvers();
}

// Capture the journal snapshot before pausing, just like an asynchronous read.
function pauseRead(journal) {
  const entered = deferred();
  const resume = deferred();
  const listEvents = journal.listEvents.bind(journal);
  journal.listEvents = async options => {
    const events = await listEvents(options);
    entered.resolve();
    await resume.promise;
    return events;
  };
  return { entered: entered.promise, resume: resume.resolve };
}

function pauseWrite(journal, pausedType) {
  const entered = deferred();
  const resume = deferred();
  const appendEvent = journal.appendEvent.bind(journal);
  journal.appendEvent = async (type, key, data) => {
    if (type === pausedType) {
      entered.resolve();
      await resume.promise;
    }
    return appendEvent(type, key, data);
  };
  return { entered: entered.promise, resume: resume.resolve };
}

test('deduplicates overlapping work and replays a completed result', async () => {
  const journal = new Journal();
  const registry = new IdempotencyRegistry({ store: journal });
  let calls = 0;
  const params = { key: 'invoice:example', request: { amount: 1 }, operation: async () => { calls++; return { receipt: 'ok' }; } };
  const [first, second] = await Promise.all([registry.execute(params), registry.execute(params)]);
  assert.equal(calls, 1);
  assert.equal(first.status, 'IDEMPOTENCY_EXECUTED');
  assert.equal(second.status, 'IDEMPOTENCY_EXECUTED');
  assert.equal((await registry.execute(params)).status, 'IDEMPOTENT_REPLAY');
  assert.equal((await registry.execute({ ...params, request: { amount: 2 } })).status, 'IDEMPOTENCY_CONFLICT');
  const recovered = new IdempotencyRegistry({ store: journal });
  await recovered.restore();
  assert.equal((await recovered.execute(params)).status, 'IDEMPOTENT_REPLAY');
});

test('ambiguous effect does not run again without authoritative reconciliation', async () => {
  const registry = new IdempotencyRegistry({ store: new Journal() });
  let calls = 0;
  const params = { key: 'job:unknown', request: { task: 'x' }, operation: () => { calls++; throw Error('socket dropped'); } };
  await assert.rejects(registry.execute(params), error => error.idempotency?.status === 'IDEMPOTENCY_UNKNOWN');
  assert.equal((await registry.execute(params)).status, 'IDEMPOTENCY_UNKNOWN');
  assert.equal(calls, 1);
  assert.equal((await registry.reconcile({ key: params.key, request: params.request, disposition: 'COMPLETED' })).status, 'IDEMPOTENCY_EVIDENCE_REQUIRED');
  assert.equal((await registry.reconcile({ key: params.key, request: params.request, disposition: 'COMPLETED', result: 'receipt', evidence: { authoritative: true } })).status, 'IDEMPOTENCY_RECONCILED_COMPLETED');
  assert.equal((await registry.execute(params)).status, 'IDEMPOTENT_REPLAY');
  assert.equal(calls, 1);
});

test('authoritative no-effect is the only failed-work retry route', async () => {
  const registry = new IdempotencyRegistry();
  let calls = 0;
  const params = { key: 'job:no-effect', request: { task: 'x' }, failureProvesNoEffect: true, operation: () => { calls++; throw Error('not started'); } };
  await assert.rejects(registry.execute(params));
  assert.equal((await registry.execute(params)).status, 'IDEMPOTENCY_FAILED');
  const retry = await registry.execute({ ...params, retryFailed: true, operation: async () => { calls++; return 42; } });
  assert.equal(retry.status, 'IDEMPOTENCY_EXECUTED');
  assert.equal(retry.result, 42);
  assert.equal(calls, 2);
});

test('incomplete STARTED journal entry is marked unknown at recovery', async () => {
  const journal = new Journal();
  await journal.appendEvent('idempotency.started', 'effect:a', { record: { key: 'effect:a', fingerprint: 'f', state: 'STARTED' } });
  const registry = new IdempotencyRegistry({ store: journal });
  assert.equal((await registry.restore()).states.UNKNOWN, 1);
  assert.equal((await registry.execute({ key: 'effect:a', request: {}, operation: () => { throw Error('must not execute'); } })).status, 'IDEMPOTENCY_CONFLICT');
});

test('completed effect with failed receipt write becomes UNKNOWN, not replayed', async () => {
  const journal = new Journal();
  journal.failType = 'idempotency.completed';
  const registry = new IdempotencyRegistry({ store: journal });
  let calls = 0;
  const params = { key: 'effect:lost-receipt', operation: async () => { calls++; return 1; } };
  await assert.rejects(registry.execute(params), error => error.idempotency?.status === 'IDEMPOTENCY_UNKNOWN');
  assert.equal((await registry.execute(params)).status, 'IDEMPOTENCY_UNKNOWN');
  assert.equal(calls, 1);
  const restored = new IdempotencyRegistry({ store: journal });
  await restored.restore();
  assert.equal((await restored.execute(params)).status, 'IDEMPOTENCY_UNKNOWN');
});

for (const phase of ['started', 'effect', 'completed']) {
  test(`reconciliation cannot overwrite a live execution during ${phase}`, async t => {
    const journal = new Journal();
    const registry = new IdempotencyRegistry({ store: journal });
    const effectEntered = deferred();
    const effectResume = deferred();
    const paused = phase === 'effect' ? null : pauseWrite(journal, `idempotency.${phase}`);
    let calls = 0;
    const params = { key: 'effect:live', operation: async () => {
      calls++;
      effectEntered.resolve();
      if (phase === 'effect') await effectResume.promise;
      return { receipt: 'original' };
    } };
    const execution = registry.execute(params);
    const resume = () => { paused?.resume(); effectResume.resolve(); };
    t.after(async () => { resume(); await execution; });
    await (paused?.entered ?? effectEntered.promise);
    await assert.rejects(registry.reconcile({ key: params.key, disposition: 'FAILED_NO_EFFECT',
      evidence: { authoritative: true, effectAbsent: true } }), /operation.*running/i);
    await assert.rejects(registry.restore(), /operations.*running/i);
    resume();
    await execution;
    assert.equal((await registry.execute({ ...params, retryFailed: true })).status, 'IDEMPOTENT_REPLAY');
    assert.equal(calls, 1);
    assert.equal(journal.events.some(event => event.type === 'idempotency.reconciled_failed'), false);
    const recovered = new IdempotencyRegistry({ store: journal });
    await recovered.restore();
    assert.deepEqual((await recovered.execute(params)).result, { receipt: 'original' });
    assert.equal(calls, 1);
  });
}

for (const disposition of ['COMPLETED', 'FAILED_NO_EFFECT']) {
  test(`pending ${disposition} reconciliation excludes other transitions for its key`, async t => {
    const journal = new Journal();
    const registry = new IdempotencyRegistry({ store: journal });
    const params = { key: 'effect:reconcile', operation: () => { throw Error('ambiguous'); } };
    await assert.rejects(registry.execute(params));
    const paused = pauseWrite(journal, disposition === 'COMPLETED'
      ? 'idempotency.reconciled_completed' : 'idempotency.reconciled_failed');
    const reconciliation = registry.reconcile({ key: params.key, disposition, result: 'receipt',
      evidence: { authoritative: true, effectAbsent: true } });
    t.after(async () => { paused.resume(); await reconciliation; });
    await paused.entered;
    await assert.rejects(registry.execute({ ...params, retryFailed: true }), /reconciliation.*running/i);
    await assert.rejects(registry.reconcile({ key: params.key,
      disposition: disposition === 'COMPLETED' ? 'FAILED_NO_EFFECT' : 'COMPLETED',
      evidence: { authoritative: true, effectAbsent: true } }), /reconciliation.*running/i);
    await assert.rejects(registry.restore(), /reconciliation.*running/i);
    assert.equal((await registry.execute({ key: 'effect:independent', operation: () => 7 })).result, 7);
    paused.resume();
    assert.equal((await reconciliation).reconciled, true);
    let calls = 0;
    const retry = { ...params, retryFailed: true, operation: () => { calls++; return 'retry-receipt'; } };
    const outcome = await registry.execute(retry);
    assert.equal(outcome.status, disposition === 'COMPLETED' ? 'IDEMPOTENT_REPLAY' : 'IDEMPOTENCY_EXECUTED');
    assert.equal(outcome.result, disposition === 'COMPLETED' ? 'receipt' : 'retry-receipt');
    assert.equal(calls, disposition === 'COMPLETED' ? 0 : 1);
    assert.equal((await registry.reconcile({ key: params.key, disposition: 'FAILED_NO_EFFECT',
      evidence: { authoritative: true, effectAbsent: true } })).status, 'IDEMPOTENCY_RECONCILIATION_NOT_REQUIRED');
    const recovered = new IdempotencyRegistry({ store: journal });
    await recovered.restore();
    assert.equal((await recovered.execute(retry)).status, 'IDEMPOTENT_REPLAY');
    assert.equal(calls, disposition === 'COMPLETED' ? 0 : 1);
  });
}

test('restore excludes execution, reconciliation and another restore during a delayed read', async t => {
  const journal = new Journal();
  const registry = new IdempotencyRegistry({ store: journal });
  let calls = 0;
  const params = { key: 'effect:restore', operation: () => { calls++; return 'receipt'; } };
  await registry.execute(params);
  const paused = pauseRead(journal);
  const restoration = registry.restore();
  t.after(async () => { paused.resume(); await restoration; });
  await paused.entered;
  await assert.rejects(registry.execute({ key: 'effect:during-read', operation: () => { calls++; } }), /restor.*progress/i);
  await assert.rejects(registry.reconcile({ key: params.key, disposition: 'FAILED_NO_EFFECT',
    evidence: { authoritative: true, effectAbsent: true } }), /restor.*progress/i);
  await assert.rejects(registry.restore(), /restor.*progress/i);
  paused.resume();
  await restoration;
  assert.equal((await registry.execute(params)).status, 'IDEMPOTENT_REPLAY');
  assert.equal(calls, 1);
  assert.equal((await registry.execute({ key: 'effect:after-read', operation: () => 'new' })).result, 'new');
});

test('restore remains exclusive while persisting recovered UNKNOWN records', async t => {
  const journal = new Journal();
  const registry = new IdempotencyRegistry({ store: journal });
  const params = { key: 'effect:interrupted', operation: () => { throw Error('interrupted'); } };
  await assert.rejects(registry.execute(params));
  journal.events = journal.events.filter(event => event.type === 'idempotency.started');
  const paused = pauseWrite(journal, 'idempotency.unknown');
  const restoration = registry.restore();
  t.after(async () => { paused.resume(); await restoration; });
  await paused.entered;
  await assert.rejects(registry.execute({ key: 'effect:other', operation: () => 1 }), /restor.*progress/i);
  await assert.rejects(registry.reconcile({ key: params.key, disposition: 'COMPLETED', result: 'receipt',
    evidence: { authoritative: true } }), /restor.*progress/i);
  paused.resume();
  await restoration;
  assert.equal((await registry.execute(params)).status, 'IDEMPOTENCY_UNKNOWN');
});

test('failed restore and reconciliation release their guards without authorizing a retry', async () => {
  const journal = new Journal();
  const registry = new IdempotencyRegistry({ store: journal });
  const params = { key: 'effect:failed-transition', operation: () => { throw Error('ambiguous'); } };
  await assert.rejects(registry.execute(params));
  const listEvents = journal.listEvents.bind(journal);
  journal.listEvents = async () => { throw Error('read unavailable'); };
  await assert.rejects(registry.restore(), /read unavailable/);
  journal.listEvents = listEvents;
  journal.failType = 'idempotency.reconciled_failed';
  await assert.rejects(registry.reconcile({ key: params.key, disposition: 'FAILED_NO_EFFECT',
    evidence: { authoritative: true, effectAbsent: true } }), /journal write/);
  assert.equal((await registry.execute({ ...params, retryFailed: true })).status, 'IDEMPOTENCY_UNKNOWN');
  journal.failType = null;
  await registry.restore();
  assert.equal((await registry.reconcile({ key: params.key, disposition: 'COMPLETED', result: 'receipt',
    evidence: { authoritative: true } })).reconciled, true);
  assert.equal((await registry.execute(params)).result, 'receipt');
});

test('execution reserves its key before invoking the journal adapter', async () => {
  const journal = new Journal();
  const registry = new IdempotencyRegistry({ store: journal });
  const appendEvent = journal.appendEvent.bind(journal);
  const checks = [];
  journal.appendEvent = async (type, key, data) => {
    if (type === 'idempotency.started') {
      checks.push(assert.rejects(registry.restore(), /operations.*running/i));
      checks.push(assert.rejects(registry.reconcile({ key, disposition: 'FAILED_NO_EFFECT',
        evidence: { authoritative: true, effectAbsent: true } }), /operation.*running/i));
    }
    return appendEvent(type, key, data);
  };
  const params = { key: 'effect:adapter', operation: () => 'receipt' };
  assert.equal((await registry.execute(params)).result, 'receipt');
  await Promise.all(checks);
  assert.equal(checks.length, 2);
  assert.equal((await registry.execute(params)).status, 'IDEMPOTENT_REPLAY');
});

// ---- FileIdempotencyRegistry: the same rules, with state shared through a directory.

const recordFile = (directory, key) => path.join(directory, `${createHash('sha256').update(key).digest('hex')}.record.json`);

test('file registry: executes once, replays from any instance and detects conflicts', async t => {
  const directory = await temporaryDirectory(t);
  const registry = new FileIdempotencyRegistry({ directory });
  let calls = 0;
  const params = { key: 'invoice:example', request: { amount: 1 }, operation: async () => { calls++; return { receipt: 'ok' }; } };
  const [first, second] = await Promise.all([registry.execute(params), registry.execute(params)]);
  assert.deepEqual([first.status, second.status, calls], ['IDEMPOTENCY_EXECUTED', 'IDEMPOTENCY_EXECUTED', 1]);
  // A fresh instance needs no restore(): state is read from the directory on every call.
  const fresh = new FileIdempotencyRegistry({ directory });
  assert.deepEqual(await fresh.execute(params), { status: 'IDEMPOTENT_REPLAY', key: params.key, executed: false, result: { receipt: 'ok' } });
  assert.equal((await fresh.execute({ ...params, request: { amount: 2 } })).status, 'IDEMPOTENCY_CONFLICT');
  assert.equal(calls, 1);
  assert.deepEqual(await fresh.snapshot(), { schema: 'mnemos.idempotency.v2', count: 1, states: { COMPLETED: 1 }, coordination: 'same-host-file-lock' });
  assert.deepEqual((await readdir(directory)).filter(name => !name.endsWith('.record.json')), []);
});

test('file registry: ambiguous and authoritative failures follow the reconciliation rules', async t => {
  const directory = await temporaryDirectory(t);
  const registry = new FileIdempotencyRegistry({ directory });
  let calls = 0;
  const params = { key: 'job:unknown', request: { task: 'x' }, operation: () => { calls++; throw Error('socket dropped'); } };
  await assert.rejects(registry.execute(params), error => error.idempotency?.status === 'IDEMPOTENCY_UNKNOWN');
  const other = new FileIdempotencyRegistry({ directory });
  assert.equal((await other.execute(params)).status, 'IDEMPOTENCY_UNKNOWN');
  assert.equal((await other.reconcile({ key: params.key, request: params.request, disposition: 'COMPLETED' })).status, 'IDEMPOTENCY_EVIDENCE_REQUIRED');
  assert.equal((await other.reconcile({ key: params.key, request: { task: 'y' }, disposition: 'COMPLETED', evidence: { authoritative: true } })).status, 'IDEMPOTENCY_CONFLICT');
  assert.equal((await other.reconcile({ key: params.key, request: params.request, disposition: 'COMPLETED', result: 'receipt',
    evidence: { authoritative: true } })).status, 'IDEMPOTENCY_RECONCILED_COMPLETED');
  assert.equal((await registry.execute(params)).result, 'receipt');
  assert.equal((await registry.reconcile({ key: params.key, request: params.request, disposition: 'COMPLETED',
    evidence: { authoritative: true } })).status, 'IDEMPOTENCY_RECONCILIATION_NOT_REQUIRED');
  assert.equal((await registry.reconcile({ key: 'missing', disposition: 'COMPLETED', evidence: { authoritative: true } })).status, 'IDEMPOTENCY_NOT_FOUND');

  const noEffect = { key: 'job:no-effect', failureProvesNoEffect: true, operation: () => { calls++; throw Error('not started'); } };
  await assert.rejects(registry.execute(noEffect), error => error.idempotency?.status === 'IDEMPOTENCY_FAILED');
  assert.deepEqual(await other.execute(noEffect), { status: 'IDEMPOTENCY_FAILED', key: noEffect.key, executed: false, retryAllowed: true });
  const retry = await other.execute({ ...noEffect, retryFailed: true, operation: () => { calls++; return 42; } });
  assert.deepEqual([retry.status, retry.result, (await other.inspect(noEffect.key)).attempt], ['IDEMPOTENCY_EXECUTED', 42, 2]);
  assert.equal(calls, 3);
});

test('file registry: falsy rejection reasons remain failures across instances', async t => {
  const directory = await temporaryDirectory(t);
  const registry = new FileIdempotencyRegistry({ directory });
  const observer = new FileIdempotencyRegistry({ directory });
  for (const [index, reason] of [undefined, null, false, 0, '', NaN].entries()) {
    for (const failureProvesNoEffect of [false, true]) {
      const key = `effect:rejected-${index}-${failureProvesNoEffect}`;
      const operation = () => Promise.reject(reason);
      const expectedStatus = failureProvesNoEffect ? 'IDEMPOTENCY_FAILED' : 'IDEMPOTENCY_UNKNOWN';
      await assert.rejects(registry.execute({ key, operation, failureProvesNoEffect }),
        error => error.idempotency?.status === expectedStatus);
      const outcome = await observer.execute({ key, operation: () => assert.fail('effect unexpectedly ran') });
      assert.equal(outcome.status, expectedStatus);
    }
  }
});

test('file registry: a second instance sees a live execution as in progress, never as interrupted', async t => {
  const directory = await temporaryDirectory(t);
  const [owner, observer] = [new FileIdempotencyRegistry({ directory }), new FileIdempotencyRegistry({ directory })];
  const entered = deferred();
  const release = deferred();
  let calls = 0;
  const params = { key: 'effect:long', operation: async () => { calls++; entered.resolve(); await release.promise; return 'done'; } };
  const running = owner.execute(params);
  t.after(async () => { release.resolve(); await running; });
  await entered.promise;
  assert.equal((await observer.execute(params)).status, 'IDEMPOTENCY_IN_PROGRESS');
  assert.equal((await observer.inspect(params.key)).ownerState, 'ALIVE');
  await assert.rejects(observer.reconcile({ key: params.key, disposition: 'FAILED_NO_EFFECT',
    evidence: { authoritative: true, effectAbsent: true } }), /operation is running/);
  await assert.rejects(owner.reconcile({ key: params.key, disposition: 'COMPLETED', evidence: { authoritative: true } }), /operation is running/);
  release.resolve();
  assert.equal((await running).status, 'IDEMPOTENCY_EXECUTED');
  assert.equal((await observer.execute(params)).result, 'done');
  assert.equal(calls, 1);
});

test('file registry: a STARTED record whose execution is not running becomes UNKNOWN, not re-run', async t => {
  const directory = await temporaryDirectory(t);
  const key = 'effect:orphan';
  // Same PID as this process (for example a reused PID) but an execution that is not live here.
  const orphan = { key, fingerprint: createHash('sha256').update('{}').digest('hex'), state: 'STARTED', startedAt: new Date().toISOString(),
    attempt: 1, owner: { pid: process.pid, host: hostname(), executionId: 'gone' } };
  await writeFile(recordFile(directory, key), JSON.stringify({ schema: 'mnemos.idempotency-record.v1', record: orphan }));
  const registry = new FileIdempotencyRegistry({ directory });
  assert.equal((await registry.inspect(key)).ownerState, 'DEAD');
  const result = await registry.execute({ key, operation: () => { throw Error('must not execute'); } });
  assert.equal(result.status, 'IDEMPOTENCY_UNKNOWN');
  assert.equal((await registry.inspect(key)).reason, 'INTERRUPTED_AFTER_EFFECT_START');
});

test('file registry: owners that cannot be verified require an authorized reconciliation override', async t => {
  const directory = await temporaryDirectory(t);
  const registry = new FileIdempotencyRegistry({ directory });
  const fixtures = [
    ['OTHER_HOST', { pid: process.pid, host: `${hostname()}-other-host` }],
    ['UNREADABLE', { pid: -1, host: hostname() }]
  ];
  for (const [ownerState, ownerFields] of fixtures) {
    const key = `effect:owner-${ownerState}`;
    const owner = { ...ownerFields, executionId: `execution-${ownerState}` };
    const started = { key, fingerprint: createHash('sha256').update('{}').digest('hex'), state: 'STARTED',
      startedAt: new Date().toISOString(), attempt: 1, owner };
    await writeFile(recordFile(directory, key), JSON.stringify({ schema: 'mnemos.idempotency-record.v1', record: started }));
    assert.equal((await registry.inspect(key)).ownerState, ownerState);
    const resolution = { key, disposition: 'COMPLETED', result: 'confirmed receipt', evidence: { authoritative: true } };
    await assert.rejects(registry.reconcile(resolution), { code: 'IDEMPOTENCY_OWNER_UNVERIFIED', ownerState });
    await assert.rejects(registry.reconcile({ ...resolution, expectedExecutionId: owner.executionId }), /authorization/i);
    await assert.rejects(registry.reconcile({ ...resolution, expectedExecutionId: 'stale', explicitAuthorization: true }),
      { code: 'IDEMPOTENCY_RECORD_CHANGED' });
    assert.equal((await registry.inspect(key)).state, 'STARTED');
    const confirmed = await registry.reconcile({ ...resolution, expectedExecutionId: owner.executionId, explicitAuthorization: true });
    assert.equal(confirmed.status, 'IDEMPOTENCY_RECONCILED_COMPLETED');
    const persisted = await registry.inspect(key);
    assert.equal(persisted.liveOwnerOverride.executionId, owner.executionId);
    assert.deepEqual(persisted.liveOwnerOverride.owner, owner);
  }
});

test('file registry: an unrecordable outcome is UNKNOWN and the effect never runs twice', async t => {
  const directory = await temporaryDirectory(t);
  const registry = new FileIdempotencyRegistry({ directory });
  let calls = 0;
  const params = { key: 'effect:unserializable', operation: () => { calls++; return { amount: 10n }; } }; // BigInt is not JSON
  await assert.rejects(registry.execute(params), error => error.idempotency?.status === 'IDEMPOTENCY_UNKNOWN');
  assert.equal((await registry.execute(params)).status, 'IDEMPOTENCY_UNKNOWN');
  assert.equal((await registry.inspect(params.key)).reason, 'COMPLETION_PERSISTENCE_FAILED');
  assert.equal(calls, 1);
});

test('file registry: unreadable or mismatched records fail closed and never run the effect', async t => {
  const directory = await temporaryDirectory(t);
  const registry = new FileIdempotencyRegistry({ directory });
  await registry.execute({ key: 'effect:a', operation: () => 'a' });
  const valid = await readFile(recordFile(directory, 'effect:a'), 'utf8');
  const parsed = JSON.parse(valid);
  const must = () => { throw Error('must not execute'); };
  for (const corrupt of ['{"schema":', JSON.stringify({ ...parsed, schema: 'other' }),
    JSON.stringify({ ...parsed, record: { ...parsed.record, state: 'DONE' } }),
    JSON.stringify({ ...parsed, record: { ...parsed.record, state: 'STARTED', owner: undefined } })]) { // STARTED without an owner
    await writeFile(recordFile(directory, 'effect:a'), corrupt);
    await assert.rejects(registry.execute({ key: 'effect:a', operation: must }), error => error.code === 'IDEMPOTENCY_STATE_INVALID');
  }
  // A record stored under another key's file name is rejected, not trusted.
  await writeFile(recordFile(directory, 'effect:b'), valid);
  await assert.rejects(registry.execute({ key: 'effect:b', operation: must }), error => error.code === 'IDEMPOTENCY_STATE_INVALID');
  await assert.rejects(registry.snapshot(), error => error.code === 'IDEMPOTENCY_STATE_INVALID');
});
