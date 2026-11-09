import test from 'node:test';
import assert from 'node:assert/strict';
import { IdempotencyRegistry } from '../src/index.mjs';

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
