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
