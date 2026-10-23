import test from 'node:test';
import assert from 'node:assert/strict';
import { PhaseJobRegistry } from '../src/index.mjs';
import { deferred } from './helpers.mjs';

test('cancels an owned phase only after its execution settles with confirmation', async () => {
  const entered = deferred();
  const registry = new PhaseJobRegistry({ execute: async ({ signal, setPhase }) => {
    setPhase('full_regression');
    entered.resolve();
    await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }));
    return { status: 'CANCELLED', termination: { confirmed: true } };
  } });
  registry.start({ jobId: 'job_1', objective: 'Verify a candidate' });
  await entered.promise;
  const result = await registry.requestCancellation('job_1');
  assert.equal(result.status, 'CANCELLED');
  assert.equal(result.phase, 'full_regression');
  assert.equal(result.cancellationConfirmed, true);
});

test('a non-cooperative executor remains running after cancellation times out', async () => {
  const entered = deferred(), release = deferred();
  const registry = new PhaseJobRegistry({ execute: async () => {
    entered.resolve(); await release.promise; return { status: 'COMPLETED' };
  } });
  registry.start({ jobId: 'job_2', objective: 'Non-cooperative work' });
  await entered.promise;
  const result = await registry.requestCancellation('job_2', { timeoutMs: 5 });
  assert.equal(result.status, 'RUNNING');
  assert.equal(result.cancellationConfirmed, false);
  release.resolve();
  assert.equal((await registry.wait('job_2')).status, 'COMPLETED');
});

test('an unconfirmed stop requires reconciliation', async () => {
  const entered = deferred(), release = deferred();
  const registry = new PhaseJobRegistry({ execute: async () => {
    entered.resolve(); await release.promise; return { status: 'CANCELLED' };
  } });
  registry.start({ jobId: 'job_3', objective: 'Uncertain stop' });
  await entered.promise;
  release.resolve();
  const result = await registry.requestCancellation('job_3');
  assert.equal(result.status, 'UNKNOWN');
  assert.equal(result.reconciliationRequired, true);
});

test('a cancellation before dispatch does not invoke an executor', async () => {
  let calls = 0;
  const registry = new PhaseJobRegistry({ execute: async () => { calls++; return { status: 'COMPLETED' }; } });
  registry.start({ jobId: 'queued_job', objective: 'Cancel before dispatch' });
  const result = await registry.requestCancellation('queued_job');
  assert.equal(result.status, 'CANCELLED');
  assert.equal(calls, 0);
});

test('job identity compares canonical JSON and rejects unsupported input', async () => {
  const registry = new PhaseJobRegistry({ execute: async () => ({ status: 'COMPLETED' }) });
  registry.start({ jobId: 'json_job', objective: 'JSON', input: { a: 1, b: 2 } });
  registry.start({ jobId: 'json_job', objective: 'JSON', input: { b: 2, a: 1 } });
  assert.throws(() => registry.start({ jobId: 'map_job', objective: 'Map', input: new Map() }), /JSON/);
  assert.throws(() => registry.start({ jobId: 'nan_job', objective: 'NaN', input: NaN }), /JSON/);
  await registry.wait('json_job');
});

test('same job identity does not replay execution, including after completion', async () => {
  let calls = 0;
  const registry = new PhaseJobRegistry({ execute: async () => { calls++; return { status: 'COMPLETED' }; } });
  const input = { jobId: 'job_4', objective: 'Once', input: { example: 1 } };
  registry.start(input); registry.start(input); await registry.wait('job_4'); registry.start(input);
  assert.equal(calls, 1);
  assert.throws(() => registry.start({ ...input, input: { example: 2 } }), /identity/);
});

test('raw executor errors are not retained in the public state', async () => {
  const registry = new PhaseJobRegistry({ execute: async () => { throw new Error('sensitive diagnostic'); } });
  registry.start({ jobId: 'job_5', objective: 'Fail' });
  const result = await registry.wait('job_5');
  assert.equal(result.status, 'FAILED');
  assert.doesNotMatch(JSON.stringify(result), /sensitive diagnostic/);
});

test('running jobs cannot be evicted to make capacity', async () => {
  const release = deferred();
  const registry = new PhaseJobRegistry({ maxEntries: 1, execute: async () => { await release.promise; return { status: 'COMPLETED' }; } });
  registry.start({ jobId: 'job_6', objective: 'Wait' });
  assert.throws(() => registry.start({ jobId: 'job_7', objective: 'Extra work' }), /capacity/);
  release.resolve(); await registry.wait('job_6');
  registry.start({ jobId: 'job_7', objective: 'Extra work' });
  await registry.wait('job_7');
  assert.equal(registry.get('job_6'), null);
});

test('returned state is a detached snapshot', async () => {
  const registry = new PhaseJobRegistry({ execute: async () => ({ status: 'COMPLETED', value: { count: 1 } }) });
  registry.start({ jobId: 'job_8', objective: 'Inspect' });
  const result = await registry.wait('job_8');
  result.result.value.count = 9;
  assert.equal(registry.get('job_8').result.value.count, 1);
});
