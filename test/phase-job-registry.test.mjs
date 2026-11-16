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

test('jobs that require reconciliation are never evicted, so their identity cannot run again', async () => {
  let calls = 0;
  const registry = new PhaseJobRegistry({ maxEntries: 2, execute: async ({ jobId, signal }) => {
    calls++;
    if (jobId === 'ambiguous') {
      await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }));
      return { status: 'CANCELLED' }; // no termination evidence
    }
    return { status: 'COMPLETED' };
  } });
  registry.start({ jobId: 'ambiguous', objective: 'Uncertain stop' });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal((await registry.requestCancellation('ambiguous')).status, 'UNKNOWN');

  // A definite outcome may still be forgotten to make room.
  registry.start({ jobId: 'done_1', objective: 'Finish' }); await registry.wait('done_1');
  registry.start({ jobId: 'done_2', objective: 'Finish' }); await registry.wait('done_2');
  assert.equal(registry.get('done_1'), null);
  assert.equal(registry.get('ambiguous').reconciliationRequired, true);

  // Starting the ambiguous identity again reports it; the executor does not run again.
  const before = calls;
  assert.equal(registry.start({ jobId: 'ambiguous', objective: 'Uncertain stop' }).status, 'UNKNOWN');
  assert.equal(calls, before);
});

test('capacity pinned by unreconciled jobs fails with a diagnostic code', async () => {
  const registry = new PhaseJobRegistry({ maxEntries: 1, execute: async ({ signal }) => {
    await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }));
    return { status: 'CANCELLED' };
  } });
  registry.start({ jobId: 'pinned', objective: 'Uncertain stop' });
  await new Promise(resolve => setImmediate(resolve));
  await registry.requestCancellation('pinned');
  assert.throws(() => registry.start({ jobId: 'extra', objective: 'More work' }), error =>
    error.code === 'JOB_CAPACITY_EXHAUSTED' && /capacity/.test(error.message) &&
    error.running === 0 && error.reconciliationRequired === 1);
});

test('a result that cannot be copied is discarded without changing the outcome', async () => {
  const outcomes = {
    completed: { status: 'COMPLETED', handle: () => {} },
    confirmed: { status: 'CANCELLED', termination: { confirmed: true }, socket: () => {} }
  };
  const registry = new PhaseJobRegistry({ execute: async ({ jobId, signal }) => {
    if (jobId === 'confirmed') await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }));
    return outcomes[jobId];
  } });
  registry.start({ jobId: 'completed', objective: 'Return a handle' });
  const completed = await registry.wait('completed');
  assert.deepEqual([completed.status, completed.result, completed.resultDiscarded], ['COMPLETED', null, true]);

  registry.start({ jobId: 'confirmed', objective: 'Cancel with a handle' });
  await new Promise(resolve => setImmediate(resolve));
  const cancelled = await registry.requestCancellation('confirmed');
  assert.deepEqual([cancelled.status, cancelled.cancellationConfirmed, cancelled.resultDiscarded], ['CANCELLED', true, true]);

  registry.start({ jobId: 'plain', objective: 'Plain result' });
  outcomes.plain = { status: 'COMPLETED', value: 1 };
  const plain = await registry.wait('plain');
  assert.deepEqual([plain.status, plain.result.value, plain.resultDiscarded], ['COMPLETED', 1, false]);
});

test('job IDs follow the checkpoint grammar: 1 to 128 characters', async () => {
  const registry = new PhaseJobRegistry({ execute: async () => ({ status: 'COMPLETED' }) });
  for (const jobId of ['a', 'a1', 'x'.repeat(128)]) {
    registry.start({ jobId, objective: 'Boundary' });
    assert.equal((await registry.wait(jobId)).status, 'COMPLETED');
  }
  for (const jobId of ['', '_a', '-a', 'x'.repeat(129), 'a.b', 'a b', 5]) {
    assert.throws(() => registry.get(jobId), /Invalid job ID/);
  }
});

test('outcomes carry a reason, and effectAbsent only from an explicit executor report', async () => {
  const reports = {
    failed_clean: async () => ({ status: 'FAILED', effectAbsent: true }),
    failed_plain: async () => ({ status: 'FAILED' }),
    completed_claim: async () => ({ status: 'COMPLETED', effectAbsent: true }),
    threw: async () => { throw new Error('offline'); },
    odd_status: async () => ({ status: 'CANCELLED' })
  };
  const registry = new PhaseJobRegistry({ execute: ({ jobId }) => reports[jobId]() });
  const outcomes = {};
  for (const jobId of Object.keys(reports)) {
    registry.start({ jobId, objective: 'Report' });
    const { status, effectAbsent, reason } = await registry.wait(jobId);
    outcomes[jobId] = [status, effectAbsent, reason];
  }
  assert.deepEqual(outcomes, {
    failed_clean: ['FAILED', true, 'EXECUTOR_REPORTED_FAILURE'],
    failed_plain: ['FAILED', false, 'EXECUTOR_REPORTED_FAILURE'],
    completed_claim: ['COMPLETED', false, null],
    threw: ['FAILED', false, 'EXECUTOR_THREW'],
    odd_status: ['FAILED', false, 'EXECUTOR_STATUS_UNEXPECTED']
  });

  registry.start({ jobId: 'early', objective: 'Cancel before dispatch' });
  const early = await registry.requestCancellation('early');
  assert.deepEqual([early.status, early.effectAbsent, early.reason, early.result], ['CANCELLED', true, 'CANCELLED_BEFORE_DISPATCH', null]);
});

test('reconcile resolves an UNKNOWN job with evidence and frees the capacity it pinned', async () => {
  let calls = 0;
  const registry = new PhaseJobRegistry({ maxEntries: 1, execute: async ({ signal }) => {
    calls++;
    await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }));
    return { status: 'CANCELLED' }; // unconfirmed
  } });
  registry.start({ jobId: 'pinned', objective: 'Uncertain stop' });
  await new Promise(resolve => setImmediate(resolve));
  await registry.requestCancellation('pinned');
  assert.throws(() => registry.start({ jobId: 'next', objective: 'More' }), { code: 'JOB_CAPACITY_EXHAUSTED' });

  const evidence = { authoritative: true, source: 'worker-host:process-table', effectAbsent: true };
  const base = { jobId: 'pinned', disposition: 'FAILED', evidence, explicitAuthorization: true };
  assert.throws(() => registry.reconcile({ ...base, explicitAuthorization: false }), { code: 'JOB_AUTHORIZATION_REQUIRED' });
  assert.throws(() => registry.reconcile({ ...base, evidence: { source: 'guess' } }), { code: 'JOB_EVIDENCE_REQUIRED' });
  assert.throws(() => registry.reconcile({ ...base, disposition: 'RUNNING' }), /disposition/);
  assert.throws(() => registry.reconcile({ ...base, jobId: 'missing' }), { code: 'JOB_NOT_FOUND' });
  assert.equal(registry.get('pinned').status, 'UNKNOWN');

  const reconciled = registry.reconcile(base);
  assert.deepEqual([reconciled.status, reconciled.reason, reconciled.reconciliationRequired, reconciled.effectAbsent],
    ['FAILED', 'RECONCILED', false, true]);
  assert.deepEqual(reconciled.reconciliation.previous, { status: 'UNKNOWN', reason: 'CANCELLATION_UNCONFIRMED' });
  assert.throws(() => registry.reconcile(base), { code: 'JOB_RECONCILIATION_NOT_REQUIRED' });
  assert.equal(registry.start({ jobId: 'pinned', objective: 'Uncertain stop' }).status, 'FAILED', 'never run again');
  assert.equal(calls, 1);

  registry.start({ jobId: 'next', objective: 'More' }); // the reconciled job may now be evicted
  assert.equal(registry.get('pinned'), null);
  await registry.requestCancellation('next');
});

test('a running in-memory job cannot be reconciled', async () => {
  const release = deferred();
  const registry = new PhaseJobRegistry({ execute: async () => { await release.promise; return { status: 'COMPLETED' }; } });
  registry.start({ jobId: 'busy', objective: 'Busy' });
  assert.throws(() => registry.reconcile({ jobId: 'busy', disposition: 'FAILED', explicitAuthorization: true,
    evidence: { authoritative: true, source: 'operator' } }), { code: 'JOB_RUNNING_HERE' });
  release.resolve();
  assert.equal((await registry.wait('busy')).reconciliation, null);
});
