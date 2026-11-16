import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { hostname } from 'node:os';
import { identityState, processIdentity } from '../src/directory-lock.mjs';
import { zombieProcess } from './helpers.mjs';

const linux = process.platform === 'linux';

async function runningChild(t) {
  const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });
  t.after(() => child.kill('SIGKILL'));
  await new Promise((resolve, reject) => { child.once('spawn', resolve); child.once('error', reject); });
  child.closed = new Promise(resolve => child.once('close', resolve));
  return child;
}

test('a running process is identified and verified; once it exits it is DEAD', async t => {
  const child = await runningChild(t);
  const identity = processIdentity(child.pid);
  assert.deepEqual([identity.pid, identity.host], [child.pid, hostname()]);
  if (linux) {
    assert.match(identity.bootId, /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    assert.match(identity.startTicks, /^\d+$/);
    assert.equal(identityState(identity), 'ALIVE');
  } else {
    assert.equal(identity.startTicks, undefined);
    assert.equal(identityState(identity), 'UNVERIFIED'); // no start time to compare
  }
  child.kill('SIGKILL');
  await child.closed;
  assert.equal(identityState(identity), 'DEAD');
  assert.equal(processIdentity(child.pid), null);
});

test('this process is identified and verified', { skip: !linux }, () => {
  const identity = processIdentity();
  assert.equal(identity.pid, process.pid);
  assert.equal(identityState(identity), 'ALIVE');
  assert.deepEqual(processIdentity(process.pid), identity, 'stable');
});

test('a PID now used by a process that started at another time is DEAD', { skip: !linux }, async t => {
  const child = await runningChild(t);
  const identity = processIdentity(child.pid);
  const earlier = String(BigInt(identity.startTicks) - 1n);
  assert.equal(identityState({ ...identity, startTicks: earlier }), 'DEAD', 'the recorded process is gone; its PID was reused');
  assert.equal(identityState(identity), 'ALIVE');
});

test('an identity recorded before a reboot is DEAD, even if its PID runs again', { skip: !linux }, () => {
  const identity = processIdentity();
  assert.equal(identityState({ ...identity, bootId: '00000000-0000-4000-8000-000000000000' }), 'DEAD');
});

test('without a start time a running PID cannot be verified; a gone PID is still DEAD', async t => {
  const child = await runningChild(t);
  assert.equal(identityState({ pid: child.pid, host: hostname() }), 'UNVERIFIED');
  assert.equal(identityState({ pid: process.pid, host: hostname() }), 'UNVERIFIED', 'not even this process: an earlier one may have had this PID');
  child.kill('SIGKILL');
  await child.closed;
  assert.equal(identityState({ pid: child.pid, host: hostname() }), 'DEAD');
});

test('a process that exited but was not reaped has no identity and is DEAD', { skip: !linux }, async t => {
  const { pid } = await zombieProcess(t);
  assert.equal(processIdentity(pid), null);
  const recorded = { ...processIdentity(), pid, startTicks: '1' };
  assert.equal(identityState(recorded), 'DEAD');
});

test('identities from another host or with malformed fields are never treated as verified', () => {
  const own = processIdentity();
  assert.equal(identityState({ ...own, host: 'other-host.invalid' }), 'OTHER_HOST');
  for (const malformed of [null, {}, { pid: -1, host: hostname() }, { pid: 1.5, host: hostname() }, { pid: process.pid }]) {
    assert.equal(identityState(malformed), 'UNREADABLE', JSON.stringify(malformed));
  }
  const base = { pid: process.pid, host: hostname() };
  for (const fields of [{ startTicks: 'abc', bootId: '00000000-0000-4000-8000-000000000000' }, { startTicks: '5' },
    { bootId: '00000000-0000-4000-8000-000000000000' }, { startTicks: 5, bootId: '00000000-0000-4000-8000-000000000000' }]) {
    assert.equal(identityState({ ...base, ...fields }), 'UNREADABLE', JSON.stringify(fields));
  }
  for (const pid of [0, -1, 1.5, '1']) assert.throws(() => processIdentity(pid), /Invalid PID/);
});
