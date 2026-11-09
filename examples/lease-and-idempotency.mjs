import { IdempotencyRegistry, ResourceLeaseManager } from '../src/index.mjs';

// Synthetic demonstration: no credential, remote process or GPU is accessed.
const nodes = { require: id => {
  if (id !== 'demo') throw Error('Unknown node');
  return { trust: 'VERIFIED', status: 'AVAILABLE' };
}};
const leases = new ResourceLeaseManager({ nodeRegistry: nodes });
const lease = leases.acquire({ ownerId: 'demo:task', nodeId: 'demo', resources: ['gpu:0'], explicitAuthorization: true });
if (lease.status !== 'LEASE_ACQUIRED') throw Error('Lease was not acquired');

const journal = {
  events: [],
  async appendEvent(type, key, data) { this.events.push({ type, key, data: structuredClone(data) }); },
  async listEvents({ typePrefix }) { return structuredClone(this.events.filter(event => event.type.startsWith(typePrefix))); }
};
const effects = new IdempotencyRegistry({ store: journal });
let actualExecutions = 0;
const request = { key: 'demo:operation', request: { action: 'simulate' }, operation: async () => {
  actualExecutions++;
  return { completed: true };
}};
const first = await effects.execute(request);
const replay = await effects.execute(request);
leases.release({ leaseId: lease.lease.leaseId, ownerId: 'demo:task', explicitAuthorization: true });
if (actualExecutions !== 1 || replay.status !== 'IDEMPOTENT_REPLAY') throw Error('Unexpected duplicate execution');
console.log(JSON.stringify({ lease: lease.status, first: first.status, replay: replay.status, actualExecutions }));
