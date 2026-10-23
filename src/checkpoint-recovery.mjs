import { mkdir, open, rename, rm } from 'node:fs/promises';
import { randomBytes } from 'node:crypto';
import path from 'node:path';

const MAX_BYTES = 1024 * 1024;
const VALID_ID = /^[a-z0-9][a-z0-9_-]{0,127}$/i;

/** Atomic same-directory replacement, serialized within this store instance. */
export class FileCheckpointStore {
  #writes = Promise.resolve();

  constructor(directory) {
    if (typeof directory !== 'string' || !directory.trim()) throw new TypeError('A checkpoint directory is required');
    this.directory = path.resolve(directory);
    this.file = path.join(this.directory, 'checkpoint.json');
  }

  save(value) {
    const text = JSON.stringify(value, null, 2) + '\n';
    if (Buffer.byteLength(text) > MAX_BYTES) throw new RangeError('Checkpoint exceeds 1 MiB');
    const operation = this.#writes.then(async () => {
      await mkdir(this.directory, { recursive: true, mode: 0o700 });
      const temporary = path.join(this.directory, `${randomBytes(16).toString('hex')}.tmp`);
      let file;
      try {
        file = await open(temporary, 'wx', 0o600);
        await file.writeFile(text, 'utf8');
        await file.sync();
        await file.close();
        file = null;
        await rename(temporary, this.file);
      } finally {
        await file?.close();
        await rm(temporary, { force: true });
      }
    });
    this.#writes = operation.catch(() => {});
    return operation;
  }

  async load() {
    await this.#writes;
    let file;
    try {
      file = await open(this.file, 'r');
      const stat = await file.stat();
      if (!stat.isFile() || stat.size > MAX_BYTES) throw new RangeError('Invalid checkpoint file');
      const text = await file.readFile('utf8');
      if (Buffer.byteLength(text) > MAX_BYTES) throw new RangeError('Checkpoint exceeds 1 MiB');
      return JSON.parse(text);
    } catch (error) {
      if (error.code === 'ENOENT') return null;
      throw error;
    } finally {
      await file?.close();
    }
  }
}

function validCheckpoint(value) {
  return value?.version === 1 && typeof value.goal?.goalId === 'string' &&
    VALID_ID.test(value.goal.goalId) && typeof value.checkpointedAt === 'string' &&
    Number.isFinite(Date.parse(value.checkpointedAt)) && Array.isArray(value.activeJobs) &&
    value.activeJobs.length <= 1000 && value.activeJobs.every(job =>
      typeof job?.id === 'string' && VALID_ID.test(job.id)) &&
    new Set(value.activeJobs.map(job => job.id)).size === value.activeJobs.length;
}

function invalidRecovery(reason) {
  return { goal: null, dag: null, jobs: [], recoveryRequired: true, recoveryError: reason };
}

/** Restore state and reconcile jobs. This class never executes or replays a task. */
export class GoalRecoveryCoordinator {
  constructor({ store, jobInspector = async () => ({ status: 'UNKNOWN' }) } = {}) {
    if (typeof store?.save !== 'function' || typeof store?.load !== 'function') {
      throw new TypeError('A checkpoint store is required');
    }
    if (typeof jobInspector !== 'function') throw new TypeError('jobInspector must be a function');
    this.store = store;
    this.jobInspector = jobInspector;
  }

  async checkpoint({ goal, dag = null, activeJobs = [] } = {}) {
    const value = structuredClone({ version: 1, goal, dag, activeJobs, checkpointedAt: new Date().toISOString() });
    if (!validCheckpoint(value)) throw new TypeError('Invalid checkpoint shape or duplicate job ID');
    await this.store.save(value);
    return value;
  }

  async recover() {
    let value;
    try { value = await this.store.load(); }
    catch { return invalidRecovery('CHECKPOINT_UNREADABLE'); }
    if (value === null) return null;
    if (!validCheckpoint(value)) return invalidRecovery('CHECKPOINT_INVALID');
    const jobs = [];
    const known = new Set(['COMPLETED', 'RUNNING', 'CANCELLED', 'FAILED', 'UNKNOWN']);
    for (const job of value.activeJobs) {
      let status = 'UNKNOWN';
      try {
        const inspection = await this.jobInspector(structuredClone(job));
        if (known.has(inspection?.status)) status = inspection.status;
      } catch { /* Inspection failure is uncertainty, never permission to replay. */ }
      jobs.push({ ...structuredClone(job), recoveryStatus: status });
    }
    return {
      ...structuredClone(value), jobs,
      recoveryRequired: jobs.some(job => job.recoveryStatus !== 'COMPLETED')
    };
  }
}
