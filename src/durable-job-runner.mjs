import { GoalRecoveryCoordinator } from './checkpoint-recovery.mjs';

// Maps a settled registry snapshot to the status saved in the checkpoint.
// CANCELLED is saved only with confirmed termination; anything else uncertain is UNKNOWN.
function durableStatus(snapshot) {
  if (snapshot.status === 'CANCELLED') return snapshot.cancellationConfirmed === true ? 'CANCELLED' : 'UNKNOWN';
  return ['COMPLETED', 'FAILED'].includes(snapshot.status) ? snapshot.status : 'UNKNOWN';
}

/**
 * Wraps a PhaseJobRegistry so a job is checkpointed before it is dispatched.
 * If the initial checkpoint cannot be saved, the job is never started.
 */
export class DurableJobRunner {
  #checkpointQueue = Promise.resolve();

  constructor({ registry, coordinator, goal } = {}) {
    this.registry = registry;
    this.coordinator = coordinator;
    this.goal = goal;
  }

  // Serialize checkpoint read-validate-merge-save within this instance; a failure must not block later operations.
  #enqueue(operation) {
    const result = this.#checkpointQueue.then(operation);
    this.#checkpointQueue = result.catch(() => {});
    return result;
  }

  async #readCheckpoint() {
    // Reuse the coordinator's validation; the default inspector avoids calling the caller's inspector.
    const recovery = await new GoalRecoveryCoordinator({ store: this.coordinator.store }).recover();
    if (recovery?.recoveryError) throw new Error(`Existing checkpoint is not usable: ${recovery.recoveryError}`);
    if (recovery && recovery.goal.goalId !== this.goal?.goalId) {
      throw new Error('Existing checkpoint belongs to a different goal');
    }
    return recovery;
  }

  async #checkpointJob(jobId) {
    const existing = await this.#readCheckpoint();
    const activeJobs = existing ? existing.activeJobs : [];
    if (activeJobs.some(job => job.id === jobId)) {
      // A checkpointed job may have run already; never replay it without explicit reconciliation.
      const error = new Error(`Job ${jobId} is already checkpointed; its outcome must be reconciled before it can run again`);
      error.code = 'JOB_RECONCILIATION_REQUIRED';
      error.reconciliationRequired = true;
      throw error;
    }
    activeJobs.push({ id: jobId });
    await this.coordinator.checkpoint({ goal: existing?.goal ?? this.goal, dag: existing?.dag ?? null, activeJobs });
  }

  async #recordTerminalState(jobId, status) {
    const existing = await this.#readCheckpoint();
    const entry = existing?.activeJobs.find(job => job.id === jobId);
    if (!entry) throw new Error(`Job ${jobId} is not in the checkpoint`);
    // Only the status value is saved, never results or executor diagnostics.
    // The entry stays in activeJobs to keep replay protection.
    entry.status = status;
    await this.coordinator.checkpoint({ goal: existing.goal, dag: existing.dag ?? null, activeJobs: existing.activeJobs });
  }

  async start({ jobId, objective, input } = {}) {
    await this.#enqueue(() => this.#checkpointJob(jobId));
    // Dispatch outside the queue so executors still run concurrently.
    return this.registry.start({ jobId, objective, input });
  }

  /** Resolves after the job settles and its terminal status is durably saved; null for an unknown job. */
  async wait(jobId) {
    const snapshot = await this.registry.wait(jobId);
    if (snapshot === null) return null;
    const status = durableStatus(snapshot);
    try {
      await this.#enqueue(() => this.#recordTerminalState(jobId, status));
    } catch (cause) {
      const error = new Error(`${status} state of job ${jobId} could not be durably recorded; its outcome must be reconciled`, { cause });
      error.reconciliationRequired = true;
      throw error;
    }
    return snapshot;
  }
}
