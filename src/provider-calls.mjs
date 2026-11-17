import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { DirectoryLock, readBoundedFile, replaceFile } from './directory-lock.mjs';
import { classifyProviderFailure } from './provider-failure.mjs';

const PROVIDER = /^[a-z0-9][a-z0-9._-]{0,63}$/i;
const STATE_SCHEMA = 'mnemos.rate-limits.v1';
const STATE_TEMPORARY = /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}\.rate\.tmp$/;
const MAX_STATE_BYTES = 1024 * 1024;
const limitError = (code, message, details = {}) => Object.assign(new Error(message), { code, ...details });

function requireProvider(provider) {
  if (typeof provider !== 'string' || !PROVIDER.test(provider)) throw new TypeError('provider must be 1 to 64 letters, digits, ., _ or -');
  return provider;
}

/**
 * Rate limits and cooldowns per provider, shared by the processes of one host
 * through a state directory (rate-limits.json, under rate-limits.lock, like
 * the lease and checkpoint state).
 *
 * - limits: { <provider>: { requests, perMs } }: at most `requests` calls per
 *   `perMs`, as a token bucket that refills continuously. A provider without a
 *   limit is only subject to its cooldown.
 * - cooldown(provider, ms): no call to that provider starts before then, in any
 *   process. Used for Retry-After; a later, longer cooldown extends it.
 * - acquire(provider, { signal, maxWaitMs }): wait for a slot, then take it.
 *   Rejects with RATE_LIMIT_WAIT_EXCEEDED (with waitMs) instead of waiting
 *   longer than maxWaitMs, and with the signal's reason when it aborts.
 * All processes must share the host clock.
 */
export class FileRateLimiter {
  #lock;

  constructor({ directory, limits = {}, clock = () => Date.now(), lockTimeoutMs = 5_000 } = {}) {
    if (typeof directory !== 'string' || !directory.trim()) throw new TypeError('A rate limit state directory is required');
    if (limits === null || typeof limits !== 'object') throw new TypeError('limits must be an object');
    for (const [provider, limit] of Object.entries(limits)) {
      requireProvider(provider);
      if (!Number.isInteger(limit?.requests) || limit.requests < 1 || !Number.isInteger(limit?.perMs) || limit.perMs < 1) {
        throw new TypeError(`The limit of ${provider} needs positive integers requests and perMs`);
      }
    }
    this.directory = path.resolve(directory);
    this.stateFile = path.join(this.directory, 'rate-limits.json');
    this.limits = structuredClone(limits);
    this.clock = clock;
    this.#lock = new DirectoryLock({ directory: this.directory, name: 'rate-limits.lock', timeoutMs: lockTimeoutMs,
      timeoutCode: 'RATE_LIMIT_LOCK_TIMEOUT', ownedTemporary: name => STATE_TEMPORARY.test(name) });
  }

  async acquire(provider, { signal, maxWaitMs = Infinity } = {}) {
    requireProvider(provider);
    const started = this.clock();
    for (;;) {
      signal?.throwIfAborted();
      const waitMs = await this.#update(provider, (entry, limit, now) => {
        if (now < entry.cooldownUntil) return entry.cooldownUntil - now;
        if (!limit) return 0;
        if (entry.tokens >= 1) { entry.tokens -= 1; return 0; }
        return Math.ceil((1 - entry.tokens) * limit.perMs / limit.requests);
      });
      if (waitMs === 0) return { waitedMs: this.clock() - started };
      if (this.clock() - started + waitMs > maxWaitMs) {
        throw limitError('RATE_LIMIT_WAIT_EXCEEDED', `${provider} has no slot for another ${waitMs} ms`, { provider, waitMs });
      }
      try { await sleep(waitMs, undefined, { signal }); }
      catch (error) { throw signal?.aborted ? signal.reason : error; } // the abort's own reason, as when it came first
    }
  }

  async cooldown(provider, durationMs) {
    requireProvider(provider);
    if (!Number.isFinite(durationMs) || durationMs < 0) throw new TypeError('durationMs must be a non-negative number');
    await this.#update(provider, (entry, limit, now) => { entry.cooldownUntil = Math.max(entry.cooldownUntil, now + durationMs); });
  }

  /** { tokens, cooldownUntil } of one provider now (tokens: null without a limit). */
  async state(provider) {
    requireProvider(provider);
    return this.#update(provider, (entry, limit) => ({ tokens: limit ? entry.tokens : null, cooldownUntil: entry.cooldownUntil }));
  }

  inspectLock() { return this.#lock.inspect(); }
  forceUnlock(request) { return this.#lock.forceUnlock(request); }

  // Under the lock: load the state, refill the provider's bucket up to now, apply `change`, save.
  #update(provider, change) {
    return this.#lock.run(async () => {
      const state = await this.#load();
      const now = this.clock();
      const limit = this.limits[provider];
      const entry = state.providers[provider] ?? { tokens: limit?.requests ?? 0, updatedAt: now, cooldownUntil: 0 };
      if (limit) entry.tokens = Math.min(limit.requests, entry.tokens + Math.max(0, now - entry.updatedAt) * limit.requests / limit.perMs);
      entry.updatedAt = now;
      const result = change(entry, limit, now);
      state.providers[provider] = entry;
      await replaceFile(this.stateFile, JSON.stringify({ schema: STATE_SCHEMA, ...state }) + '\n', '.rate.tmp', { syncDirectory: true });
      return result;
    });
  }

  async #load() {
    const bytes = await readBoundedFile(this.stateFile, MAX_STATE_BYTES,
      reason => limitError('RATE_LIMIT_STATE_INVALID', reason === 'TOO_LARGE' ? 'Rate limit state exceeds 1 MiB' : 'Rate limit state is not a file'));
    if (bytes === null) return { providers: {} };
    let value;
    try { value = JSON.parse(bytes.toString('utf8')); }
    catch { throw limitError('RATE_LIMIT_STATE_INVALID', 'Rate limit state is not valid JSON'); }
    const valid = value?.schema === STATE_SCHEMA && value.providers !== null && typeof value.providers === 'object' &&
      Object.entries(value.providers).every(([provider, entry]) => PROVIDER.test(provider) &&
        [entry?.tokens, entry?.updatedAt, entry?.cooldownUntil].every(number => Number.isFinite(number) && number >= 0));
    if (!valid) throw limitError('RATE_LIMIT_STATE_INVALID', 'Rate limit state is invalid');
    return { providers: value.providers };
  }
}

// Outcomes a failed attempt can have, from classifyProviderFailure's requestOutcome.
const NO_EFFECT = new Set(['NOT_SENT', 'REJECTED']);

// How a call ended, thrown out of the attempts so that an idempotency registry records it.
class CallStopped extends Error {
  constructor(outcome) {
    super(`Provider call ${outcome.status}`);
    this.outcome = outcome;
  }
}

/**
 * Calls a provider with controlled retries. Each attempt waits for the rate
 * limiter (if any), runs operation({ signal, attempt }), and on failure is
 * classified by classifyProviderFailure. A failed attempt is retried only when
 * repeating it is safe and may succeed:
 * - safe: the attempt certainly had no effect (requestOutcome NOT_SENT or
 *   REJECTED), or the caller declares the operation idempotent (the provider
 *   deduplicates it). An AMBIGUOUS failure of anything else is never repeated:
 *   the call is UNCERTAIN and needs reconciliation.
 * - may succeed: the failure is retryable, or the provider sent Retry-After.
 * The delay before the next attempt is exponential backoff with full jitter
 * (baseDelayMs doubling, at most maxDelayMs), and never shorter than
 * Retry-After, which also becomes a cooldown shared through the limiter. A
 * delay (or limiter wait) longer than maxDelayMs ends the call instead. The
 * signal stops any wait at once and is passed to the operation.
 *
 * With `key` (and `effects`, an IdempotencyRegistry or FileIdempotencyRegistry)
 * the whole call is one keyed effect: a completed call is replayed instead of
 * run again, a call still running elsewhere is IN_PROGRESS, and an UNCERTAIN
 * one stays UNKNOWN until reconciled (effects.reconcile); a call that failed
 * without any effect may be run again with retryFailed: true.
 *
 * Resolves to { status, attempts, ... }, never rejects for a provider failure:
 * - SUCCEEDED: result (replayed: true when taken from the idempotency record)
 * - FAILED: failure, effectAbsent (true only if no attempt can have had an effect), retryAfterMs
 * - UNCERTAIN: failure, reconciliationRequired: true
 * - CANCELLED: the signal aborted before or between attempts; effectAbsent as for FAILED
 * - IN_PROGRESS, CONFLICT (the key was used with another request)
 * attempts: [{ attempt, at, outcome, kind, requestOutcome, statusCode, retryAfterMs, message, delayMs }].
 */
export class ProviderCaller {
  constructor({ limiter = null, effects = null, maxAttempts = 3, baseDelayMs = 500, maxDelayMs = 30_000, random = Math.random } = {}) {
    if (limiter !== null && (typeof limiter.acquire !== 'function' || typeof limiter.cooldown !== 'function')) {
      throw new TypeError('limiter must be a FileRateLimiter');
    }
    if (effects !== null && typeof effects.execute !== 'function') throw new TypeError('effects must be an idempotency registry');
    for (const [name, value, min] of [['maxAttempts', maxAttempts, 1], ['baseDelayMs', baseDelayMs, 0], ['maxDelayMs', maxDelayMs, 0]]) {
      if (!Number.isInteger(value) || value < min) throw new TypeError(`${name} must be an integer of at least ${min}`);
    }
    if (typeof random !== 'function') throw new TypeError('random must be a function');
    Object.assign(this, { limiter, effects, maxAttempts, baseDelayMs, maxDelayMs, random });
  }

  async call({ provider, operation, signal, key, request, idempotent = false, retryFailed = false } = {}) {
    requireProvider(provider);
    if (typeof operation !== 'function') throw new TypeError('operation must be a function');
    if (signal !== undefined && !(signal instanceof AbortSignal)) throw new TypeError('signal must be an AbortSignal');
    if (idempotent !== true && idempotent !== false) throw new TypeError('idempotent must be a boolean');
    const attempts = [];
    const run = () => this.#attempts({ provider, operation, signal, idempotent, attempts });
    if (key === undefined) {
      try { return { status: 'SUCCEEDED', result: await run(), attempts }; }
      catch (error) { if (error instanceof CallStopped) return error.outcome; throw error; }
    }
    if (!this.effects) throw new TypeError('A key needs an idempotency registry (effects)');
    let executed;
    try {
      executed = await this.effects.execute({ key, request, operation: run, retryFailed,
        failureProvesNoEffect: error => error instanceof CallStopped && error.outcome.effectAbsent === true });
    } catch (error) {
      // The registry recorded UNKNOWN: an uncertain call, or an outcome it could not record.
      if (error?.idempotency?.status === 'IDEMPOTENCY_UNKNOWN') {
        return { status: 'UNCERTAIN', reconciliationRequired: true, attempts,
          ...(error instanceof CallStopped && error.outcome.failure ? { failure: error.outcome.failure } : {}) };
      }
      if (error instanceof CallStopped) return error.outcome; // recorded FAILED: no attempt had an effect
      throw error;
    }
    const { status } = executed;
    if (status === 'IDEMPOTENCY_EXECUTED') return { status: 'SUCCEEDED', result: executed.result, attempts };
    if (status === 'IDEMPOTENT_REPLAY') return { status: 'SUCCEEDED', result: executed.result, replayed: true, attempts };
    if (status === 'IDEMPOTENCY_IN_PROGRESS') return { status: 'IN_PROGRESS', attempts };
    if (status === 'IDEMPOTENCY_UNKNOWN') return { status: 'UNCERTAIN', reconciliationRequired: true, attempts };
    if (status === 'IDEMPOTENCY_FAILED') return { status: 'FAILED', effectAbsent: executed.retryAllowed === true, attempts };
    return { status: 'CONFLICT', attempts };
  }

  // The attempts of one call. Resolves to the operation's result; otherwise throws CallStopped.
  async #attempts({ provider, operation, signal, idempotent, attempts }) {
    const effectAbsent = () => attempts.every(attempt => NO_EFFECT.has(attempt.requestOutcome));
    const stop = (status, extra = {}) => new CallStopped({ status, attempts, ...extra,
      ...(['FAILED', 'CANCELLED'].includes(status) ? { effectAbsent: effectAbsent() } : {}),
      ...(status === 'UNCERTAIN' ? { reconciliationRequired: true } : {}) });
    for (let attempt = 1; ; attempt++) {
      if (signal?.aborted) throw stop('CANCELLED');
      if (this.limiter) {
        try { await this.limiter.acquire(provider, { signal, maxWaitMs: this.maxDelayMs }); }
        catch (error) {
          if (signal?.aborted) throw stop('CANCELLED');
          if (error.code === 'RATE_LIMIT_WAIT_EXCEEDED') throw stop('FAILED', { reason: 'RATE_LIMIT_WAIT_TOO_LONG', retryAfterMs: error.waitMs });
          throw error;
        }
      }
      const at = new Date().toISOString();
      try {
        const result = await operation({ signal, attempt });
        attempts.push({ attempt, at, outcome: 'SUCCEEDED' });
        return result;
      } catch (error) {
        const failure = classifyProviderFailure(error);
        const record = { attempt, at, outcome: 'FAILED', kind: failure.kind, requestOutcome: failure.requestOutcome,
          statusCode: failure.statusCode, retryAfterMs: failure.retryAfterMs, message: failure.message };
        attempts.push(record);
        if (failure.retryAfterMs !== null && this.limiter) await this.limiter.cooldown(provider, failure.retryAfterMs);
        if (failure.requestOutcome === 'AMBIGUOUS' && !idempotent) throw stop('UNCERTAIN', { failure });
        if (signal?.aborted) throw stop(effectAbsent() ? 'CANCELLED' : 'UNCERTAIN', { failure });
        if (!(failure.retryable || failure.retryAfterMs !== null) || attempt >= this.maxAttempts) {
          throw stop('FAILED', { failure, retryAfterMs: failure.retryAfterMs });
        }
        const backoff = Math.floor(this.random() * Math.min(this.maxDelayMs, this.baseDelayMs * 2 ** (attempt - 1)));
        const delayMs = Math.max(backoff, failure.retryAfterMs ?? 0);
        if (delayMs > this.maxDelayMs) throw stop('FAILED', { failure, reason: 'RETRY_AFTER_TOO_LONG', retryAfterMs: failure.retryAfterMs });
        record.delayMs = delayMs;
        try { await sleep(delayMs, undefined, { signal }); }
        catch (sleepError) { if (signal?.aborted) throw stop('CANCELLED', { failure }); throw sleepError; }
      }
    }
  }
}
