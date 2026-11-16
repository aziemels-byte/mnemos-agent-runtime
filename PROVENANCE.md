# Extraction record

This repository has an independent Git history. The original application history, runtime state and configuration were not imported.

| Public component | MNEMOS source module | Adaptation |
| --- | --- | --- |
| Candidate verifier | `packages/core/src/orchestration/autonomous-execution-adapters.mjs` | Isolated candidate verification; explicit changed-file adapter; malformed paths fail; zero test exit required |
| Phase job registry | `packages/core/src/controller/controller-investigation-jobs.mjs` | Removed controller and HTTP coupling; removed unconditional cancellation; confirmation requires settled owned execution and an explicit termination receipt |
| Provider failure classification | `packages/core/src/execution/provider-runtime-failure-classifier.mjs` | Selected classification rules; standalone Retry-After parsing; no account inventory, quota history or runtime profiles |
| Diagnostic redaction | `packages/core/src/security/secret-filter.mjs` | Selected credential patterns, plus quoted assignments and credential-bearing URLs |
| Goal recovery | `packages/core/src/goals/goal-recovery-coordinator.mjs` | Small store interface, versioned validation, unique job IDs and bounded checkpoint size. Bounded inspections, inspector reasons, stale-status hints and the job-record inspector (`FilePhaseJobRegistry.jobInspector()`) were added in this repository |
| Effect idempotency | `packages/core/src/runtime/idempotency-registry.mjs` | Journal adapter with explicit UNKNOWN reconciliation; no production event records, storage backend or external effects. The cross-process `FileIdempotencyRegistry` and the shared decision helpers were added in this repository |
| Resource leases | `packages/core/src/resources/resource-lease-manager.mjs` | Single-process lease state; injected node registry, no actual GPU/CPU allocation or production topology. The same-host file coordinator (`FileResourceLeaseManager`), fencing tokens with `FencingGuard`, `keepLeaseAlive`, lock inspection and `forceUnlock`, and case-insensitive resource comparison were added in this repository and are not extracted from the private module |
| Quality evaluation | `packages/core/src/self-development/automated-evaluation-gate.mjs` | Requires completed test receipts with successful exit codes and evidence identifiers rather than booleans alone |
| Candidate comparison | `packages/core/src/self-development/candidate-comparison-gate.mjs` | Validates numeric metrics and matched positive sample counts before comparing regressions |
| Self-modification policy | `packages/core/src/self-development/isolated-self-modification-gate.mjs` | Requires declared isolation and explicit auto-merge prohibition; no actual workspace modification |
| Release qualification | `packages/core/src/self-development/qualification-gate.mjs` | Evaluates eight traceable evidence categories; never releases automatically |
| Quality floor | `packages/core/src/routing/quality-floor-policy.mjs` | Standalone selection of quality-qualified routes without accounts, calibrated scoring or execution |
| Atomic checkpoint storage | `packages/core/src/runtime/store.mjs` | Only the atomic-file pattern; no conversations, event journals, task definitions or private storage layout. Cross-process save locking and revision compare-and-swap were added in this repository |

The shared same-host lock (`src/directory-lock.mjs`), including its optional directory flush after a replacement, the durable job record store (`src/job-store.mjs`) `FilePhaseJobRegistry` (`src/file-phase-job-registry.mjs`) and the supervised process trees (`src/process-tree.mjs`) were written for this repository and are not extracted from MNEMOS. The in-memory registry's outcome rules were factored out so that both registries share them.

Formatting, interfaces, validation and tests were adapted for this standalone sample. It is not an unmodified mirror of the private runtime. Source hashes and extraction evidence are retained locally by the author.

Examples and test fixtures in this repository were created for this sample. They contain no production conversations, account profiles or execution logs.
