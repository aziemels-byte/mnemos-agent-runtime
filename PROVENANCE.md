# Extraction record

This repository has an independent Git history. The original application history, runtime state and configuration were not imported.

| Public component | MNEMOS source module | Adaptation |
| --- | --- | --- |
| Candidate verifier | `packages/core/src/orchestration/autonomous-execution-adapters.mjs` | Isolated candidate verification; explicit changed-file adapter; malformed paths fail; zero test exit required |
| Phase job registry | `packages/core/src/controller/controller-investigation-jobs.mjs` | Removed controller and HTTP coupling; removed unconditional cancellation; confirmation requires settled owned execution and an explicit termination receipt |
| Provider failure classification | `packages/core/src/execution/provider-runtime-failure-classifier.mjs` | Selected classification rules; standalone Retry-After parsing; no account inventory, quota history or runtime profiles |
| Diagnostic redaction | `packages/core/src/security/secret-filter.mjs` | Selected credential patterns, plus quoted assignments and credential-bearing URLs |
| Goal recovery | `packages/core/src/goals/goal-recovery-coordinator.mjs` | Small store interface, versioned validation, unique job IDs and bounded checkpoint size |
| Atomic checkpoint storage | `packages/core/src/runtime/store.mjs` | Only the atomic-file pattern; no conversations, event journals, task definitions or private storage layout |

Formatting, interfaces, validation and tests were adapted for this standalone sample. It is not an unmodified mirror of the private runtime. Source hashes and extraction evidence are retained locally by the author.

Examples and test fixtures in this repository were created for this sample. They contain no production conversations, account profiles or execution logs.
