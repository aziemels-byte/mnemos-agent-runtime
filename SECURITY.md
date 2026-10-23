# Security and publication boundaries

This is a small public code sample. It does not expose a network service and has no automatic model, account or credential discovery.

Do not put credentials, personal records, production checkpoints or diagnostic logs in issues or pull requests. For a concern that needs private handling, contact the author through the [GitHub profile](https://github.com/aziemels-byte) without including the sensitive material in a public message.

The publication gate permits a fixed file list, rejects symbolic links and common credential/private-data patterns, and verifies that source imports stay within this repository or use Node.js built-ins. A separate Gitleaks scan is also run before publication. Automated scanning reduces exposure risk; it does not prove that arbitrary content is safe.

Run the tests and publication gate before proposing changes. Use synthetic fixtures and an owner-controlled temporary directory for local state.
