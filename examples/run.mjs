import { runVerificationDemo } from './verify-candidate.mjs';
import { runRecoveryDemo } from './recoverable-task.mjs';

console.log(JSON.stringify({
  candidateVerification: await runVerificationDemo(),
  checkpointRecovery: await runRecoveryDemo()
}, null, 2));
