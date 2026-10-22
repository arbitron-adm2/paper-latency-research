'use strict';
// Absolute virtual entry limit. This is an immutable research commitment, NOT an
// EIP-712 signature, and never invokes a wallet, signer, register or exchange API.
const {sha}=require('./strategy.cjs');
function virtualEntryLimit({intentId,decidedAtMs,signalValidUntilMs,safetySeconds=1}){
  if(!intentId||!Number.isSafeInteger(decidedAtMs)||!Number.isSafeInteger(signalValidUntilMs)||signalValidUntilMs<=decidedAtMs||!Number.isInteger(safetySeconds)||safetySeconds<0||safetySeconds>10)throw new Error('Invalid absolute entry limit');
  // Contract comparison is block.timestamp <= deadlineSec (equality allowed).
  // floor(validUntilMs/1000)-1 reserves the WHOLE final allowed second, not just
  // its first millisecond. Additional safetySeconds shorten, never extend validity.
  const deadlineSec=Math.floor(signalValidUntilMs/1000)-1-safetySeconds;
  const payload={type:'virtual-signed-absolute-entry-limit',intentId,decidedAtMs,signalValidUntilMs,deadlineSec,safetySeconds,
    inequality:'executionBlockSeconds <= deadlineSec',validity:'anchored at original decision; immutable; never renewed',
    maxAllowedExecutionMs:(deadlineSec+1)*1000-1,realSignature:false};
  return Object.freeze({...payload,commitmentSha256:sha(JSON.stringify(payload))});
}
function entryAllowed(limit,executionBlockSeconds){
  if(!Number.isSafeInteger(executionBlockSeconds)||executionBlockSeconds<0)throw new Error('Block seconds required');
  return executionBlockSeconds<=limit.deadlineSec;
}
module.exports={virtualEntryLimit,entryAllowed};
