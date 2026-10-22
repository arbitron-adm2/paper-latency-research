'use strict';
// Trusted runtime context, never built from POST body. Quantisation only floors spending capacity.
function floorUsdc(value){
 if(typeof value!=='number'||!Number.isFinite(value)||value<0||value>1000000000)return null;
 const micro=Math.floor(value*1000000);if(!Number.isSafeInteger(micro))return null;
 return `${Math.floor(micro/1000000)}.${String(micro%1000000).padStart(6,'0')}`;
}
function liveSettingsContext(snapshot){
 const wallet=snapshot.provider?.wallet;
 return{LIVE:snapshot.LIVE,entriesEnabled:snapshot.entriesEnabled,providerFresh:snapshot.providerFresh,openManaged:snapshot.openManaged,
  provider:{...snapshot.provider,wallet:wallet?{...wallet,available:floorUsdc(wallet.available),balance:floorUsdc(wallet.balance)}:undefined},
  instrumentLimits:{confirmed:false},riskBudget:{confirmed:false},contractCapability:{btcEntryWithDeadline:false,independentClose:false},safetyReady:false,
  source:'PARENT_RUNTIME_CONTEXT_NO_REQUEST_AUTHORITY',fundsQuantisation:'FLOORED_USDC_6DP_NOT_A_WALLET_MUTATION'};
}
module.exports={liveSettingsContext,floorUsdc};
