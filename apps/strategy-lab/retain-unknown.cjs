'use strict';
const {censor}=require('../papertrade-shadow/engine-state.cjs');
// Run AFTER the underlying SIM fire returns: nonstop versions may release after noting UNKNOWN.
function retainUnknown(e,evidence){
 const r=e.intents.get(evidence.intentId);if(!r)throw new Error('UNKNOWN_INTENT_WITHOUT_WRITE_AHEAD_RECORD');
 const originalState=r.state;r.state='outcome_unknown';r.executionPriceMissing=true;r.executionAtMs=evidence.executionAtMs;
 censor(e,'RESEARCH_UNKNOWN_EXECUTION_PRICE');
 const result={intentId:r.id,kind:r.kind,originalState,state:r.state,reservedRaw:e.reserved.toString(),lockedRaw:e.locked.toString(),realisedUnchanged:true};
 e.note('research_unknown_risk_retained',result);return result;
}
module.exports={retainUnknown};
