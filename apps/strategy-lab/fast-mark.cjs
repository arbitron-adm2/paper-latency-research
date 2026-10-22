'use strict';
// Static replay scenarios: skip repeated settlement math when price and every
// monetary/position input are unchanged. Clocks, collateral time and source wallet
// timestamps still advance for EVERY input. Live executor is unaffected.
function installMarkCache(e){
 const original=e.mark.bind(e);let last=null;const scenario=e.scenario;
 Object.freeze(scenario);if(scenario.instrumentRow)Object.freeze(scenario.instrumentRow);
 function stamp(px){const p=e.positions.size===1?e.positions.values().next().value:null;return[px,e.balance,e.locked,e.reserved,e.positions.size,p?.positionId,p?.entry,p?.margin,p?.leverage,p?.long,p?.censored,e.accountingQuarantined,e.config.bustBufferRaw,e.bestEquity,e.drawdown];}
 e.mark=function(t){
  if(t<e.now||e.positions.size>1||e.scenario!==scenario){last=null;return original(t);}
  const before=e.now;let px;try{e.now=t;px=e.quote();}finally{e.now=before;}
  const current=stamp(px);
  if(last&&current.every((x,i)=>x===last[i])){const elapsed=t-before;e.exposureMs+=elapsed*e.positions.size;e.capitalMs+=BigInt(Math.round(elapsed))*(e.locked+e.reserved);e.now=t;e.bridge.setTime(t);return;}
  original(t);last=stamp(px);
 };
 return e;
}
module.exports={installMarkCache};
