'use strict';
// Integer 1e18 research accounting. No float conversion in realised ledger math.
const UNIT = 10n ** 18n;
function raw(value) {
  const s = String(value);
  if (!/^-?\d+(\.\d{1,18})?$/.test(s)) throw new Error('Invalid fixed decimal: '+s);
  const negative = s.startsWith('-'), [w,f=''] = s.replace('-','').split('.');
  return (negative ? -1n : 1n) * (BigInt(w)*UNIT+BigInt(f.padEnd(18,'0')));
}
function decimal(value) {
  value=BigInt(value); const sign=value<0n?'-':''; if(value<0n)value=-value;
  return sign+(value/UNIT)+'.'+(value%UNIT).toString().padStart(18,'0');
}
function price(value, scale=10n) {
  const v=raw(value)*scale;
  if(v%UNIT)throw new Error('Price off tick');
  return v/UNIT;
}
function gross(margin, leverage, entry, exit, long) {
  return BigInt(margin)*BigInt(leverage)*(BigInt(exit)-BigInt(entry))*(long?1n:-1n)/BigInt(entry);
}
function reconstructedPayout(margin,leverage,entry,exit,long,row,winFeeRaw) {
  const a=BigInt(entry),m=BigInt(exit),threshold=a/50000n,mar=BigInt(margin);
  const effective=long?(m>a+threshold?m-threshold:m>a?a:m):(m+threshold<a?m+threshold:m<a?a:m);
  const diff=long?effective-a:a-effective;
  const g=mar*BigInt(leverage)*diff/a;
  if(g<=0n){const adjusted=g < -mar ? -mar : g;return {adjusted,fee:0n,cash:adjusted};}
  const distance=effective>a?effective-a:a-effective,v=distance*UNIT/a;
  const y=v*BigInt(row[12])/UNIT,x=1000000n*v*BigInt(row[13])/UNIT;
  const b=y?UNIT*UNIT/y:(2n**256n-1n),s=x?BigInt(row[18])*UNIT/x:(2n**256n-1n);
  if(b>UNIT*UNIT||s>UNIT*UNIT)return {adjusted:0n,fee:0n,cash:0n};
  const factor=(UNIT-BigInt(row[17]))*UNIT/(UNIT+b+s),adjusted=g*factor/UNIT;
  const fee=adjusted*BigInt(winFeeRaw)/UNIT;
  return {adjusted,fee,cash:adjusted-fee};
}
function settle({margin,leverage,entry,exit,long,liquidated=false}, model) {
  const g=gross(margin,leverage,entry,exit,long);
  if(liquidated)return {gross:g,adjusted:-BigInt(margin),fee:0n,funding:0n,cash:-BigInt(margin),semantics:'hypothetical full-margin liquidation; not an exact threshold model'};
  // Reconstructed historical formula is conditional on the supplied parameter snapshot.
  const reconstructed=model.payoutMode==='reconstructed-deadband-haircut'
    ?reconstructedPayout(margin,leverage,entry,exit,long,model.instrumentRow,model.winFeeRaw):null;
  let adjusted=reconstructed?.adjusted??g;
  if(!reconstructed&&g>0n)adjusted=g*BigInt(model.positivePayoutBps)/10000n;
  const fee=reconstructed?.fee??(adjusted>0n?adjusted*BigInt(model.winFeeBps)/10000n:0n);
  const funding=BigInt(margin)*BigInt(model.fundingBps||0)/10000n;
  return {gross:g,adjusted,fee,funding,cash:adjusted-fee-funding,
    semantics:'scenario adjusted minus scenario fee/funding; NOT verified Paper cash payout'};
}
module.exports={UNIT,raw,decimal,price,gross,settle,reconstructedPayout};
