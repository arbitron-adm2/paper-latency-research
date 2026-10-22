'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const script=fs.readFileSync(require.resolve('../../apps/vchera2/instrument-dashboard.js'),'utf8');
function harness(){
 const elements=new Map(),requests=[];let now=0;
 const node=()=>({textContent:'',className:'',value:'',disabled:false,children:[],append(...a){this.children.push(...a);},replaceChildren(...a){this.children=a;},addEventListener(k,fn){this[k]=fn;}});
 const document={getElementById(id){if(!elements.has(id))elements.set(id,node());return elements.get(id);},createElement:node};
 const window={};const context=vm.createContext({document,window,Date,performance:{now:()=>now},AbortSignal,fetch:async(url,options)=>{requests.push({url,options});return{ok:true,json:async()=>({ok:true,message:'Выбор сохранён. LIVE выключен.'})};}});
 vm.runInContext(script,context);return{elements,requests,render:s=>window.InstrumentDashboard.render(s),tick:t=>window.InstrumentDashboard.tick(t),advance:ms=>{now+=ms;}};
}
function fixture(){
 const at=Date.now();const market=(symbol,mid,id,digits)=>({symbol,instrumentId:id,priceDigits:digits,maxFeedAgeMs:1500,feeds:Object.fromEntries(['paper','hl','bf','bs'].map((s,i)=>[s,{mid:mid+i*.01,bid:mid,ask:mid+.02,ageMs:20,bookAgeMs:s==='paper'?null:100,bookNA:s==='paper',imb:s==='paper'?null:.5,receivedAtMs:at}])),execution:{selected:true,reason:`${symbol}: LIVE выключен`},positions:[],stats:{closedTrades:symbol==='BTC'?13:0,adjustedPnlUsd:symbol==='BTC'?-334.97:null},history:symbol==='BTC'?[{symbol,instrumentId:id,position_id:'42',side:'S',entry:81541.5,exit:81569.5,real_pnl:-432.66}]:[]});
 return{observedAt:new Date(at).toISOString(),LIVE:false,entriesEnabled:false,providerFresh:true,config:{margin:2100,leverage:600},instrumentSelection:{mode:'BOTH',selectedSymbols:['BTC','ETH'],reason:'Guard/shadow ожидаются'},markets:{BTC:market('BTC',81500.1,0,1),ETH:market('ETH',3300.11,1,2)}};
}
test('independent BTC/ETH quote cards, histories and precision; no BTC pnl copied to ETH',()=>{
 const h=harness();h.render(fixture());
 assert.match(h.elements.get('BTC-paper').textContent,/81.*500,1/);assert.match(h.elements.get('ETH-paper').textContent,/3.*300,11/);
 assert.match(h.elements.get('BTC-pnl').textContent,/334,97/);assert.match(h.elements.get('ETH-pnl').textContent,/нет истории/);
 assert.equal(h.elements.get('BTC-history').children.length,1);assert.match(h.elements.get('ETH-history').children[0].children[0].textContent,/ETH/);
 assert.equal(h.elements.get('ETH-feeds').children[0].children[3].textContent,'N/A · не отдельный стакан');
 assert.equal(h.requests.length,0);
});
test('both asset spreads expire locally even when latest snapshot is cached',()=>{
 const h=harness();const s=fixture();h.render(s);assert.match(h.elements.get('ETH-fresh').textContent,/свежие/);
 h.advance(4000);h.tick(Date.now()+4000);
 for(const symbol of ['BTC','ETH']){assert.match(h.elements.get(`${symbol}-fresh`).textContent,/устарели/);assert.match(h.elements.get(`${symbol}-paper-spread`).textContent,/нет свежей пары/);}
 assert.equal(h.elements.get('save-instruments').disabled,true);
});
test('explicit selection sends only mode, never order params or a trading start',async()=>{
 const h=harness();h.render(fixture());h.elements.get('instrument-mode').value='ETH';h.elements.get('instrument-mode').change();await h.elements.get('save-instruments').click();
 assert.equal(h.requests.length,1);const request=h.requests[0];assert.equal(request.url,'/api/instruments/selection');assert.equal(request.options.headers['X-Vchera2-Control'],'1');
 assert.deepEqual(JSON.parse(request.options.body),{mode:'ETH'});assert.match(h.elements.get('instrument-message').textContent,/LIVE выключен/);
});
test('unsafe real-time state never permits selection save and provider text is not HTML',async()=>{
 const h=harness(),s=fixture();s.LIVE=true;s.markets.ETH.execution.reason='<img src=x onerror=alert(1)>';h.render(s);
 assert.equal(h.elements.get('save-instruments').disabled,true);await h.elements.get('save-instruments').click();assert.equal(h.requests.length,0);
 assert.equal(h.elements.get('ETH-execution').textContent,s.markets.ETH.execution.reason);assert.equal(script.includes('innerHTML'),false);
});
