'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync(require.resolve('../../apps/vchera2/live-settings.js'),'utf8');
function fixture(){return{LIVE:false,entriesEnabled:false,openManaged:[],providerFresh:true,config:{margin:2100,leverage:600},provider:{wallet:{available:1980.693347,openPositions:[],operations:[]},entrySafety:{unknown:[]}},liveSettings:{revision:7,settings:{capital:{budgetUsdc:'100',marginUsdc:'10',leverage:100},schedule:{kind:'daily',enabled:false,timezone:'Europe/Moscow',start:'09:00',stop:'18:00',weekdays:[1,2,3,4,5],dateFrom:null,dateTo:null},features:{spreadEnabled:true,bookEnabled:true,bookUseHl:true,bookUseBf:true,bookUseBs:false,bookExitOnFlip:true,cushBoth:false,noLongs:false}}}};}
function harness(response={ok:true,data:{ok:true}}){const nodes=new Map(),requests=[];
 const node=()=>({value:'',checked:false,disabled:false,textContent:'',className:'',addEventListener(k,fn){this[k]=fn;}});
 const document={getElementById:id=>{if(!nodes.has(id))nodes.set(id,node());return nodes.get(id);}};const window={};
 vm.runInNewContext(source,{document,window,AbortSignal,fetch:async(url,options)=>{requests.push({url,options});return{ok:response.ok,json:async()=>response.data};}});
 return{nodes,requests,render:s=>window.LiveSettingsPanel.render(s)};}
test('capital budget does not edit actual wallet funds or old active strategy config',()=>{
 const h=harness(),s=fixture();h.render(s);assert.equal(h.nodes.get('live-budget').value,'100');assert.match(h.nodes.get('live-real-funds').textContent,/1.*980/);
 h.nodes.get('live-budget').value='500';h.nodes.get('live-budget').change();assert.equal(s.provider.wallet.available,1980.693347);assert.equal(s.config.margin,2100);assert.equal(h.requests.length,0);
});
test('save sends only settings draft/version and never arms LIVE or scheduler',async()=>{
 const h=harness(),s=fixture();h.render(s);h.nodes.get('live-margin').value='20';h.nodes.get('live-window-requested').checked=true;h.nodes.get('live-margin').change();await h.nodes.get('save-live-settings').click();
 assert.equal(h.requests.length,1);const r=h.requests[0];assert.equal(r.url,'/api/live/settings');assert.equal(r.options.headers['X-Vchera2-Control'],'1');const b=JSON.parse(r.options.body);assert.equal(b.expectedRevision,7);assert.equal(b.settings.capital.marginUsdc,'20');assert.equal(b.settings.schedule.enabled,true);assert.equal(b.live,undefined);assert.equal(b.authorized,undefined);assert.match(h.nodes.get('live-settings-message').textContent,/НЕ разрешены/);
});
test('active live/position/pending/unknown refuses frontend save without any POST',async()=>{
 const patches=[{LIVE:true},{entriesEnabled:true},{openManaged:[{}]},{provider:{wallet:{openPositions:[{}],operations:[]}}},{provider:{wallet:{openPositions:[],operations:[{}]}}},{provider:{wallet:{openPositions:[],operations:[]},entrySafety:{unknown:[{}]}}}];
 for(const patch of patches){const h=harness(),s={...fixture(),...patch};h.render(s);assert.equal(h.nodes.get('save-live-settings').disabled,true);await h.nodes.get('save-live-settings').click();assert.equal(h.requests.length,0);}
});
test('CAS failure preserves unsaved values until explicit reload; no optimistic activation',async()=>{
 const h=harness({ok:false,data:{ok:false,reasons:['REVISION_CONFLICT']}}),s=fixture();h.render(s);h.nodes.get('live-budget').value='50';h.nodes.get('live-budget').change();await h.nodes.get('save-live-settings').click();assert.match(h.nodes.get('live-settings-message').textContent,/REVISION_CONFLICT/);
 const updated=fixture();updated.liveSettings.revision=8;updated.liveSettings.settings.capital.budgetUsdc='200';h.render(updated);assert.equal(h.nodes.get('live-budget').value,'50');h.nodes.get('reload-live-settings').click();assert.equal(h.nodes.get('live-budget').value,'200');
});
test('missing backend leaves form disabled; provider strings are never inserted as HTML',()=>{
 const h=harness(),s=fixture();delete s.liveSettings;h.render(s);assert.equal(h.nodes.get('save-live-settings').disabled,true);assert.equal(source.includes('innerHTML'),false);assert.equal(source.includes('/api/trading/start'),false);assert.equal(source.includes('session_schedule_write'),false);
});
