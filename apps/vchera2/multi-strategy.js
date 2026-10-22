'use strict';
(() => {
 const $=id=>document.getElementById(id),PRIMARY='y2_strict_primary';
 let state=null,data=null,busy=false,last=0,asset='ALL',selected='ALL',tab='trades',adding=false,requestId=null;
 const text=(id,v)=>{const n=$(id);if(n&&n.textContent!==String(v))n.textContent=String(v);};
 const fmt=(v,d=2)=>v!=null&&Number.isFinite(Number(v))?Number(v).toLocaleString('ru-RU',{minimumFractionDigits:d,maximumFractionDigits:d}):'—';
 const raw=(v,scale=1e18)=>v==null?null:Number(v)/scale;
 const time=v=>Number.isFinite(v)?new Date(v).toLocaleTimeString('ru-RU',{timeZone:'Europe/Moscow'}):'—';
 const node=(tag,value,cls)=>{const n=document.createElement(tag);if(value!=null)n.textContent=value;if(cls)n.className=cls;return n;};
 const matches=(id,a)=> (selected==='ALL'||selected===id)&&(asset==='ALL'||asset===a);
 const models=()=>data?.models||[];
 const names=()=>new Map(models().map(m=>[m.id,m.label||m.id]));
 function table(id,rows,empty,n){const b=$(id);if(!b)return;b.replaceChildren(...(rows.length?rows.map(values=>{const tr=node('tr');values.forEach(v=>{const td=node('td');td.append(v instanceof Node?v:document.createTextNode(String(v??'—')));tr.append(td);});return tr;}):[(()=>{const tr=node('tr'),td=node('td',empty,'muted');td.colSpan=n;tr.append(td);return tr;})()]));}
 function choose(id){selected=id;$('shadow-model-filter').value=id;draw();}
 function cards(){
  const shown=models().filter(m=>asset==='ALL'||m.asset===asset),old=$('shadow-model-filter').value;
  const options=[node('option','Все стратегии'),...shown.map(m=>{const o=node('option',m.label||m.id);o.value=m.id;return o;})];options[0].value='ALL';
  $('shadow-model-filter').replaceChildren(...options);$('shadow-model-filter').value=shown.some(m=>m.id===selected)?selected:'ALL';
  $('shadow-models').replaceChildren(...shown.map(m=>{
   const card=node('button',null,'strategy-card'+(m.id===selected?' selected':''));card.type='button';card.setAttribute('aria-pressed',String(m.id===selected));card.dataset.modelId=m.id;
   const heading=node('div',null,'strategy-card-heading');heading.append(node('span',m.asset,'strategy-asset'),node('span',m.error?'Ошибка':m.status==='RUNNING'?'Наблюдает':m.status==='WARMUP'?'Прогрев':m.status,'pill '+(m.status==='RUNNING'?'good':'warn')));
   card.append(heading,node('h3',m.label||m.id),node('p',m.timingIsWarning?'ETA предупреждает':'ETA блокирует','strategy-policy'));
   const net=m.priceEpoch?.netUsdc??m.netUsdc;
   const pnl=node('strong',fmt(net)+' USDC',Number(net)>0?'pos':Number(net)<0?'neg':'');card.append(pnl,node('span',m.priceEpoch?'PnL SIM · после исправления цены':'PnL SIM · прежняя модель цены','strategy-caption'));
   const c=m.priceEpoch?.counts||m.counts||{},stats=node('div',null,'strategy-card-stats');
   for(const [label,value]of[['Заявки',c.submitted||0],['Входы',c.filled||0],['Закрытия',c.closed||0]]){const x=node('div');x.append(node('b',String(value)),node('span',label));stats.append(x);}card.append(stats);
   card.append(node('p',`В работе ${fmt(Number(m.reservedUsdc||0)+Number(m.lockedUsdc||0))} USDC · отказы ${c.entryPolicyRejected||0}`,'strategy-caption'));
   if(m.priceEpoch)card.append(node('p',`Новая серия с ${time(m.priceEpoch.startedAtMs)} · неопределённых исполнений ${m.priceUnresolved??c.executionPriceUnknown??0}. Вся история SIM: ${fmt(m.netUsdc)} USDC, включает прежние цены.`,'strategy-caveat'));
   if(m.asset==='ETH')card.append(node('p','Задержка ETH пока не измерена','strategy-caveat'));if(m.error)card.append(node('p',m.error,'neg'));
   card.addEventListener('click',()=>choose(m.id));return card;
  }));
 }
 function detailButton(r){if(!r.id)return '—';const b=node('button','Подробнее','strategy-detail-button');b.type='button';b.addEventListener('click',async()=>{b.disabled=true;try{const response=await fetch('/api/shadow/decision?id='+r.id,{signal:AbortSignal.timeout(3000)});if(!response.ok)throw new Error('Проверки пока недоступны');const d=await response.json();text('shadow-decision-json',JSON.stringify(d,null,2));$('shadow-decision-detail').hidden=false;$('shadow-decision-detail').open=true;}catch(e){text('shadow-decision-json',e.message);$('shadow-decision-detail').hidden=false;$('shadow-decision-detail').open=true;}finally{b.disabled=false;}});return b;}
 function draw(){
  if(!data)return;const h=data,labels=names(),label=id=>labels.get(id)||id,p=state?.shadowProcess||h.process||{},fresh=Date.now()-h.atMs<6000;
  text('shadow-history-status',!fresh?'Снимок устарел':p.running?'Shadow работает · SIM':'Shadow остановлен');$('shadow-history-status').className='pill '+(fresh&&p.running?'good':'warn');
  text('shadow-history-process',`${models().length} стратегий · PID ${p.pid||'—'} · общий поток · история сохраняется`);
  cards();text('shadow-selected-title',selected==='ALL'?'История всех стратегий':label(selected));text('shadow-detail-scope','Последние 40 записей каждого раздела · МСК');
  const trades=[...(h.modelTrades||[]),...(h.trades||[]).map(r=>({modelId:PRIMARY,asset:'BTC',side:'—',openedMs:r.opened_ms,closedMs:r.closed_ms,entryRaw:r.entry_raw,exitRaw:r.exit_raw,cashRaw:r.cash_raw,reason:r.reason}))].filter(r=>matches(r.modelId,r.asset)).sort((a,b)=>b.closedMs-a.closedMs);
  table('comparison-trades',trades.map(r=>[label(r.modelId)+' / '+r.asset,`${time(r.openedMs)} → ${time(r.closedMs)}`,`${r.side} · ${fmt(raw(r.entryRaw,r.asset==='ETH'?100:10))} → ${fmt(raw(r.exitRaw,r.asset==='ETH'?100:10))}`,fmt(raw(r.cashRaw))+' USDC',r.reason]),'Закрытых SIM-сделок в этой выборке пока нет',5);
  const positions=[...(h.modelPositions||[]),...(h.positions||[]).map(r=>({...r,modelId:PRIMARY,asset:'BTC'}))].filter(r=>matches(r.modelId,r.asset));
  const intents=[...(h.modelIntents||[]),...(h.intents||[]).map(r=>({...r,modelId:PRIMARY,asset:'BTC'}))].filter(r=>matches(r.modelId,r.asset));
  table('comparison-positions',[...positions.map(r=>[label(r.modelId),r.positionId,`${r.censored?'UNKNOWN':'Открыта'} / ${r.side}`,time(r.openedMs),fmt(raw(r.entryRaw,r.asset==='ETH'?100:10))]),...intents.map(r=>[label(r.modelId),r.intentId,`${r.state} / ${r.side||'—'}`,time(r.createdMs),r.reason||r.kind])],'Открытых позиций и записанных заявок пока нет',5);
  const decisions=[...(h.modelDecisions||[]),...(h.decisions||[]).map(r=>({...r,model_id:PRIMARY,source_reason:r.baseline_reason}))].filter(r=>matches(r.model_id,models().find(m=>m.id===r.model_id)?.asset)).sort((a,b)=>b.second_ms-a.second_ms);
  table('comparison-decisions',decisions.map(r=>[`${label(r.model_id)} · ${time(r.second_ms)}`,`${r.strategy} / ${r.side}`,r.source_reason||'—',`${r.timing_status||''} ${r.timing_reason||''} · ${r.entry_reason||'без дополнительного блока'}`,detailButton(r)]),'Решения появятся после прогрева и подходящих сигналов',5);
  table('comparison-probes',(h.signalProbes||[]).filter(r=>matches(r.model_id,r.asset)).map(r=>[label(r.model_id)+' / '+r.asset,`${time(r.at_ms)} / ${r.side}`,r.entry_reason,[1,5,30].map(s=>{const v=r.observations.find(v=>v.seconds===s);return v?.status==='OBSERVED'?fmt(v.signedMoveBps):v?'UNKNOWN':'…';}).join(' / ')]),'Замеров после отказа в этой выборке пока нет',4);
  const blocks=(h.blockSummary||[]).filter(r=>matches(r.model_id,models().find(m=>m.id===r.model_id)?.asset)).slice(0,3);
  $('shadow-block-summary').replaceChildren(...blocks.map(r=>node('p',`${label(r.model_id)} · ${r.observations} наблюдений: ${r.reason}`)));
  for(const key of['trades','positions','decisions','probes'])$('shadow-tab-'+key).hidden=key!==tab;
  text('shadow-history-warning',h.warning||'Независимые SIM-счета. Модель очереди и результат не подтверждают прибыль кошелька.');
 }
 async function render(s,force=false){state=s;if(busy||(!force&&performance.now()-last<2000))return;busy=true;last=performance.now();try{const r=await fetch('/api/shadow/history?limit=40',{cache:'no-store',signal:AbortSignal.timeout(5000)});if(!r.ok)throw new Error('История пока недоступна');data=await r.json();if(data.mode!=='SIM')throw new Error('Ожидалась SIM история');draw();}catch(e){text('shadow-history-status','Нет свежей истории');text('shadow-history-process',e.message);}finally{busy=false;}}
 document.querySelectorAll('[data-shadow-asset]').forEach(b=>b.addEventListener('click',()=>{asset=b.dataset.shadowAsset;selected='ALL';document.querySelectorAll('[data-shadow-asset]').forEach(x=>x.setAttribute('aria-pressed',String(x===b)));draw();}));
 document.querySelectorAll('[data-shadow-tab]').forEach(b=>b.addEventListener('click',()=>{tab=b.dataset.shadowTab;document.querySelectorAll('[data-shadow-tab]').forEach(x=>x.setAttribute('aria-pressed',String(x===b)));draw();}));
 $('shadow-model-filter').addEventListener('change',e=>choose(e.target.value));
 $('shadow-add-toggle').addEventListener('click',()=>{const form=$('shadow-add-form');form.hidden=!form.hidden;$('shadow-add-toggle').setAttribute('aria-expanded',String(!form.hidden));if(!form.hidden)$('shadow-new-label').focus();});
 $('shadow-add-form').addEventListener('input',()=>{requestId=null;});
 $('shadow-add-form').addEventListener('submit',async e=>{e.preventDefault();if(adding)return;adding=true;$('shadow-add-submit').disabled=true;requestId??=crypto.randomUUID();text('shadow-add-message','Подключаем стратегию к общему потоку…');try{const response=await fetch('/api/shadow/strategies',{method:'POST',headers:{'Content-Type':'application/json','X-Vchera2-Control':'1'},body:JSON.stringify({requestId,template:$('shadow-template').value,label:$('shadow-new-label').value,timing:$('shadow-timing').value,signals:$('shadow-signals').value}),signal:AbortSignal.timeout(12000)});const r=await response.json();if(!response.ok||!r.ok)throw new Error(r.message||'Добавление не подтверждено. Проверьте список стратегий.');text('shadow-add-message',r.message||'Стратегия уже добавлена');selected=r.model.id;asset='ALL';document.querySelectorAll('[data-shadow-asset]').forEach(x=>x.setAttribute('aria-pressed',String(x.dataset.shadowAsset==='ALL')));await render(state,true);}catch(error){text('shadow-add-message',error.message+' · Проверьте список перед повтором.');await render(state,true);}finally{adding=false;$('shadow-add-submit').disabled=false;}});
 window.ShadowHistoryPanel={render};
})();
