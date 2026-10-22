'use strict';
// Editable LIVE drafts only. No trade start/stop or scheduler device is invoked here.
(() => {
  const $=id=>document.getElementById(id);
  const TEXT={spreadEnabled:'Стратегия «Спред»',bookEnabled:'Стратегия «Лимитки»',bookUseHl:'Стакан Hyperliquid',bookUseBf:'Стакан Binance Futures',bookUseBs:'Стакан Binance Spot',bookExitOnFlip:'Выход при развороте стакана',cushBoth:'Подтверждение быстрой и медленной подушки',noLongs:'Только шорты (не открывать лонги)'};
  const KEYS=Object.keys(TEXT);let latest=null,revision=null,dirty=false,busy=false,messagePinned=false;
  const text=(id,value)=>{const n=$(id);if(n)n.textContent=value;};
  const val=(id,value)=>{const n=$(id);if(n)n.value=value==null?'':String(value);};
  const num=value=>value===''?null:Number(value);
  function note(message,bad=false){text('live-settings-message',message);if($('live-settings-message'))$('live-settings-message').className='instrument-message'+(bad?' bad':'');}
  function unsafe(s){const w=s?.provider?.wallet;return !!(s?.LIVE||s?.entriesEnabled||(s?.openManaged||[]).length||(w?.openPositions||[]).length||(w?.operations||[]).length||(s?.provider?.entrySafety?.unknown||[]).length);}
  function fill(settings){
    if(!settings)return;
    val('live-budget',settings.capital?.budgetUsdc);val('live-margin',settings.capital?.marginUsdc);val('live-leverage',settings.capital?.leverage);
    const schedule=settings.schedule||{};val('live-timezone',schedule.timezone||'Europe/Moscow');val('live-start',schedule.start||'09:00');val('live-stop',schedule.stop||'18:00');
    val('live-date-from',schedule.dateFrom);val('live-date-to',schedule.dateTo);$('live-window-requested').checked=!!schedule.enabled;
    for(let day=1;day<=7;day++)$('live-day-'+day).checked=(schedule.weekdays||[]).includes(day);
    for(const key of KEYS)$('live-feature-'+key).checked=!!settings.features?.[key];
  }
  function human(code){return({DRAFT_NOT_SAVED:'Черновик ещё не сохранён',CAPITAL_NOT_SELECTED:'Выберите бюджет, маржу и плечо',BTC_INSTRUMENT_LIMITS_UNKNOWN_OR_STALE:'Перед LIVE нужно подтвердить лимиты BTC',RISK_BUDGET_UNKNOWN_OR_STALE:'Денежный риск ещё не одобрен',RESERVE_UNKNOWN:'Нужно подтвердить резерв средств',CONTRACT_CAPABILITY_UNCONFIRMED:'Перед LIVE нужна свежая проверка контракта',LOCKED_RUNTIME_SAFETY_NOT_CONFIRMED:'Денежный запуск ещё не прошёл общую приёмку',WALLET_UNKNOWN_OR_STALE:'Нет свежего подтверждения состояния кошелька',STOP_FIRST_LIVE_OR_ENTRY_STATE:'Сначала остановите LIVE',UNKNOWN_ENTRY_LEDGER:'Есть заявка с неизвестным исходом',AVAILABLE_BELOW_BUDGET_PLUS_RESERVE:'Бюджет с резервом больше доступных средств',AVAILABLE_BELOW_MARGIN_PLUS_RESERVE:'На маржу с резервом недостаточно средств'})[code]||code;}
  function render(s){
    latest=s;const saved=s.liveSettings;
    if(!saved){$('save-live-settings').disabled=true;note('Настройки LIVE ещё не подключены к backend. Торговля не включается.');return;}
    const settings=saved.settings||saved.draft;
    if(!dirty&&!busy){revision=saved.revision;fill(settings);}
    text('live-draft-revision','Черновик v'+(saved.revision??'—')+' · не активирован');
    const available=s.provider?.wallet?.available;
    text('live-real-funds',typeof available==='number'?available.toLocaleString('ru-RU',{maximumFractionDigits:6})+' USDC':'Нет достоверного баланса');
    text('live-capital-note','Фактический баланс кошелька не редактируется. Бюджет — лимит средств для бота, не пополнение.');
    $('save-live-settings').disabled=busy||unsafe(s)||!settings||!!saved.storage?.error;
    const errors=saved.readiness?.blockers||saved.validation?.reasons||saved.blockers||saved.reasons||[];
    if(!dirty&&!busy&&!messagePinned)note(errors.length?'Перед денежным стартом: '+errors.map(e=>human(typeof e==='string'?e:e.message||e.code||'Настройки не готовы')).join(' · '):'Сохранение не включает LIVE. Для денежного запуска и scheduler нужна отдельная авторизация.',!!saved.storage?.error);
    text('live-schedule-status','Автостарт НЕ активирован. Окно — сохранённый план для scheduler текущей сессии.');
    preview();
  }
  function preview(){
    const margin=num($('live-margin').value),leverage=num($('live-leverage').value),nominal=margin*leverage;
    text('live-notional-preview',margin>0&&leverage>0&&Number.isFinite(nominal)?'Номинал: $'+nominal.toLocaleString('ru-RU',{maximumFractionDigits:2}):'Номинал = маржа × плечо');
  }
  function body(){
    const current=latest?.liveSettings?.settings||latest?.liveSettings?.draft||{};
    return{expectedRevision:revision,settings:{...current,
      capital:{budgetUsdc:$('live-budget').value||null,marginUsdc:$('live-margin').value||null,leverage:num($('live-leverage').value)},
      schedule:{...current.schedule,enabled:$('live-window-requested').checked,kind:'daily',timezone:$('live-timezone').value,start:$('live-start').value,stop:$('live-stop').value,weekdays:[1,2,3,4,5,6,7].filter(day=>$('live-day-'+day).checked),dateFrom:$('live-date-from').value||null,dateTo:$('live-date-to').value||null},
      features:Object.fromEntries(KEYS.map(key=>[key,$('live-feature-'+key).checked]))}};
  }
  async function save(){
    if(busy||!latest||unsafe(latest))return;
    busy=true;messagePinned=true;$('save-live-settings').disabled=true;note('Сохраняем LIVE черновик без включения торговли…');
    try{
      const response=await fetch('/api/live/settings',{method:'POST',headers:{'Content-Type':'application/json','X-Vchera2-Control':'1'},body:JSON.stringify(body()),signal:AbortSignal.timeout(5000)});
      const result=await response.json();
      if(!response.ok||result.ok!==true)throw new Error((result.reasons||[]).map(e=>typeof e==='string'?e:e.message||e.code).join(' ')||result.message||'Черновик не сохранён');
      dirty=false;note('Черновик LIVE сохранён. Реальные ордера и автостарт НЕ разрешены; ETH только наблюдать.');
    }catch(error){note(error.message||'Нет подтверждения сохранения',true);}finally{busy=false;}
  }
  const ids=['live-budget','live-margin','live-leverage','live-timezone','live-start','live-stop','live-date-from','live-date-to','live-window-requested',...[1,2,3,4,5,6,7].map(day=>'live-day-'+day),...KEYS.map(key=>'live-feature-'+key)];
  for(const id of ids)$(id).addEventListener('change',()=>{dirty=true;messagePinned=false;note('Изменения не сохранены. LIVE не включается.');preview();});
  $('save-live-settings').addEventListener('click',save);
  $('reload-live-settings').addEventListener('click',()=>{dirty=false;messagePinned=false;if(latest)render(latest);});
  window.LiveSettingsPanel={render};
})();
