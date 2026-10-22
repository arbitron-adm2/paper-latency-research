// Плавающее окно «Очередь papertrade» на вкладке биржи. Данные берёт у фона расширения (порт 'ptq'),
// сам в сеть не ходит. Теневой DOM — чтобы стили сайта не ломали окно и наоборот.
(() => {
  'use strict';
  if (window.top !== window || document.getElementById('ptq-host')) return;
  const C = globalThis.PTQ;

  const LIM = { minW: 230, maxW: 640, minH: 90, maxH: 900, gap: 8 };
  const STATE_RU = { clear: 'Свободно', building: 'Набирается', backed: 'Затор' };
  let cfg = { visible: true, collapsed: false, market: 'BTC', margin: 300, lev: 500, wallet: '', x: null, y: null, w: 290, h: null, sound: false, chartSpot: true, chartFut: true, chartAlign: true, guardQueue: true, maxAhead: 30, maxWait: 6 };
  let last = null, lastMsgAt = 0, port = null, prevState = null;
  const trackRoom = C.roomTracker(300000);

  // ---------- разметка ----------
  const host = document.createElement('div');
  host.id = 'ptq-host';
  host.style.cssText = 'all:initial;position:fixed;z-index:2147483646;left:0;top:0;width:0;height:0;';
  const root = host.attachShadow({ mode: 'open' });
  root.innerHTML = `<style>${CSS()}</style>
<div class="ptq" role="region" aria-label="Очередь papertrade" hidden>
  <header class="bar" tabindex="0" title="Перетащить окно (или стрелки на клавиатуре)">
    <span class="chip" data-s="off"><i></i><span class="chip-t">Подключаюсь…</span></span>
    <div class="seg" role="group" aria-label="Рынок">
      <button type="button" data-m="BTC" aria-pressed="true">BTC</button><button type="button" data-m="ETH" aria-pressed="false">ETH</button>
    </div>
    <span class="sp"></span>
    <button type="button" class="ib" data-act="settings" aria-label="Настройки" title="Настройки">⚙</button>
    <button type="button" class="ib" data-act="collapse" aria-label="Свернуть" title="Свернуть / развернуть">–</button>
    <button type="button" class="ib" data-act="hide" aria-label="Спрятать окно" title="Спрятать (Alt+Q или кнопка расширения). Тики Binance на графике остаются.">×</button>
  </header>
  <div class="mini" aria-live="polite"></div>
  <div class="body">
    <section class="tb" aria-label="Быстрая торговля">
      <div class="r1"><span class="dv" title="Отклонение спреда (HL − Binance futures) от фона за 2 мин, в bp. + → HL дороже → шорт; − → лонг.">Δ —</span><span class="du sub"></span><span class="sp"></span><span class="sp2 sub" title="Сырой спред HL − Binance futures / spot, bp"></span></div>
      <div class="rec" data-r="none" title="">нет данных</div>
      <div class="btns">
        <button type="button" class="tbn lg" data-t="L" title="Лонг по пресету (Alt+L)">ЛОНГ</button>
        <button type="button" class="tbn sh" data-t="S" title="Шорт по пресету (Alt+S)">ШОРТ</button>
        <button type="button" class="tbn cl" data-t="X" title="Закрыть все позиции (Alt+X)">ЗАКРЫТЬ ВСЕ</button>
      </div>
      <div class="r4"><label title="Маржа пресета, $ (одна для лонга и шорта)">$<input type="number" min="1" step="1" data-k="margin"></label><label title="Плечо пресета">×<input type="number" min="1" max="1000" step="1" data-k="lev"></label><button type="button" class="arm" data-arm="off" title="Взвод: 🔒 выкл → 🧪 проба (ставит форму, «Place Trade» НЕ жмёт) → ⚡ бой (жмёт). Сам через 10 мин выключается.">🔒 выкл</button><span class="tst sub"></span></div>
    </section>
    <section class="q" title="Очередь общая для BTC и ETH. «ждут» — ещё у платформы; «в пути» — отправлены в блокчейн, ждут блока.">
      <div class="qtop">
        <span class="wait">—</span>
        <div class="qinfo">
          <div class="l1"><b class="ahead">—</b> <span class="sub split"></span></div>
          <div class="l2 sub">затор <b class="backlog">—</b><span class="trend"></span> · <b class="rate">—</b><span class="front"></span></div>
        </div>
      </div>
      <div class="blocks" aria-hidden="true" title="Последние блоки HyperEVM: оранжевым — газ papertrade, серым — остальное; высота = заполнение блока"></div>
    </section>
    <section class="oi" title="Полоска — сколько свободного места осталось от наибольшего за последние 5 минут. Потолок OI плавающий: место выбирают, потом оно снова набегает.">
      <div class="oi-h"><h3>Место OI · <span class="mk">BTC</span></h3><span class="oi-age sub"></span></div>
      <div class="side" data-side="long"><span class="lbl lg">Л</span><span class="bar2"><i></i></span><span class="room">—</span><span class="fits sub"></span><span class="refill sub"></span></div>
      <div class="side" data-side="short"><span class="lbl sh">Ш</span><span class="bar2"><i></i></span><span class="room">—</span><span class="fits sub"></span><span class="refill sub"></span></div>
      <div class="size-warn sub"></div>
    </section>
    <section class="my" hidden>
      <h3>Мои заявки</h3>
      <div class="my-t sub"></div>
    </section>
    <section class="set" hidden>
      <h3>Настройки</h3>
      <label>Маржа, $ <input type="number" min="1" step="1" data-k="margin"></label>
      <label>Плечо, × <input type="number" min="1" max="1000" step="1" data-k="lev"></label>
      <p class="sub size-t"></p>
      <label class="wide">Кошелёк (необязательно, только адрес) <input type="text" spellcheck="false" placeholder="0x…" data-k="wallet"></label>
      <label class="chk"><input type="checkbox" data-k="chartSpot"> Тики Binance spot на графике</label>
      <label class="chk"><input type="checkbox" data-k="chartFut"> Тики Binance futures на графике</label>
      <label class="chk" title="Лонг/Шорт блокируются, пока очередь больше порога или данных об очереди нет. «Закрыть всё» не блокируется."><input type="checkbox" data-k="guardQueue"> Guard: не открывать при большой очереди</label>
      <label>Макс. заявок в очереди <input type="number" min="1" step="1" data-k="maxAhead"></label>
      <label>Макс. ожидание, с <input type="number" min="1" step="1" data-k="maxWait"></label>
      <label class="chk" title="Сдвигает линии Binance на среднюю разницу с ценой Paper, чтобы видеть форму и опережение, а не разницу уровней"><input type="checkbox" data-k="chartAlign"> Совмещать уровень Binance с Paper (≈)</label>
      <label class="chk"><input type="checkbox" data-k="sound"> Звук, когда очередь освободилась</label>
      <p class="note">Только чтение: окно ничего не подписывает и не отправляет. Адрес нужен, чтобы показать место ваших заявок в очереди.</p>
    </section>
  </div>
  <footer><span class="fresh"><i></i><span class="fresh-t">нет данных</span></span><span class="bn" title="Binance: spot / futures"><i data-v="bs"></i><i data-v="bf"></i></span><span class="src sub"></span></footer>
  <div class="rz" role="separator" aria-label="Изменить размер" title="Тянуть — изменить размер"></div>
</div>`;
  const $ = (s) => root.querySelector(s);
  const $$ = (s) => [...root.querySelectorAll(s)];
  const panel = $('.ptq');
  const blocksEl = $('.blocks');
  for (let i = 0; i < 24; i++) { const b = document.createElement('span'); b.innerHTML = '<i class="o"></i><i class="p"></i>'; blocksEl.appendChild(b); }
  (document.documentElement || document.body).appendChild(host);

  // ---------- настройки и положение ----------
  const save = (() => { let t = null, pend = {}; return (o) => { Object.assign(pend, o); clearTimeout(t); t = setTimeout(() => { try { chrome.storage.local.set(pend); } catch {} pend = {}; }, 250); }; })();
  function clampBox() {
    const vw = window.innerWidth, vh = window.innerHeight;
    let w = Math.min(LIM.maxW, Math.max(LIM.minW, Number(cfg.w) || 290));
    w = Math.min(w, Math.max(LIM.minW, vw - LIM.gap * 2));
    let h = cfg.h == null ? null : Math.min(LIM.maxH, Math.max(LIM.minH, Number(cfg.h)), Math.max(LIM.minH, vh - LIM.gap * 2));
    // по умолчанию слева над графиком: справа у биржи форма заявки, её не закрываем
    let x = cfg.x == null ? 16 : Number(cfg.x);
    let y = cfg.y == null ? 110 : Number(cfg.y);
    const ph = panel.offsetHeight || 120;
    x = Math.min(Math.max(LIM.gap, x), Math.max(LIM.gap, vw - w - LIM.gap));
    y = Math.min(Math.max(LIM.gap, y), Math.max(LIM.gap, vh - Math.min(ph, 60) - LIM.gap));
    return { x, y, w, h };
  }
  function place() {
    const b = clampBox();
    panel.style.left = b.x + 'px';
    panel.style.top = b.y + 'px';
    panel.style.width = b.w + 'px';
    panel.style.height = cfg.collapsed || b.h == null ? '' : b.h + 'px';
    panel.style.maxHeight = (window.innerHeight - b.y - LIM.gap) + 'px';
  }
  function applyCfg() {
    panel.hidden = !cfg.visible;
    panel.classList.toggle('collapsed', !!cfg.collapsed);
    $('[data-act="collapse"]').textContent = cfg.collapsed ? '+' : '–';
    $('[data-act="collapse"]').setAttribute('aria-label', cfg.collapsed ? 'Развернуть' : 'Свернуть');
    for (const b of $$('.seg button')) b.setAttribute('aria-pressed', String(b.dataset.m === cfg.market));
    for (const el of $$('.mk')) el.textContent = cfg.market;
    for (const inp of $$('[data-k]')) {
      if (root.activeElement === inp) continue;
      if (inp.type === 'checkbox') inp.checked = !!cfg[inp.dataset.k]; else inp.value = cfg[inp.dataset.k] ?? '';
    }
    $('.size-t').textContent = `Размер позиции: ${C.money(mySize())}`;
    place();
    syncPort();
    render();
  }
  const mySize = () => { const m = Number(cfg.margin), l = Number(cfg.lev); return m > 0 && l > 0 ? m * l : NaN; };

  try {
    chrome.storage.local.get(null, (s) => { cfg = { ...cfg, ...(s || {}) }; applyCfg(); });
    chrome.storage.onChanged.addListener((ch, area) => {
      if (area !== 'local') return;
      for (const [k, v] of Object.entries(ch)) cfg[k] = v.newValue;
      applyCfg();
    });
  } catch { applyCfg(); }

  // ---------- перетаскивание ----------
  const bar = $('.bar');
  bar.addEventListener('pointerdown', (e) => {
    if (e.button !== 0 || e.target.closest('button')) return;
    const r = panel.getBoundingClientRect();
    const dx = e.clientX - r.left, dy = e.clientY - r.top;
    bar.setPointerCapture(e.pointerId);
    panel.classList.add('dragging');
    const move = (ev) => { cfg.x = ev.clientX - dx; cfg.y = ev.clientY - dy; place(); };
    const up = () => {
      bar.removeEventListener('pointermove', move); bar.removeEventListener('pointerup', up); bar.removeEventListener('pointercancel', up);
      panel.classList.remove('dragging');
      const b = clampBox(); cfg.x = b.x; cfg.y = b.y; save({ x: b.x, y: b.y });
    };
    bar.addEventListener('pointermove', move); bar.addEventListener('pointerup', up); bar.addEventListener('pointercancel', up);
    e.preventDefault();
  });
  bar.addEventListener('keydown', (e) => {
    const step = e.shiftKey ? 40 : 10;
    const d = { ArrowLeft: [-step, 0], ArrowRight: [step, 0], ArrowUp: [0, -step], ArrowDown: [0, step] }[e.key];
    if (!d || e.target !== bar) return;
    const b = clampBox(); cfg.x = b.x + d[0]; cfg.y = b.y + d[1]; place();
    const nb = clampBox(); save({ x: nb.x, y: nb.y }); e.preventDefault();
  });

  // ---------- изменение размера ----------
  const rz = $('.rz');
  rz.addEventListener('pointerdown', (e) => {
    if (e.button !== 0) return;
    const r = panel.getBoundingClientRect();
    const sx = e.clientX, sy = e.clientY, sw = r.width, sh = r.height;
    rz.setPointerCapture(e.pointerId);
    panel.classList.add('dragging');
    const move = (ev) => {
      cfg.w = Math.min(LIM.maxW, Math.max(LIM.minW, sw + ev.clientX - sx, 0));
      if (!cfg.collapsed) cfg.h = Math.min(LIM.maxH, Math.max(LIM.minH, sh + ev.clientY - sy));
      place();
    };
    const up = () => {
      rz.removeEventListener('pointermove', move); rz.removeEventListener('pointerup', up); rz.removeEventListener('pointercancel', up);
      panel.classList.remove('dragging');
      const b = clampBox(); save({ w: b.w, h: cfg.collapsed ? cfg.h : b.h });
    };
    rz.addEventListener('pointermove', move); rz.addEventListener('pointerup', up); rz.addEventListener('pointercancel', up);
    e.preventDefault(); e.stopPropagation();
  });
  rz.addEventListener('dblclick', () => { cfg.h = null; cfg.w = 290; place(); save({ w: 290, h: null }); });
  window.addEventListener('resize', place);

  // ---------- кнопки и настройки ----------
  root.addEventListener('click', (e) => {
    const b = e.target.closest('button');
    if (!b) return;
    if (b.dataset.m) { cfg.market = b.dataset.m; save({ market: cfg.market }); applyCfg(); }
    else if (b.dataset.act === 'collapse') { cfg.collapsed = !cfg.collapsed; save({ collapsed: cfg.collapsed }); applyCfg(); }
    else if (b.dataset.act === 'hide') { cfg.visible = false; save({ visible: false }); applyCfg(); }
    else if (b.dataset.act === 'settings') { const s = $('.set'); s.hidden = !s.hidden; if (cfg.collapsed && !s.hidden) { cfg.collapsed = false; save({ collapsed: false }); applyCfg(); } }
  });
  for (const inp of $$('[data-k]')) {
    inp.addEventListener(inp.type === 'checkbox' ? 'change' : 'input', () => {
      const k = inp.dataset.k;
      let v = inp.type === 'checkbox' ? inp.checked : inp.value;
      if (k === 'margin' || k === 'lev' || k === 'maxAhead' || k === 'maxWait') { v = Number(v); if (!(v > 0)) return; }
      if (k === 'wallet') v = String(v).trim();
      cfg[k] = v; save({ [k]: v });
      $('.size-t').textContent = `Размер позиции: ${C.money(mySize())}`;
      render();
    });
  }
  // Не даём сайту перехватывать клавиши, пока печатаем в окне.
  for (const t of ['keydown', 'keyup', 'keypress']) root.addEventListener(t, (e) => { if (e.target.tagName === 'INPUT') e.stopPropagation(); });

  // ---------- связь с фоном: только пока окно видно и вкладка активна ----------
  function syncPort() {
    // Окно видно или включены тики Binance на графике (они работают и при спрятанном окне). Ничего не торгует.
    const want = cfg.visible || cfg.chartSpot !== false || cfg.chartFut !== false;
    if (want && !port) {
      try {
        port = chrome.runtime.connect({ name: 'ptq' });
        port.onMessage.addListener((m) => {
          if (!m) return;
          if (m.type === 'ticks') globalThis.PTQChart?.feed(m);
          else if (m.type === 'state') { last = m; lastMsgAt = Date.now(); globalThis.PTQChart?.setMids(m.mids); scheduleRender(); }
        });
        port.onDisconnect.addListener(() => { port = null; setTimeout(syncPort, 1000); });
      } catch { port = null; } // расширение перезагрузили — эта вкладка устарела
    } else if (!want && port) { const p = port; port = null; try { p.disconnect(); } catch {} }
  }
  document.addEventListener('visibilitychange', syncPort);

  // ---------- отрисовка (не чаще 5 раз в секунду) ----------
  let rq = false, lastRender = 0;
  function scheduleRender() {
    if (rq) return; rq = true;
    setTimeout(() => { const paint = () => { rq = false; lastRender = performance.now(); render(); }; if (document.hidden) paint(); else requestAnimationFrame(paint); }, Math.max(0, 200 - (performance.now() - lastRender)));
  }
  setInterval(() => { if (cfg.visible && !document.hidden) scheduleRender(); }, 1000);

  // ---------- быстрая торговля ----------
  const sgn = (x, d = 2) => (Number.isFinite(x) ? (x >= 0 ? '+' : '−') + Math.abs(x).toFixed(d) : '—');
  let arm = 'off', armAt = 0, busy = false, tstUntil = 0;
  const ARM_TXT = { off: '🔒 выкл', dry: '🧪 проба', live: '⚡ бой' };
  const pageMarket = () => (globalThis.PTQTrader && PTQTrader.marketOnPage()) || cfg.market;
  function setArm(v) { arm = v; armAt = Date.now(); const b = $('.arm'); b.dataset.arm = v; b.textContent = ARM_TXT[v]; for (const x of $$('.tbn')) x.dataset.armed = v; }
  function tstMsg(text, bad) { const el = $('.tst'); el.textContent = text; el.dataset.bad = bad ? '1' : ''; tstUntil = Date.now() + 6000; }
  // Жёсткий guard очереди: причина запрета открытия или null. Нет свежих данных об очереди — тоже запрет.
  function queueBlock() {
    if (cfg.guardQueue === false) return null;
    const s = last, q = s && s.queue, link = lastMsgAt && Date.now() - lastMsgAt < 4000;
    if (!link || !q || !q.fresh) return 'нет свежих данных об очереди';
    if (q.stalled) return 'очередь стоит';
    if (q.ahead > Number(cfg.maxAhead)) return `очередь ${q.ahead} > ${cfg.maxAhead} заявок`;
    if (q.waitSec !== null && q.waitSec > Number(cfg.maxWait)) return `ожидание ≈ ${Math.round(q.waitSec)} с > ${cfg.maxWait} с`;
    if (q.waitSec === null) return 'ожидание очереди не оценено';
    return null;
  }
  async function act(kind) {
    if (busy) return;
    if (arm === 'off') { tstMsg('сначала взведите 🔒', true); return; }
    if (!globalThis.PTQTrader) { tstMsg('нет модуля торговли', true); return; }
    if (kind !== 'X') { const qb = queueBlock(); if (qb) { tstMsg('⛔ guard: ' + qb, true); return; } }
    busy = true;
    const label = kind === 'L' ? 'Лонг' : kind === 'S' ? 'Шорт' : 'Закрыть всё';
    try {
      const mk = pageMarket();
      const r = kind === 'X' ? await PTQTrader.closeAll({ mode: arm }) : await PTQTrader.open(kind, { margin: Number(cfg.margin), lev: Number(cfg.lev), mode: arm });
      tstMsg(r.ok ? `${r.dry ? '🧪 ' : '✓ '}${label}${kind === 'X' ? '' : ' ' + mk} · ${r.ms} мс` : `✗ ${label}: ${r.error}`, !r.ok);
    } catch (e) { tstMsg('✗ ' + label + ': ' + (e && e.message || e), true); }
    busy = false;
  }
  root.addEventListener('click', (e) => {
    const b = e.target.closest('button');
    if (!b) return;
    if (b.dataset.t) act(b.dataset.t);
    else if (b.dataset.arm !== undefined) setArm(arm === 'off' ? 'dry' : arm === 'dry' ? 'live' : 'off');
  });
  window.addEventListener('keydown', (e) => {
    if (!e.altKey || e.ctrlKey || e.metaKey || e.shiftKey || e.repeat) return;
    const t = e.target, tag = t && t.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || (t && t.isContentEditable)) return;
    const kind = { KeyL: 'L', KeyS: 'S', KeyX: 'X' }[e.code];
    if (!kind || !cfg.visible) return;
    e.preventDefault(); e.stopPropagation(); act(kind);
  }, true);
  setInterval(() => { if (arm !== 'off' && Date.now() - armAt > 600000) { setArm('off'); tstMsg('взвод снят по таймеру', false); } }, 5000);
  function renderTb(s, link) {
    const pm = pageMarket(), sp = link && s && s.spread ? s.spread[pm] : null;
    const dv = $('.dv'), rec = $('.rec');
    if (!sp || !sp.ready) { set(dv, 'Δ —'); dv.dataset.d = ''; set('.du', ''); set('.sp2', sp && Number.isFinite(sp.spreadBp) ? `спред ${sgn(sp.spreadBp)} bp` : ''); rec.dataset.r = 'none'; set(rec, sp ? sp.title : 'нет данных'); rec.title = ''; }
    else {
      set(dv, `Δ ${sgn(sp.devBp)} bp`); dv.dataset.d = sp.devBp > 0 ? 'up' : sp.devBp < 0 ? 'down' : '';
      set('.du', `$${sgn(sp.devUsd, 1)}`);
      set('.sp2', `F ${sgn(sp.spreadBp)}${Number.isFinite(sp.spotBp) ? ` · S ${sgn(sp.spotBp)}` : ''}`);
      rec.dataset.r = sp.rec; set(rec, sp.rec === 'long' ? '▲ ЛОНГ — правила проходят' : sp.rec === 'short' ? '▼ ШОРТ — правила проходят' : sp.title);
      rec.title = sp.rules.map((r) => (r.ok ? '✓ ' : '✗ ') + r.text).join('\n') + '\n' + sp.note;
    }
    for (const b of $$('.tbn')) b.dataset.rec = (sp && ((b.dataset.t === 'L' && sp.rec === 'long') || (b.dataset.t === 'S' && sp.rec === 'short'))) ? '1' : '';
    const qb = queueBlock();
    for (const b of $$('.tbn.lg,.tbn.sh')) { b.dataset.block = qb ? '1' : ''; b.title = (b.dataset.t === 'L' ? 'Лонг по пресету (Alt+L)' : 'Шорт по пресету (Alt+S)') + (qb ? '\n⛔ guard: ' + qb : ''); }
    if (Date.now() > tstUntil) set('.tst', qb && arm !== 'off' ? '⛔ ' + qb : '');
  }
  const set = (sel, text) => { const el = typeof sel === 'string' ? $(sel) : sel; if (el && el.textContent !== text) el.textContent = text; };
  function render() {
    if (!cfg.visible) return;
    const s = last;
    const linkAge = lastMsgAt ? Date.now() - lastMsgAt : Infinity;
    const q = s && s.queue;
    const link = linkAge < 4000;
    const qFresh = !!(q && q.fresh && link);
    globalThis.PTQLatencyPanel?.render(link ? s : null);

    // Состояние очереди
    const chip = $('.chip');
    let st = 'off', stText = 'Подключаюсь…';
    if (!link && lastMsgAt) { st = 'off'; stText = 'Нет связи с фоном'; }
    else if (q && !qFresh) { st = 'off'; stText = q.platformStale ? 'Платформа: данные устарели' : `Нет обновлений ${Math.round(q.ageMs / 1000)} с`; }
    else if (q) { st = q.state; stText = STATE_RU[q.state]; }
    else if (s && s.conn && s.conn.papertrade === 'error' && s.source !== 'paperwatch') { st = 'off'; stText = 'Поток не пускает'; }
    chip.dataset.s = st; set('.chip-t', stText); chip.title = stText;
    panel.classList.toggle('stale', !qFresh);

    if (q) {
      set('.wait', C.waitText(q));
      set('.ahead', q.ahead.toLocaleString('ru-RU') + ' ' + C.plural(q.ahead, 'заявка', 'заявки', 'заявок'));
      set('.split', `(${q.queued} ждут · ${q.submitted} в пути)`);
      set('.backlog', `${q.backlog.toFixed(1)} бл`); $('.backlog').title = 'Затор: газ в очереди и в пути, в блоках HyperEVM';
      const tr = $('.trend');
      if (q.trend === null) { set(tr, ''); tr.className = 'trend'; }
      else { set(tr, ` ${q.trend >= 0 ? '▲' : '▼'}${Math.abs(q.trend).toFixed(1)}`); tr.className = 'trend ' + (q.trend >= 0.1 ? 'up' : q.trend <= -0.1 ? 'down' : ''); tr.title = 'Изменение затора за 15 с, в блоках'; }
      set('.rate', q.intentsPerSec === null ? 'считаю…' : `≈${q.intentsPerSec.toFixed(q.intentsPerSec < 10 ? 1 : 0)}/с${q.gasPerIntentMeasured ? '' : '*'}`);
      $('.rate').title = 'Проходит сейчас, заявок в секунду. Сколько заявок papertrade реально попадает в блоки за секунду (по последним 20 блокам). Это не предел: когда очередь короткая, проходит меньше. '
        + (q.gasPerIntentMeasured ? `Газ на заявку ${Math.round(q.gasPerIntent / 1000)} тыс. — по живой очереди.` : '* Газ на заявку взят по умолчанию 310 тыс., пока очередь пуста.');
      set('.front', q.frontWaitSec !== null ? ` · 1-я ${Math.round(q.frontWaitSec)} с` : ''); $('.front').title = 'Самая старая заявка в очереди ждёт';
      const bs = q.blocks.slice(-24);
      [...blocksEl.children].forEach((el, i) => {
        const b = bs[i - (24 - bs.length)];
        const o = el.firstChild, p = el.lastChild;
        if (!b) { o.style.height = '0'; p.style.height = '0'; el.title = ''; return; }
        o.style.height = Math.min(100, ((b.gas - b.pt) / b.limit) * 100) + '%';
        p.style.height = Math.min(100, (b.pt / b.limit) * 100) + '%';
        el.classList.toggle('big', b.big);
        el.title = `блок ${b.n}: papertrade ${Math.round((b.pt / b.limit) * 100)}%, всего ${Math.round((b.gas / b.limit) * 100)}%`;
      });
    } else {
      set('.wait', '—'); set('.ahead', '—'); set('.split', ''); set('.backlog', '—'); set('.trend', ''); set('.rate', '—'); set('.front', '');
    }

    renderTb(s, link);

    // Место OI по выбранному рынку
    const t = s && s.trading;
    const market = t && t.markets ? t.markets[cfg.market] : null;
    const mid = s && s.mids ? s.mids[cfg.market] : null;
    const midOk = s && s.mids && s.mids.ageMs !== null && s.mids.ageMs < 15000;
    const oi = market && midOk ? C.oiSummary(market, mid, mySize(), t.minOpenNotional) : null;
    $('.oi').classList.toggle('stale', !oi || !link);
    set('.oi-age', !t ? '' : t.paused ? 'торговля на паузе!' : !midOk ? 'нет цены' : '');
    for (const side of ['long', 'short']) {
      const box = $(`.side[data-side="${side}"]`);
      const d = oi && oi[side];
      const room = box.querySelector('.room'), fits = box.querySelector('.fits'), rf = box.querySelector('.refill'), bar = box.querySelector('.bar2 i');
      if (!d) { set(room, '—'); set(fits, ''); set(rf, ''); room.title = market ? 'жду цену' : 'нет данных'; bar.style.width = '0'; continue; }
      set(room, d.full ? 'ПОЛНО' : C.money(d.room));
      box.classList.toggle('full', d.full); box.classList.toggle('tight', !d.full && d.tight);
      const left = trackRoom(`${cfg.market}:${side}`, d.room, Date.now());
      bar.style.width = ((left ?? 0) * 100).toFixed(1) + '%';
      set(fits, d.fits === null || d.full ? '' : d.fits === 0 ? '<1' : `×${d.fits}`);
      fits.title = d.fits === null ? '' : d.full ? 'не войти' : d.fits === 0 ? `Свободного места меньше одной позиции ${C.money(d.perPos)}` : `Влезет ${d.fits} поз. по ${C.money(d.perPos)}`;
      room.title = `${d.full ? 'Места нет: ' : 'Свободно '}${C.money(d.room)} · открыто ${C.money(d.oi)} из потолка ${C.money(d.cap)} (${(d.usedPct ?? 0).toFixed(1)}%)`;
      // Пополнение места: биржа раз в ~15 с поднимает потолок до «открыто + запас»
      const cs = s.caps && s.caps[`${cfg.market}:${side}`];
      let rt = '', rtTitle = '';
      if (d.over > 0) { rt = '⛔'; rtTitle = `Закрыто: потолок ниже открытого на ${C.money(d.over)} — войти можно, когда столько закроют`; }
      else if (cs && cs.nextInSec !== null) { rt = `↻${Math.max(0, Math.round(cs.nextInSec))}с`; rtTitle = `Пополнение места через ${Math.max(0, Math.round(cs.nextInSec))} с${cs.headroom ? `, +${C.money(cs.headroom)}` : ''}${cs.periodSec ? ` (раз в ${Math.round(cs.periodSec)} с)` : ''}`; }
      else if (cs && cs.headroom) { rt = `↻+${C.money(cs.headroom)}`; rtTitle = `Пополняют на ~${C.money(cs.headroom)}`; }
      else { rt = '↻…'; rtTitle = 'Пополнение: замеряю…'; }
      set(rf, rt); rf.title = rtTitle;
      rf.classList.toggle('bad', d.over > 0);
    }
    const sw = $('.size-warn');
    set(sw, oi && oi.sizeOverMax ? `Размер > потолка позиции ${C.money(oi.maxPos)}: примут кусками` : '');
    sw.style.color = 'var(--warn)';

    // Мои заявки
    const my = s && s.my;
    const hasWallet = /^0x[0-9a-fA-F]{40}$/.test(String(cfg.wallet || '').trim());
    $('.my').hidden = !hasWallet;
    if (hasWallet) {
      let txt = 'жду данные…';
      if (my && my.error) txt = 'не удалось: ' + my.error;
      else if (my && Array.isArray(my.entries)) txt = my.entries.length ? my.entries.map((e) => `№${e.position}`).join(', ') + ` из ${q ? q.ahead : '?'} — место в очереди` : 'в очереди нет';
      set('.my-t', txt);
    }

    // Свёрнутый вид — одна строка
    const ro = oi ? `Л ${oi.long.full ? 'полно' : C.money(oi.long.room)} · Ш ${oi.short.full ? 'полно' : C.money(oi.short.room)}` : '';
    set('.mini', q ? `${q.ahead} в очереди · ${C.waitText(q)}${ro ? ' · ' + ro : ''}` : 'нет данных');

    // Подвал: свежесть и источник
    const fr = $('.fresh');
    fr.dataset.ok = String(qFresh);
    set('.fresh-t', !link ? (lastMsgAt ? `нет связи ${Math.round(linkAge / 1000)} с` : 'подключаюсь…') : q ? (q.ageMs < 1000 ? 'свежие' : (q.ageMs / 1000).toFixed(1) + ' с назад') : 'жду данные…');
    set('.src', s ? (s.source === 'direct' ? 'биржа' : s.source === 'paperwatch' ? 'paperwatch' : s.lastError ? 'ошибка: ' + s.lastError.slice(0, 40) : 'ищу…') : '');
    $('.src').title = s && s.source === 'paperwatch'
      ? 'Поток биржи сейчас не отвечает — данные идут через paperwatch.app (запасной источник).'
      : s && s.lastError ? s.lastError : 'Поток stream.papertrade.xyz напрямую';
    for (const dot of $$('.bn i')) { const st = s && s.conn ? s.conn[dot.dataset.v] : 'off'; dot.dataset.s = st; dot.title = `Binance ${dot.dataset.v === 'bs' ? 'spot' : 'futures'}: ${st}`; }

    // Звук: очередь освободилась
    if (cfg.sound && q && qFresh) {
      if (prevState && prevState !== 'clear' && q.state === 'clear') beep();
      prevState = q.state;
    } else if (q) prevState = q.state;
  }

  let actx = null;
  function beep() {
    try {
      actx = actx || new AudioContext();
      const o = actx.createOscillator(), g = actx.createGain();
      o.frequency.value = 880; g.gain.value = 0.08;
      o.connect(g); g.connect(actx.destination);
      o.start(); o.stop(actx.currentTime + 0.15);
    } catch {}
  }

  function CSS() {
    return `
:host{all:initial}
.ptq{container-type:inline-size;--bg:#000;--bg2:#0d0f12;--line:#23272e;--tx:#e8eaed;--sub:#8b929c;--acc:#f08a3c;--ok:#3fbf6f;--warn:#e5b53a;--bad:#ef5a4f;--lg:#3fbf6f;--sh:#ef5a4f;
  position:fixed;box-sizing:border-box;display:flex;flex-direction:column;min-width:${LIM.minW}px;min-height:36px;
  background:var(--bg);color:var(--tx);border:1px solid var(--line);border-radius:9px;box-shadow:0 8px 28px rgba(0,0,0,.55);
  font:12px/1.3 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;font-variant-numeric:tabular-nums;overflow:hidden;user-select:none}
.ptq[hidden]{display:none}
.ptq *{box-sizing:border-box}
.ptq.dragging{opacity:.92;cursor:grabbing}
.bar{display:flex;align-items:center;gap:5px;padding:4px 4px 4px 8px;background:var(--bg2);border-bottom:1px solid var(--line);cursor:grab;touch-action:none;flex:none}
.bar:focus-visible{outline:2px solid var(--acc);outline-offset:-2px}
.sp{flex:1}
.chip{display:inline-flex;align-items:center;gap:5px;font-weight:600;white-space:nowrap;min-width:0;overflow:hidden;text-overflow:ellipsis}
.chip i{width:8px;height:8px;border-radius:50%;background:var(--sub);flex:none}
.chip[data-s=clear] i{background:var(--ok)} .chip[data-s=building] i{background:var(--warn)} .chip[data-s=backed] i{background:var(--bad)}
.chip[data-s=clear]{color:var(--ok)} .chip[data-s=building]{color:var(--warn)} .chip[data-s=backed]{color:var(--bad)} .chip[data-s=off]{color:var(--sub)}
.seg{display:inline-flex;border:1px solid var(--line);border-radius:6px;overflow:hidden;flex:none}
.seg button{all:unset;cursor:pointer;padding:1px 7px;font-weight:600;font-size:11.5px;color:var(--sub)}
.seg button[aria-pressed=true]{background:var(--acc);color:#000}
.seg button:focus-visible,.ib:focus-visible{outline:2px solid var(--acc)}
.ib{all:unset;cursor:pointer;width:20px;height:20px;display:inline-flex;align-items:center;justify-content:center;border-radius:5px;color:var(--sub);font-size:13px;flex:none}
.ib:hover{background:#1b1f25;color:var(--tx)}
.mini{display:none;padding:5px 10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.collapsed .mini{display:block}
.collapsed .body,.collapsed footer,.collapsed .rz{display:none}
.body{flex:1;overflow:auto;padding:2px 8px;scrollbar-width:thin}
section{padding:5px 0;border-bottom:1px solid var(--line)}
section:last-child{border-bottom:0}
h3{margin:0;font-size:10.5px;font-weight:600;color:var(--sub);text-transform:uppercase;letter-spacing:.04em}
.qtop{display:flex;align-items:center;gap:10px}
.wait{font-size:22px;font-weight:700;letter-spacing:-.01em;white-space:nowrap;min-width:64px}
.qinfo{min-width:0;flex:1;line-height:1.3}
.qinfo>div{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sub{color:var(--sub);font-size:11px}
.trend{font-size:11px;color:var(--sub)} .trend.up{color:var(--bad)} .trend.down{color:var(--ok)}
.blocks{display:flex;align-items:flex-end;gap:2px;height:16px;margin-top:4px}
.blocks span{flex:1;height:100%;display:flex;flex-direction:column-reverse;background:#0b0d10;border-radius:1px;overflow:hidden}
.blocks .p{display:block;background:var(--acc)} .blocks .o{display:block;background:#3a4049}
.blocks span.big .p{background:#b07cff}
.note{margin:3px 0 0;color:var(--sub);font-size:10.5px}
.note.warn{color:var(--warn)}
.oi-h{display:flex;justify-content:space-between;gap:8px;margin-bottom:2px}
.side{display:grid;grid-template-columns:12px minmax(24px,1fr) auto auto auto;align-items:center;gap:6px;height:18px;white-space:nowrap}
.lbl{font-weight:700} .lbl.lg{color:var(--lg)} .lbl.sh{color:var(--sh)}
.room{font-weight:600;text-align:right;min-width:52px}
.fits{min-width:20px;text-align:right}
.refill{min-width:34px;text-align:right}
.refill.bad{color:var(--bad)}
.side.full .room{color:var(--bad)} .side.tight .room{color:var(--warn)}
.bar2{height:5px;background:#1a1e24;border-radius:3px;overflow:hidden}
.bar2 i{display:block;height:100%;width:0;background:var(--ok);transition:width .3s}
.side.tight .bar2 i{background:var(--warn)} .side.full .bar2 i{background:var(--bad)}
.set label{display:flex;justify-content:space-between;align-items:center;gap:8px;margin:3px 0;color:var(--sub)}
.set label.wide{flex-direction:column;align-items:stretch}
.set label.chk{justify-content:flex-start}
.set input[type=number],.set input[type=text]{all:unset;box-sizing:border-box;background:#0d0f12;border:1px solid var(--line);border-radius:5px;padding:2px 6px;color:var(--tx);width:100px;user-select:text;font:inherit}
.set input[type=text]{width:100%;font-family:ui-monospace,Consolas,monospace;font-size:11px}
.set input:focus{border-color:var(--acc)}
.set input[type=checkbox]{accent-color:var(--acc)}
.set h3{margin-top:6px}
footer{display:flex;align-items:center;gap:8px;padding:3px 8px 4px;border-top:1px solid var(--line);background:var(--bg2);white-space:nowrap;flex:none}
.fresh{display:inline-flex;align-items:center;gap:5px;font-size:11px;color:var(--sub)}
.fresh i{width:6px;height:6px;border-radius:50%;background:var(--bad)}
.fresh[data-ok=true] i{background:var(--ok)}
.bn{display:inline-flex;gap:3px;margin-left:auto}
.bn i{width:6px;height:6px;border-radius:50%;background:var(--sub)}
.bn i[data-s=open]{background:var(--ok)} .bn i[data-s=error]{background:var(--bad)} .bn i[data-s=connecting]{background:var(--warn)}
.src{overflow:hidden;text-overflow:ellipsis}
.stale .q .wait,.stale .q .qinfo,.oi.stale .side{opacity:.45}
.tb{padding:4px 0 5px}
.tb .r1{display:flex;align-items:baseline;gap:6px;height:20px;white-space:nowrap;overflow:hidden}
.dv{font-size:15px;font-weight:700;min-width:92px}
.dv[data-d=up]{color:var(--sh)} .dv[data-d=down]{color:var(--lg)}
.rec{height:20px;line-height:20px;padding:0 7px;border-radius:5px;font-weight:700;font-size:11.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;background:#1a1e24;color:var(--sub)}
.rec[data-r=long]{background:#123a22;color:#5fe08f} .rec[data-r=short]{background:#431a18;color:#ff8d84} .rec[data-r=wait]{background:#3a2f10;color:var(--warn)}
.btns{display:grid;grid-template-columns:1fr 1fr 1.15fr;gap:5px;margin:5px 0;height:32px}
.tbn{all:unset;box-sizing:border-box;cursor:pointer;text-align:center;font-weight:800;font-size:12px;border-radius:6px;line-height:32px;height:32px;user-select:none;white-space:nowrap}
.tbn.lg{background:#14532d;color:#d7ffe6} .tbn.sh{background:#6b1f1b;color:#ffe0dc} .tbn.cl{background:#2a2f38;color:#e8eaed}
.tbn[data-block="1"]{opacity:.3;cursor:not-allowed;background-image:repeating-linear-gradient(135deg,transparent 0 6px,rgba(0,0,0,.35) 6px 8px)}
.tbn[data-armed=off]{opacity:.45} .tbn:active{filter:brightness(1.35)} .tbn[data-rec="1"]{box-shadow:inset 0 0 0 2px var(--acc)}
.tbn:focus-visible,.arm:focus-visible{outline:2px solid var(--acc)}
.r4{display:flex;align-items:center;gap:5px;height:24px;overflow:hidden}
.r4 label{display:flex;align-items:center;gap:2px;color:var(--sub);flex:none}
.r4 input{all:unset;box-sizing:border-box;background:#0d0f12;border:1px solid var(--line);border-radius:5px;padding:2px 4px;color:var(--tx);width:62px;height:22px;user-select:text;font:inherit;text-align:right}
.r4 input::-webkit-inner-spin-button,.r4 input::-webkit-outer-spin-button{-webkit-appearance:none;margin:0}
.r4 label:nth-child(2) input{width:48px}
.r4 input:focus{border-color:var(--acc)}
.arm{all:unset;cursor:pointer;box-sizing:border-box;flex:none;height:22px;line-height:22px;padding:0 7px;border-radius:5px;background:#1b1f25;color:var(--sub);font-weight:700;font-size:11px;white-space:nowrap}
.arm[data-arm=dry]{background:#3a2f10;color:var(--warn)} .arm[data-arm=live]{background:#7a1d16;color:#fff}
.tst{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap} .tst[data-bad="1"]{color:var(--bad)}
.size-warn{min-height:14px;font-size:10.5px}
@container (max-width: 255px){ .chip-t{display:none} .split{display:none} }
.rz{position:absolute;right:0;bottom:0;width:14px;height:14px;cursor:nwse-resize;touch-action:none;
  background:linear-gradient(135deg,transparent 50%,var(--sub) 50%,var(--sub) 58%,transparent 58%,transparent 70%,var(--sub) 70%,var(--sub) 78%,transparent 78%)}
`;
  }
})();
