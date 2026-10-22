'use strict';
// Реалтайм-панель: /state опрашивается цепочкой без перекрытия (POLL_MS), а возрасты,
// таймеры и график между снимками досчитываются локально каждые TICK_MS.
// Серверный процесс не меняется: он торгует, его перезапуск ради панели не нужен.
const POLL_MS = 500, TICK_MS = 100, STALE_MS = 3000, TIMEOUT_MS = 2500, CHART_WINDOW_MS = 120000, EVENT_BUFFER = 300;
const el = id => document.getElementById(id);
const num = v => typeof v === 'number' && Number.isFinite(v);
const fmt = (v, d = 2) => num(v) ? v.toLocaleString('ru-RU', { minimumFractionDigits: d, maximumFractionDigits: d }) : '—';
const money = v => num(v) ? v.toLocaleString('ru-RU', { minimumFractionDigits: 2, maximumFractionDigits: 6 }) : '—';
const signed = (v, d = 2) => num(v) ? `${v > 0 ? '+' : v < 0 ? '−' : ''}${fmt(Math.abs(v), d)}` : '—';
const tone = v => !num(v) || v === 0 ? '' : v > 0 ? 'pos' : 'neg';
function dur(ms) {
  if (!num(ms)) return 'нет данных';
  if (ms < 0) ms = 0;
  if (ms < 1000) return `${Math.round(ms)} мс`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1).replace('.', ',')} с`;
  const s = Math.floor(ms / 1000);
  if (s < 3600) return `${Math.floor(s / 60)} мин ${String(s % 60).padStart(2, '0')} с`;
  return `${Math.floor(s / 3600)} ч ${String(Math.floor(s / 60) % 60).padStart(2, '0')} мин`;
}
const clock = t => { const d = new Date(t); return `${d.toLocaleTimeString('ru-RU')}.${String(d.getMilliseconds()).padStart(3, '0')}`; };
const titles = {
  SESSION_KEY_NOT_ACTIVE: 'Ключ бота не активен на PaperTrade',
  WALLET_STATE_NOT_FRESH: 'Поток кошелька молчит больше 120 с',
  INSUFFICIENT_FULL_MARGIN: 'Недостаточно полной маржи',
  MARKET_FEEDS_STALE: 'Цены HL/Binance устарели',
  TRADE_UNTIL_REACHED: 'Окно торговли закончилось',
  DAY_HIGH_WATER_TRIPPED: 'Сработал лимит отката дня (high-water)',
  LIVE_OFF: 'LIVE выключен (защита или kill switch)',
  EXECUTION_SYNC_NOT_READY: 'Сверка исполнений ещё не завершена',
  LIVE_EXECUTION_NOT_INTEGRATED: 'Торговый транспорт не интегрирован',
};
const feedNames = { paper: 'Paper (котировка сайта)', hl: 'Hyperliquid', bf: 'Binance Futures', bs: 'Binance Spot' };
const feedColors = { paper: '#d4a1ff', hl: '#78dab4', bf: '#edbd75', bs: '#91b9eb' };

// ---------- состояние клиента ----------
let snap = null, snapPerf = 0, serverOffset = 0, lastSuccess = 0;
let paused = false, inflight = false, timer = null, failures = 0, staleShown = false;
let controlBusy = false;
const rtts = [], okTimes = [];
const series = { paper: [], hl: [], bf: [], bs: [] };
let lastChartT = 0;
const eventLog = new Map(); // ключ t|kind|text → событие; накапливается дольше, чем 30 событий снимка
let eventsSig = '', eventsMaxSeen = 0, eventsFirst = true;

// Время сервера: независимо от сдвига часов браузера (панель может открываться через ssh -L).
const serverNow = () => Date.now() + serverOffset;
const sinceSnap = () => snap ? performance.now() - snapPerf : NaN;

// ---------- минимальные DOM-обновления ----------
function text(id, value) { const n = typeof id === 'string' ? el(id) : id; if (n.textContent !== value) n.textContent = value; }
function cls(id, value) { const n = typeof id === 'string' ? el(id) : id; if (n.className !== value) n.className = value; }
const detailCache = new Map();
function details(id, pairs) {
  // pairs: [label, value, className?]; перестраиваем только при смене набора подписей.
  const sig = pairs.map(p => p[0]).join('\u0001');
  let entry = detailCache.get(id);
  if (!entry || entry.sig !== sig) {
    const dds = [], nodes = [];
    for (const [label] of pairs) {
      const dt = document.createElement('dt'), dd = document.createElement('dd');
      dt.textContent = label; nodes.push(dt, dd); dds.push(dd);
    }
    el(id).replaceChildren(...nodes);
    entry = { sig, dds }; detailCache.set(id, entry);
  }
  pairs.forEach(([, value, c], i) => { text(entry.dds[i], String(value)); cls(entry.dds[i], c || ''); });
}

// ---------- рендер снимка ----------
function render(s) {
  const wallet = s.provider.wallet, cfg = s.config, feeds = s.feeds || {};
  const blockers = [...(s.blockers || [])];
  if (!s.liveExecutionIntegrated && !blockers.some(b => b.code === 'LIVE_EXECUTION_NOT_INTEGRATED')) blockers.push({ code: 'LIVE_EXECUTION_NOT_INTEGRATED', detail: 'Процесс только читает рынок и кошелёк.' });

  const trading = s.LIVE && s.exchangeMutationsEnabled && blockers.length === 0;
  text('live-label', !s.LIVE ? 'LIVE выключен — входы запрещены' : trading ? 'LIVE включён — бот может торговать' : 'LIVE включён, но входы сейчас заблокированы');
  text('mode-description', s.liveExecutionIntegrated ? 'Статус исполнения определяется runtime, не кнопкой на панели.' : 'Работает только мониторинг. Торговые заявки не отправляются.');
  cls('mode-banner', `banner ${trading ? 'ok' : !s.LIVE ? 'off' : ''}`.trim());
  el('start-trading').disabled = controlBusy || !!(s.LIVE && s.entriesEnabled) || !!(s.timer && s.timer.reached) || !s.liveExecutionIntegrated || !!(s.instrumentSelection && !s.instrumentSelection.liveReady);
  el('stop-trading').disabled = controlBusy;

  text('available', `${money(wallet.available)} USDC`);
  text('margin-note', s.fullMarginReady ? 'Полная маржа с резервом доступна.' : `Для входа нужно ${num(s.requiredForFullMargin) ? money(s.requiredForFullMargin) : '—'} USDC с резервом; не хватает ${num(s.marginShortfall) ? money(s.marginShortfall) : '—'} USDC`);
  text('exposure', `${money(cfg.margin)} × ${cfg.leverage}`);
  text('notional', `Номинал до ${money(s.maximumAuthorizedNotionalUsd)} USD`);

  const hl = feeds.hl || {}, bf = feeds.bf || {};
  text('btc', num(hl.mid) ? `$${fmt(hl.mid, 1)}` : '—');
  text('spread', `Спред HL − Binance: ${num(hl.mid) && num(bf.mid) ? signed(hl.mid - bf.mid) + ' $' : '—'}`);
  const paper = feeds.paper || {};
  const paperFresh = num(paper.mid) && num(paper.ageMs) && paper.ageMs <= cfg.maxFeedAgeMs && num(bf.mid) && num(bf.ageMs) && bf.ageMs <= cfg.maxFeedAgeMs;
  text('paper-price', num(paper.mid) ? `$${fmt(paper.mid, 1)}` : '—');
  text('paper-spread', paperFresh ? `Спред Paper − Binance: ${signed(paper.mid - bf.mid)} $` : 'Спред Paper − Binance: нет свежих данных');

  const dayPnl = s.pnlDay ? s.pnlDay.realisedUsd : s.guard ? s.guard.dayPnl : s.highWater && s.highWater.dayPnl;
  text('day-pnl', num(dayPnl) ? `${signed(dayPnl)} $` : '—');
  cls('day-pnl', tone(dayPnl));
  const hw = s.highWater && s.highWater.state;
  text('high-water', hw ? `Лучшая ${signed(hw.best)} · откат ${fmt(hw.drawdown)} из ${fmt(s.highWater.limitUsd, 0)}${hw.tripped ? ' · СРАБОТАЛ' : ''}` : `High-water: лимит ${fmt(s.highWater && s.highWater.limitUsd, 0)}, состояние не записано`);

  const managed = Array.isArray(s.openManaged) ? s.openManaged : [];
  text('positions', `${wallet.openPositions.length} / ${wallet.operations.length}`);
  text('positions-note', `Кошелёк: позиции / заявки · бот ведёт: ${managed.length}`);
  text('streams', s.marketFresh && s.providerFresh ? 'Свежие' : 'Есть задержки');
  cls('streams', s.marketFresh && s.providerFresh ? '' : 'neg');
  text('process', `PID ${s.pid} · входы ${s.entriesEnabled ? 'разрешены' : 'запрещены'}`);

  text('blocker-count', blockers.length ? `${blockers.length} блокер${blockers.length === 1 ? '' : blockers.length < 5 ? 'а' : 'ов'}` : 'Нет блокеров');
  cls('blocker-count', `pill ${blockers.length ? 'warn' : 'good'}`);
  renderBlockers(blockers);

  renderOpen(managed, wallet, hl.mid, s.markets || {});
  renderFeeds(feeds, cfg);
  renderDecisions(s.decisions);
  renderEvents(s.events);
  pushChart(s);

  el('portfolio').href = `https://exchange.papertrade.xyz/portfolio?view=${encodeURIComponent(s.actualWallet)}`;
  renderSlow(s);
  if (window.InstrumentDashboard) window.InstrumentDashboard.render(s);
  if (window.LiveSettingsPanel) window.LiveSettingsPanel.render(s);
  if (window.ShadowHistoryPanel) window.ShadowHistoryPanel.render(s);
}

let blockersSig = '';
function renderBlockers(blockers) {
  const sig = JSON.stringify(blockers);
  if (sig === blockersSig) return;
  blockersSig = sig;
  el('blockers').replaceChildren(...(blockers.length ? blockers.map(b => {
    const li = document.createElement('li'), strong = document.createElement('strong'), span = document.createElement('span');
    strong.textContent = titles[b.code] || b.code;
    span.textContent = b.detail || 'Не подтверждено'; li.append(strong, span); return li;
  }) : [Object.assign(document.createElement('li'), { className: 'clear', textContent: 'Блокеров нет: runtime допускает входы при появлении сигнала.' })]));
}

// Позиции: строки бота + позиции кошелька, которых бот не ведёт (чужие/ручные/не подхваченные).
let openSig = null, openRows = [];
function renderOpen(managed, wallet, hlMid, markets = {}) {
  const known = new Set(managed.map(p => p.positionId).filter(Boolean));
  const rows = managed.map((p, i) => ({
    key: p.id != null ? `b${p.id}` : `q${i}:${p.strategy}:${p.side}`,
    strategy: `${p.symbol || 'BTC'} · ${p.strategy || '—'}`, side: p.side, state: p.state || '—', live: p.live,
    entry: p.entry, mark: p.mark, fav: p.fav, pnl: num(p.est) ? p.est : p.gross, held: num(p.held) ? p.held * 1000 : NaN, bust: p.bust,
  }));
  for (const w of wallet.openPositions || []) {
    if (known.has(w.positionId)) continue;
    const dir = w.isLong ? 1 : -1;
    const symbol = w.instrumentId === 0 ? 'BTC' : w.instrumentId === 1 ? 'ETH' : null;
    const market = symbol && markets[symbol], quote = market && market.feeds && market.feeds.hl;
    const mark = quote && num(quote.mid) && num(quote.ageMs) && quote.ageMs <= market.maxFeedAgeMs ? quote.mid : symbol === 'BTC' && !market ? hlMid : NaN;
    const opened = num(w.openedAt) ? (w.openedAt > 1e12 ? w.openedAt : w.openedAt * 1000) : NaN;
    rows.push({ key: `w${w.positionId}`, strategy: `${symbol || 'Неизвестный тикер'} · кошелёк (не бот)`, side: w.isLong ? 'L' : 'S', state: `#${w.positionId} · маржа ${fmt(w.margin)} × ${w.leverage}`, live: true,
      entry: w.entry, mark, fav: num(mark) ? dir * (mark - w.entry) : NaN, pnl: NaN, held: num(opened) ? serverNow() - opened : NaN, bust: w.bust, foreign: true });
  }
  text('open-count', rows.length ? `${rows.length} активн.` : 'Нет позиций');
  cls('open-count', `pill ${rows.length ? 'warn' : 'neutral'}`);
  const sig = rows.map(r => r.key).join('|');
  if (sig !== openSig) {
    openSig = sig;
    openRows = rows.map(() => {
      const tr = document.createElement('tr'), cells = [];
      for (let k = 0; k < 9; k++) { const td = document.createElement('td'); tr.append(td); cells.push(td); }
      return { tr, cells, held: NaN };
    });
    if (rows.length) el('open').replaceChildren(...openRows.map(r => r.tr));
    else {
      const tr = document.createElement('tr'), td = document.createElement('td');
      td.colSpan = 9; td.className = 'muted'; td.textContent = 'Открытых позиций и заявок нет';
      tr.append(td); el('open').replaceChildren(tr);
    }
  }
  rows.forEach((r, i) => {
    const { cells, tr } = openRows[i];
    cls(tr, r.foreign ? 'foreign' : '');
    text(cells[0], r.strategy);
    text(cells[1], r.side === 'L' ? 'long' : r.side === 'S' ? 'short' : '—'); cls(cells[1], r.side === 'L' ? 'pos' : r.side === 'S' ? 'neg' : '');
    text(cells[2], `${r.live ? '' : '[paper] '}${r.state}`);
    text(cells[3], fmt(r.entry, 1)); text(cells[4], fmt(r.mark, 1));
    text(cells[5], signed(r.fav)); cls(cells[5], tone(r.fav));
    text(cells[6], signed(r.pnl)); cls(cells[6], tone(r.pnl));
    text(cells[8], fmt(r.bust, 1));
    openRows[i].held = r.held;
  });
  tickOpen();
}
function tickOpen() {
  const e = sinceSnap();
  for (const r of openRows) if (r.cells) text(r.cells[7], num(r.held) ? dur(r.held + (num(e) ? e : 0)) : '—');
}

const feedRows = new Map();
let feedLimits = {};
function renderFeeds(feeds, cfg) {
  const names = ['paper', ...Object.keys(feeds).filter(name => name !== 'paper')].filter(name => feeds[name]);
  if (names.join() !== [...feedRows.keys()].join()) {
    feedRows.clear();
    el('feeds').replaceChildren(...names.map(name => {
      const tr = document.createElement('tr'), cells = [];
      for (let k = 0; k < 7; k++) { const td = document.createElement('td'); tr.append(td); cells.push(td); }
      cells[0].textContent = feedNames[name] || name;
      const sw = document.createElement('i'); sw.className = `swatch ${name}`; cells[0].prepend(sw);
      feedRows.set(name, { tr, cells, f: null });
      return tr;
    }));
  }
  for (const name of names) {
    const row = feedRows.get(name), f = feeds[name];
    row.f = f;
    text(row.cells[1], fmt(f.bid, 1)); text(row.cells[2], fmt(f.ask, 1)); text(row.cells[3], fmt(f.mid, 2));
    text(row.cells[6], num(f.imb) ? `${signed(f.imb * 100, 1)}%` : '—'); cls(row.cells[6], tone(f.imb));
  }
  feedLimits = { price: cfg.maxFeedAgeMs, book: name => name === 'hl' ? 10000 : 3000 };
  tickFeeds();
}
function tickFeeds() {
  const e = num(sinceSnap()) ? sinceSnap() : 0;
  for (const [name, row] of feedRows) {
    const f = row.f; if (!f) continue;
    const a = f.ageMs === null ? NaN : f.ageMs + e, b = f.bookAgeMs === null ? NaN : f.bookAgeMs + e;
    const staleA = !num(a) || a > feedLimits.price, staleB = !f.bookNA && (!num(b) || b > feedLimits.book(name));
    text(row.cells[4], dur(a)); cls(row.cells[4], staleA ? 'stale' : '');
    text(row.cells[5], f.bookNA ? 'нет отдельного стакана' : dur(b)); cls(row.cells[5], staleB ? 'stale' : '');
    cls(row.tr, staleA || staleB ? 'stale-row' : '');
  }
}

let decisionsSig = '', decisionItems = [];
function renderDecisions(decisions) {
  const list = Object.entries(decisions || {}).filter(([, d]) => d && d.t);
  const sig = JSON.stringify(list);
  if (sig === decisionsSig) return;
  decisionsSig = sig;
  decisionItems = list.map(([name, d]) => {
    const li = document.createElement('li'), main = document.createElement('span'), ago = document.createElement('span');
    main.textContent = `${name}: ${d.ok ? 'сигнал разрешён' : 'вход запрещён'} · ${d.why || 'нет причины'} · ${clock(d.t)}`;
    ago.className = 'ago'; li.className = d.ok ? 'ok' : ''; li.append(main, ago);
    return { li, ago, t: d.t };
  });
  el('decisions').replaceChildren(...(decisionItems.length ? decisionItems.map(x => x.li) : [Object.assign(document.createElement('li'), { textContent: 'Решений ещё не было' })]));
  tickDecisions();
}
function tickDecisions() { const now = serverNow(); for (const x of decisionItems) text(x.ago, ` · ${dur(now - x.t)} назад`); }

function renderEvents(events) {
  for (const e of events || []) if (e && e.t) { const k = `${e.t}|${e.kind}|${e.text}`; if (!eventLog.has(k)) eventLog.set(k, e); }
  if (eventLog.size > EVENT_BUFFER) {
    const sorted = [...eventLog.entries()].sort((a, b) => b[1].t - a[1].t).slice(0, EVENT_BUFFER);
    eventLog.clear(); for (const [k, v] of sorted) eventLog.set(k, v);
  }
  drawEvents();
}
function drawEvents() {
  const showSkip = el('show-skip').checked;
  const all = [...eventLog.values()].sort((a, b) => b.t - a.t);
  const visible = all.filter(e => showSkip || e.kind !== 'skip').slice(0, 80);
  const sig = `${showSkip}|${visible.length}|${visible.length ? visible[0].t + visible[0].text : ''}|${visible.length ? visible[visible.length - 1].t : ''}`;
  text('events-note', `В буфере браузера ${all.length}, скрыто пропусков ${showSkip ? 0 : all.length - all.filter(e => e.kind !== 'skip').length}`);
  if (sig === eventsSig) return;
  eventsSig = sig;
  const prevMax = eventsMaxSeen;
  el('events').replaceChildren(...(visible.length ? visible.map(e => {
    const li = document.createElement('li'), time = document.createElement('time'), kind = document.createElement('span'), msg = document.createElement('span');
    time.textContent = clock(e.t); kind.className = 'kind'; kind.textContent = e.kind || '—'; msg.textContent = e.text || '';
    li.dataset.kind = e.kind || ''; if (!eventsFirst && e.t > prevMax) li.classList.add('fresh');
    li.append(time, kind, msg); return li;
  }) : [Object.assign(document.createElement('li'), { textContent: showSkip ? 'Нет событий' : 'Нет событий, кроме пропусков' })]));
  if (all.length) eventsMaxSeen = Math.max(eventsMaxSeen, all[0].t);
  eventsFirst = false;
}

// Защиты/наблюдения содержат обратные отсчёты — пересчитываются и между снимками.
function renderSlow(s) {
  if (!s) return;
  const cfg = s.config, wallet = s.provider.wallet, now = serverNow(), hwState = s.highWater && s.highWater.state;
  const cutoff = Date.parse(s.timer.absoluteCutoff);
  const left = until => num(until) && until > now ? `ещё ${dur(until - now)}` : 'нет';
  details('wallet-details', [
    ['Кошелёк', s.actualWallet], ['Всего на PaperTrade', `${money(wallet.balance)} USDC`],
    ['Доступно', `${money(wallet.available)} USDC`],
    ['Wallet stream', `${s.providerFresh ? 'свежий' : 'устарел / отключён'} · ${wallet.t ? dur(now - wallet.t) : '—'}`, s.providerFresh ? '' : 'neg'],
    ['Поток сделок', s.provider.tradesConnected ? 'подключён' : 'нет', s.provider.tradesConnected ? '' : 'neg'],
    ['Ключ бота', s.authorizationReady ? `активен · до ${s.provider.key.expiresAt ? new Date(s.provider.key.expiresAt).toLocaleDateString('ru-RU') : '—'}` : 'не активен', s.authorizationReady ? '' : 'neg'],
    ['Разрешены exchange mutations', s.exchangeMutationsEnabled ? 'да' : 'нет'],
    ['Ошибка потока', wallet.err || 'нет', wallet.err ? 'neg' : ''],
  ]);
  details('guards', [
    ['Таймер торговли', s.timer.reached ? `окно закрыто (${s.timer.absoluteCutoff})` : num(cutoff) ? `до ${new Date(cutoff).toLocaleTimeString('ru-RU')} · ${left(cutoff)}` : s.timer.absoluteCutoff, s.timer.reached ? 'neg' : ''],
    ['High-water', hwState ? `${hwState.tripped ? 'СРАБОТАЛ' : 'вооружён'} · откат ${fmt(hwState.drawdown)} / ${fmt(s.highWater.limitUsd, 0)}` : (s.highWater.armed ? 'вооружён' : 'НЕ вооружён'), hwState && hwState.tripped ? 'neg' : ''],
    ['Пауза после убытка', left(s.guard && s.guard.pausedUntil), s.guard && s.guard.pausedUntil ? 'neg' : ''],
    ['Funding', s.funding.mustClose ? 'закрыть позиции до XX:00' : s.funding.noEntryReason || 'вне запретного окна', s.funding.noEntryReason || s.funding.mustClose ? 'neg' : ''],
    ['Cooldown стены long / short', `${left(s.walls.until && s.walls.until.L)} / ${left(s.walls.until && s.walls.until.S)}`],
    ['OI запас L / S', s.oi ? `${fmt(s.oi.headroomL)} / ${fmt(s.oi.headroomS)}${s.oi.paused ? ' · пауза инструмента' : ''}` : '—', s.oi && s.oi.paused ? 'neg' : ''],
    ['Один слот', `${cfg.maxPositions}, включая pending`], ['Маржа / дробление', `${cfg.margin} USDC / нет`],
    ['Стороны', cfg.noLongs ? 'только short' : 'long и short'], ['Стоп BTC', `$${cfg.stopUsd}`],
    ['Подушка против', `$${cfg.cushionExitUsd} / ${cfg.cushionExitMs} мс`], ['Откат Binance', `$${cfg.binExitUsd}`],
    ['Ожидание входа', `${cfg.entryMaxWaitMs / 1000} с`], ['Круглые уровни', `$${cfg.roundZoneUsd} снизу / $${cfg.roundZoneAboveUsd} сверху`],
  ]);
  const r = s.regime || {};
  details('observations', [
    ['Режим', r.regime ? `${r.regime}${num(r.r5m) ? ` · размах 5 мин ${r.r5m}$` : ''}${r.pending ? ` → ${r.pending}` : ''}` : 'нет данных'],
    ['Стена против long', s.walls.blockL || 'нет'], ['Стена против short', s.walls.blockS || 'нет'],
    ['Видимых стен', String((s.walls.walls || []).length)],
    ['Процесс запущен', s.startedAt ? `${new Date(s.startedAt).toLocaleTimeString('ru-RU')} · ${dur(now - Date.parse(s.startedAt))}` : '—'],
  ]);
}

// ---------- график ----------
function pushChart(s) {
  const t = Date.parse(s.observedAt);
  if (!(t > lastChartT)) return;
  lastChartT = t;
  for (const k of Object.keys(series)) {
    const f = s.feeds && s.feeds[k];
    if (f && num(f.mid)) series[k].push([t, f.mid]);
    const cut = t - CHART_WINDOW_MS - 2000;
    while (series[k].length && series[k][0][0] < cut) series[k].shift();
  }
}
function drawChart() {
  const canvas = el('chart'), dpr = window.devicePixelRatio || 1, w = canvas.clientWidth, h = canvas.clientHeight;
  if (!w || !h) return;
  if (canvas.width !== Math.round(w * dpr) || canvas.height !== Math.round(h * dpr)) { canvas.width = Math.round(w * dpr); canvas.height = Math.round(h * dpr); }
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0); ctx.clearRect(0, 0, w, h);
  const tmax = serverNow(), tmin = tmax - CHART_WINDOW_MS, padL = 64, padR = 8, padT = 8, padB = 18;
  let lo = Infinity, hi = -Infinity;
  for (const k of Object.keys(series)) for (const [t, v] of series[k]) if (t >= tmin) { lo = Math.min(lo, v); hi = Math.max(hi, v); }
  const entries = (snap && Array.isArray(snap.openManaged) ? snap.openManaged : []).filter(p => num(p.entry));
  for (const p of entries) if (num(lo) && Math.abs(p.entry - (lo + hi) / 2) < 400) { lo = Math.min(lo, p.entry); hi = Math.max(hi, p.entry); }
  ctx.font = '11px ui-sans-serif, system-ui, sans-serif'; ctx.fillStyle = '#7c8b9e';
  if (!num(lo) || !num(hi)) { ctx.fillText('Накопление данных…', padL, h / 2); return; }
  if (hi - lo < 10) { const m = (hi + lo) / 2; lo = m - 5; hi = m + 5; }
  const span = hi - lo; lo -= span * 0.06; hi += span * 0.06;
  const x = t => padL + (t - tmin) / CHART_WINDOW_MS * (w - padL - padR), y = v => padT + (hi - v) / (hi - lo) * (h - padT - padB);
  ctx.strokeStyle = '#1d2737'; ctx.lineWidth = 1;
  for (let i = 0; i <= 3; i++) {
    const v = lo + (hi - lo) * i / 3, yy = Math.round(y(v)) + 0.5;
    ctx.beginPath(); ctx.moveTo(padL, yy); ctx.lineTo(w - padR, yy); ctx.stroke();
    ctx.fillText(fmt(v, 0), 4, yy + 4);
  }
  for (let s = 0; s <= CHART_WINDOW_MS / 1000; s += 30) ctx.fillText(s ? `−${s} с` : 'сейчас', x(tmax - s * 1000) - (s ? 14 : 34), h - 4);
  for (const p of entries) {
    ctx.setLineDash([4, 4]); ctx.strokeStyle = p.side === 'L' ? '#78dab4' : '#f2a3ac';
    const yy = Math.round(y(p.entry)) + 0.5; ctx.beginPath(); ctx.moveTo(padL, yy); ctx.lineTo(w - padR, yy); ctx.stroke(); ctx.setLineDash([]);
    ctx.fillStyle = ctx.strokeStyle; ctx.fillText(`${p.side === 'L' ? 'long' : 'short'} вход`, w - padR - 70, yy - 4); ctx.fillStyle = '#7c8b9e';
  }
  ctx.save(); ctx.beginPath(); ctx.rect(padL, 0, w - padL - padR, h); ctx.clip();
  for (const k of ['bs', 'bf', 'hl', 'paper']) {
    const pts = series[k]; if (pts.length < 2) continue;
    ctx.strokeStyle = feedColors[k]; ctx.lineWidth = k === 'paper' ? 2.2 : k === 'hl' ? 1.8 : 1.2; ctx.beginPath();
    pts.forEach(([t, v], i) => i ? ctx.lineTo(x(t), y(v)) : ctx.moveTo(x(t), y(v)));
    if (sinceSnap() <= STALE_MS) ctx.lineTo(x(tmax), y(pts[pts.length - 1][1]));
    ctx.stroke();
  }
  ctx.restore();
}

// ---------- соединение ----------
function showConnection() {
  const e = sinceSnap();
  if (paused) { text('connection', 'Пауза панели'); cls('connection', 'pill neutral'); el('start-trading').disabled=true; return; }
  if (!snap || failures || e > STALE_MS) {
    text('connection', snap ? `Нет свежих данных ${dur(e)}` : 'Нет соединения'); cls('connection', 'pill bad');
    el('start-trading').disabled=true;
    if (!staleShown) {
      staleShown = true;
      text('live-label', 'Статус LIVE неизвестен: соединение потеряно');
      text('mode-description', 'Показанные ниже значения — предыдущий снимок, не подтверждение текущего состояния.');
      cls('mode-banner', 'banner lost');
    }
    return;
  }
  staleShown = false;
  text('connection', snap.technicalReady ? 'Онлайн' : 'Онлайн · потоки с задержкой');
  cls('connection', `pill ${snap.technicalReady ? 'good' : 'warn'}`);
}
function showRate() {
  const now = performance.now();
  while (okTimes.length && now - okTimes[0] > 5000) okTimes.shift();
  const avg = rtts.length ? rtts.reduce((a, b) => a + b, 0) / rtts.length : NaN;
  text('rate', paused ? 'обновление остановлено' : `${(okTimes.length / 5).toFixed(1).replace('.', ',')} Гц · ответ ${num(avg) ? Math.round(avg) : '—'} мс`);
}

function schedule(delay) { clearTimeout(timer); timer = setTimeout(poll, Math.max(0, delay)); }
async function poll() {
  timer = null;
  if (paused || document.hidden || inflight) return;
  inflight = true;
  const t0 = performance.now();
  try {
    const response = await fetch('/state', { cache: 'no-store', signal: AbortSignal.timeout(TIMEOUT_MS) });
    if (!response.ok) throw new Error('runtime недоступен');
    const s = await response.json();
    const t1 = performance.now(), observed = Date.parse(s.observedAt);
    if (!num(observed)) throw new Error('нет observedAt');
    if (Date.now() - observed > Math.max(10000, t1 - t0 + STALE_MS)) throw new Error('устаревший снимок');
    // Оценка часов сервера: середина запроса ≈ observedAt.
    const offset = observed - (Date.now() - (t1 - t0) / 2);
    serverOffset = snap ? serverOffset * 0.8 + offset * 0.2 : offset;
    snap = s; snapPerf = t1 - Math.max(0, (t1 - t0) / 2);
    rtts.push(t1 - t0); if (rtts.length > 10) rtts.shift(); okTimes.push(t1);
    failures = 0; lastSuccess = Date.now(); text('last-error', '');
    render(s);
    showConnection();
  } catch {
    failures++;
    showConnection();
    text('last-error', `Не удалось обновить состояние (попыток подряд: ${failures}). Последний успешный снимок: ${lastSuccess ? new Date(lastSuccess).toLocaleTimeString('ru-RU') : 'нет'}.`);
  } finally {
    inflight = false;
    if (!paused && !document.hidden) schedule(failures ? Math.min(5000, POLL_MS * 2 ** Math.min(failures, 4)) : POLL_MS - (performance.now() - t0));
  }
}

function tick() {
  if (document.hidden) return;
  const e = sinceSnap();
  text('timestamp', snap ? `Снимок ${new Date(Date.parse(snap.observedAt)).toLocaleTimeString('ru-RU')} · ${dur(e)} назад` : 'Нет снимка');
  cls('timestamp', e > STALE_MS ? 'late' : '');
  showConnection(); showRate();
  if (snap) { tickFeeds(); tickOpen(); tickDecisions(); renderSlow(snap); }
  drawChart();
  if (window.InstrumentDashboard) window.InstrumentDashboard.tick(serverNow());
}

el('pause').addEventListener('click', () => {
  paused = !paused;
  el('pause').setAttribute('aria-pressed', String(paused));
  text('pause', paused ? 'Продолжить панель' : 'Пауза панели');
  if (paused) clearTimeout(timer); else poll();
  tick();
});
el('show-skip').addEventListener('change', () => { eventsSig = ''; drawEvents(); });
document.addEventListener('visibilitychange', () => { if (document.hidden) clearTimeout(timer); else { poll(); tick(); } });
window.addEventListener('online', () => poll());
setInterval(tick, TICK_MS);
poll();

async function tradingControl(action) {
  if (controlBusy) return;
  if (action === 'start' && snap && snap.instrumentSelection && !snap.instrumentSelection.liveReady) { text('control-message', snap.instrumentSelection.reason); return; }
  if (action === 'start' && !window.confirm('Включить LIVE 2100 × 600 до 15:30 МСК? Защиты остаются действовать.')) return;
  if (paused) { paused=false;el('pause').setAttribute('aria-pressed','false');text('pause','Пауза панели'); }
  controlBusy=true;el('start-trading').disabled=true;el('stop-trading').disabled=true;
  text('control-message', action === 'stop' ? 'Выключаю LIVE, снимаю свои заявки и отправляю свои позиции на закрытие…' : 'Проверяю ключ, баланс, позиции и защиты…');
  cls('control-message','control-message');
  try {
    const r=await fetch(`/api/trading/${action}`,{method:'POST',headers:{'X-Vchera2-Control':'1'},signal:AbortSignal.timeout(30000)});
    const result=await r.json();
    if(!r.ok || result.ok!==true)throw new Error((result.reasons || []).join(' ') || result.message || 'Команда не подтверждена. Проверьте состояние.');
    text('control-message',result.message || 'Команда принята. Дождитесь фактического исполнения заявок на бирже.');
  } catch(e) { text('control-message',e.message || 'Команда не подтверждена. Проверьте состояние.');cls('control-message','control-message bad'); }
  finally { controlBusy=false;await poll();el('stop-trading').disabled=false;if(snap)el('start-trading').disabled=!!(snap.LIVE&&snap.entriesEnabled)||!!snap.timer.reached||failures>0||!!(snap.instrumentSelection&&!snap.instrumentSelection.liveReady); }
}
el('start-trading').addEventListener('click',()=>tradingControl('start'));
el('stop-trading').addEventListener('click',()=>tradingControl('stop'));
