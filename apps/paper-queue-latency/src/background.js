// Фон расширения: собирает данные и раздаёт их всем окнам (на любых вкладках).
// Источник 1 — вкладка papertrade: её content-скрипт открывает поток stream.papertrade.xyz
//   (сервер пускает только с Origin биржи; Chrome не даёт расширению подменить Origin у WebSocket —
//   проверено 17.10.2026, поэтому поток открывается изнутри вкладки биржи) и пересылает сюда.
// Источник 2 (запасной, если вкладки биржи нет) — поток paperwatch.app.
// Цены BTC/ETH — Hyperliquid. Только чтение: ничего не подписывает и не отправляет.
importScripts('core.js', 'latency.js', 'binance.js', 'spread.js');
const C = globalThis.PTQ;

const HL_WS = 'wss://api.hyperliquid.xyz/ws';
const PW_EVENTS = 'https://paperwatch.app/events';
const RANK_URL = (w) => `https://exchange.papertrade.xyz/queue-rank/accounts/${w}`;
const SUMMARY_URL = 'https://exchange.papertrade.xyz/query/protocol/summary';
const CHANGES_MAX = 100;
const SEND_MS = 250;          // окна получают цифры не чаще 4 раз в секунду
const IDLE_CLOSE_MS = 30000;  // нет открытых окон 30 с — закрываем соединения
const FALLBACK_AFTER_MS = 3000; // вкладка биржи молчит 3 с — включаем запасной источник
const FILL_KEEP_MS = 90000;

const DEFAULTS = { visible: true, collapsed: false, market: 'BTC', margin: 300, lev: 500, wallet: '', x: null, y: null, w: 290, h: null, sound: false, chartSpot: true, chartFut: true, chartAlign: true, guardQueue: true, maxAhead: 30, maxWait: 6 };

chrome.runtime.onInstalled.addListener(async () => {
  const cur = await chrome.storage.local.get(null);
  const add = {};
  for (const [k, v] of Object.entries(DEFAULTS)) if (!(k in cur)) add[k] = v;
  if (Object.keys(add).length) await chrome.storage.local.set(add);
});

// Кнопка расширения и сочетание клавиш — показать/спрятать окно на всех вкладках.
async function toggleVisible() {
  const { visible } = await chrome.storage.local.get('visible');
  await chrome.storage.local.set({ visible: visible === false });
}
chrome.action.onClicked.addListener(toggleVisible);
chrome.commands.onCommand.addListener((cmd) => { if (cmd === 'toggle-panel') toggleVisible(); });

// ---------- состояние ----------
const queue = C.createQueueModel();
const LATENCY_KEYS = ['gateEntryBudgetSec', 'gateExitBudgetSec', 'gateSignalTtlSec'];
let timingLimits = { gateEntryBudgetSec: 6, gateExitBudgetSec: 15, gateSignalTtlSec: 5 };
let timing = new PTQLatency.TimingGate(), timingConfigError = false;
function applyTiming(values) {
  for (const key of LATENCY_KEYS) if (key in values) timingLimits[key] = values[key];
  try {
    if (Object.values(timingLimits).some(x => typeof x !== 'number' || !Number.isFinite(x) || x <= 0 || x > 600)) throw new Error('INVALID_CONFIG');
    timing = new PTQLatency.TimingGate({ entry_budget_ms: timingLimits.gateEntryBudgetSec * 1000, exit_budget_ms: timingLimits.gateExitBudgetSec * 1000, signal_ttl_ms: timingLimits.gateSignalTtlSec * 1000 });
    timingConfigError = false;
  } catch { timingConfigError = true; }
}
chrome.storage.local.get(LATENCY_KEYS).then(applyTiming).catch(() => { timingConfigError = true; });
chrome.storage.onChanged.addListener((changes, area) => {
  if (area === 'local' && LATENCY_KEYS.some(k => k in changes)) applyTiming(Object.fromEntries(LATENCY_KEYS.filter(k => k in changes).map(k => [k, changes[k].newValue])));
});
function timingSnapshot(now) {
  return timingConfigError ? { ...PTQLatency.empty(), reasons: ['INVALID_CONFIG'] } : timing.evaluate(now, source() === 'direct');
}
let trading = null, tradingAt = 0;
let mids = { BTC: null, ETH: null, t: 0, src: null };
let fills = [];
const seen = new Set();
const conn = { pump: 'off', hl: 'off', paperwatch: 'off', bs: 'off', bf: 'off' };
let pumpDataAt = 0;           // когда вкладка биржи последний раз прислала данные
let lastError = null;
let my = { wallet: '', entries: null, t: 0, error: null };

// ---------- журнал изменений биржи (сохраняется между запусками) ----------
// На экран не выводится (Влад 17.10: «пусть будет в коде, но не на экране»). Лежит в chrome.storage.local
// под ключом 'changes'; посмотреть: chrome://extensions → «Очередь papertrade» → service worker → консоль:
//   chrome.storage.local.get('changes').then((s) => console.table(s.changes))
const capWatch = C.createCapWatch();
const curParams = {};          // текущие настройки биржи из всех источников
let paramBase = null;          // с чем сравниваем (последнее известное), хранится в storage
let changes = [];              // [{ t, kind, key?, text }]
let watchSince = null;
const journalReady = chrome.storage.local.get(['paramBase', 'changes', 'headBase', 'watchSince']).then((s) => {
  paramBase = s.paramBase && typeof s.paramBase === 'object' ? s.paramBase : {};
  changes = Array.isArray(s.changes) ? s.changes : [];
  capWatch.setHeadBase(s.headBase);
  watchSince = s.watchSince || Date.now();
  if (!s.watchSince) chrome.storage.local.set({ watchSince });
});
function addChanges(list) {
  if (!list.length) return;
  changes = [...list, ...changes].slice(0, CHANGES_MAX);
  chrome.storage.local.set({ changes, headBase: capWatch.headBase() });
}
// Сравнение настроек с последними известными: новое — молча запоминаем, изменённое — в журнал.
let paramTimer = null;
function checkParams() {
  if (paramTimer) return;
  paramTimer = setTimeout(async () => {
    paramTimer = null;
    await journalReady;
    const ev = [];
    let dirty = false;
    for (const [k, v] of Object.entries(curParams)) {
      if (!(k in paramBase)) { paramBase[k] = v; dirty = true; }
      else if (paramBase[k] !== v) { ev.push({ t: Date.now(), kind: 'param', key: k, text: `${k}: ${paramBase[k]} → ${v}` }); paramBase[k] = v; dirty = true; }
    }
    if (dirty) chrome.storage.local.set({ paramBase });
    addChanges(ev);
  }, 1500);
}
// Пополнение и журнал — только по данным самой биржи: у paperwatch потолки бывают другими (17.10 видели
// 186 против 168 млн$ у биржи), а смешивание источников даёт ложные «урезан / снова открыт».
function onTrading(t, fromExchange) {
  trading = t; tradingAt = Date.now();
  if (fromExchange) addChanges(capWatch.feed(t, tradingAt, mids));
  if (t.params) { Object.assign(curParams, t.params); checkParams(); }
}
let summaryTimer = null;
async function pollSummary() {
  if (!ports.size) return;
  try {
    const r = await fetch(SUMMARY_URL, { credentials: 'omit', headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(8000), cache: 'no-store' });
    if (!r.ok) return;
    const p = C.summaryParams(await r.json());
    if (Object.keys(p).length) { Object.assign(curParams, p); checkParams(); }
  } catch {}
}

const pumpLive = () => conn.pump === 'open' && Date.now() - pumpDataAt < FALLBACK_AFTER_MS;
const source = () => (pumpLive() ? 'direct' : conn.paperwatch === 'open' ? 'paperwatch' : null);

// ---------- окна (порты 'ptq') и вкладки биржи (порты 'ptq-pump') ----------
const ports = new Set();
const pumps = new Set();
let activePump = null;
let idleTimer = null;
chrome.runtime.onConnect.addListener((port) => {
  if (port.name === 'ptq') {
    ports.add(port);
    if (idleTimer) { clearTimeout(idleTimer); idleTimer = null; }
    startAll();
    try { port.postMessage({ type: 'ticks', full: true, series: ticks.dump() }); } catch {}
    port.onDisconnect.addListener(() => {
      ports.delete(port);
      if (!ports.size && !idleTimer) idleTimer = setTimeout(() => { idleTimer = null; if (!ports.size) stopAll(); }, IDLE_CLOSE_MS);
    });
  } else if (port.name === 'ptq-pump') {
    pumps.add(port);
    port.onMessage.addListener((m) => onPumpMessage(port, m));
    port.onDisconnect.addListener(() => {
      pumps.delete(port);
      if (activePump === port) { activePump = null; conn.pump = 'off'; pickPump(); }
    });
    pickPump();
  }
});

// Поток открывает только одна вкладка биржи; закрыли её — берём следующую.
function pickPump() {
  if (!ports.size) return;
  if (activePump || !pumps.size) { if (!activePump) ensureFallback(); return; }
  activePump = [...pumps][0];
  conn.pump = 'connecting';
  try { activePump.postMessage({ type: 'start' }); } catch { pumps.delete(activePump); activePump = null; pickPump(); }
}
function onPumpMessage(port, m) {
  if (!m || port !== activePump) return;
  if (m.type === 'status') {
    conn.pump = m.state;
    if (m.error) lastError = 'вкладка биржи: ' + m.error;
    if (m.state !== 'open') ensureFallback();
  } else if (m.type === 'pub') {
    pumpDataAt = Date.now();
    conn.pump = 'open';
    onPub(m.ch, m.data);
    if (conn.paperwatch !== 'off') stopPaperwatch();
  }
}
function onPressure(p) {
  queue.add(p);
  const g = String(p.limit);
  if (curParams['газ на блок (очередь)'] !== g) { curParams['газ на блок (очередь)'] = g; checkParams(); }
}
function onPub(ch, data) {
  if (ch === 'pressure:current') { timing.observe(data, Date.now()); const p = C.parsePressure(data); if (p.ok) onPressure(p); else lastError = 'очередь: ' + p.error; }
  else if (ch === 'trading:current') { const t = C.parseTrading(data); if (t.ok) onTrading(t, true); else lastError = 'рынки: ' + t.error; }
  else if (ch === 'public:trades') addFills(C.parseTrades(data));
}
function addFills(list) {
  for (const f of list) if (!seen.has(f.seq)) { seen.add(f.seq); fills.push(f); }
  const cut = Date.now() - FILL_KEEP_MS;
  if (fills.length && fills[0].t < cut) {
    fills.sort((a, b) => a.t - b.t);
    while (fills.length && fills[0].t < cut) seen.delete(fills.shift().seq);
  }
}

function snapshot() {
  const now = Date.now();
  const timing = timingSnapshot(now);
  const f = {}, caps = {};
  for (const sym of C.MARKETS) {
    f[sym] = { w10: C.fillsSummary(fills, sym, now, 10000), w60: C.fillsSummary(fills, sym, now, 60000) };
    for (const side of ['long', 'short']) caps[`${sym}:${side}`] = capWatch.status(`${sym}:${side}`, now);
  }
  return {
    type: 'state', now, source: source(), conn: { ...conn }, lastError,
    queue: queue.summary(now),
    timing, timingLimits: { ...timingLimits }, spread: spreadSnapshot(now, timing),
    trading: trading && { ...trading, params: undefined }, tradingAgeMs: tradingAt ? now - tradingAt : null,
    mids: { ...mids, ageMs: mids.t ? now - mids.t : null },
    fills: f, my, caps,
  };
}
setInterval(() => {
  if (!ports.size) return;
  if (!pumpLive()) ensureFallback();
  const s = snapshot();
  for (const p of ports) { try { p.postMessage(s); } catch {} }
}, SEND_MS);

// ---------- тики Binance spot/futures для графика Paper ----------
// Пачка уходит окнам раз в 100 мс; новая вкладка получает накопленную историю одним сообщением (full).
const ticks = PTQBinance.createStore();
let tickBatch = [], tickTimer = null;
const spreadModel = PTQSpread.createModel();
const binance = PTQBinance.createFeed({
  onTick(v, m, t, p) { ticks.add(v, m, t, p); tickBatch.push([v, m, t, p]); spreadModel.tick(v, m, t, p); },
  onState(v, st) { conn[v === 's' ? 'bs' : 'bf'] = st; },
});
function startBinance() {
  binance.start();
  if (!tickTimer) tickTimer = setInterval(() => {
    if (!tickBatch.length) return;
    const msg = { type: 'ticks', ticks: tickBatch }; tickBatch = [];
    for (const p of ports) { try { p.postMessage(msg); } catch {} }
  }, 100);
}
function stopBinance() {
  binance.stop();
  if (tickTimer) { clearInterval(tickTimer); tickTimer = null; }
  tickBatch = []; ticks.clear(); spreadModel.reset();
}
// Спред Binance↔Paper и совет guard по обоим рынкам (правила — в spread.js).
function spreadSnapshot(now, tg) {
  const out = {};
  const scaleOk = !!(mids.BTC && mids.ETH && mids.t && now - mids.t < 3000);
  const tOk = tg && tg.entry_ms !== null && tg.exit_ms !== null;
  for (const m of C.MARKETS) {
    const q = hlBbo[m];
    if (q && now - q.t < 3000) spreadModel.sample(m, now, q.mid, q.t);
    else if (mids[m] && mids.t && now - mids.t < 3000) spreadModel.sample(m, now, mids[m], mids.t);
    const ctx = { timingAllow: tOk ? !!tg.allow : null, timingReasons: tg && tg.reasons ? tg.reasons.map((c) => PTQLatency.reason(c)) : [],
      paused: !!(trading && trading.paused), scale: m === 'ETH' && scaleOk ? mids.ETH / mids.BTC : 1, scaleOk: m === 'BTC' || scaleOk };
    out[m] = spreadModel.evaluate(m, now, ctx);
  }
  return out;
}

// ---------- цены Hyperliquid (нужны, чтобы перевести OI из монет в $) ----------
let hl = null, hlRetry = 0, hlTimer = null, hlPing = null;
const hlBbo = {};            // coin → { mid, t } из канала bbo
function startHl() {
  if (hl || !ports.size) return;
  conn.hl = 'connecting';
  let sock;
  try { sock = new WebSocket(HL_WS); } catch { scheduleHl(); return; }
  hl = sock;
  sock.onopen = () => {
    conn.hl = 'open'; hlRetry = 0;
    sock.send(JSON.stringify({ method: 'subscribe', subscription: { type: 'allMids' } }));
    // BBO — та же цена, по которой исполняет Paper (середина лучших bid/ask HyperCore), обновляется чаще allMids
    for (const coin of C.MARKETS) sock.send(JSON.stringify({ method: 'subscribe', subscription: { type: 'bbo', coin } }));
    hlPing = setInterval(() => { try { sock.send('{"method":"ping"}'); } catch {} }, 30000);
  };
  sock.onmessage = (ev) => {
    let m; try { m = JSON.parse(ev.data); } catch { return; }
    if (m.channel === 'bbo' && m.data && Array.isArray(m.data.bbo)) {
      const b = C.num(m.data.bbo[0] && m.data.bbo[0].px), a = C.num(m.data.bbo[1] && m.data.bbo[1].px), coin = String(m.data.coin || '');
      if (b && a && a >= b && C.MARKETS.includes(coin)) { const t = Date.now(); hlBbo[coin] = { mid: (a + b) / 2, t }; mids = { ...mids, [coin]: (a + b) / 2, t, src: 'hl' }; }
      return;
    }
    if (m.channel === 'allMids' && m.data && m.data.mids) {
      const b = C.num(m.data.mids.BTC), e = C.num(m.data.mids.ETH);
      if (b && e) mids = { BTC: b, ETH: e, t: Date.now(), src: 'hl' };
    }
  };
  sock.onclose = () => { if (hl === sock) hl = null; clearInterval(hlPing); conn.hl = 'reconnecting'; scheduleHl(); };
  sock.onerror = () => {};
}
function scheduleHl() {
  if (hlTimer || !ports.size) return;
  hlTimer = setTimeout(() => { hlTimer = null; startHl(); }, Math.min(15000, 1000 * Math.pow(2, hlRetry++)));
}
function stopHl() {
  if (hlTimer) { clearTimeout(hlTimer); hlTimer = null; }
  clearInterval(hlPing);
  if (hl) { const s = hl; hl = null; try { s.close(); } catch {} }
  conn.hl = 'off';
}

// ---------- запасной источник: поток paperwatch.app (SSE через fetch) ----------
// Включается, только если вкладка биржи молчит дольше FALLBACK_AFTER_MS (даём ей подключиться первой).
let pwAbort = null, pwTimer = null, pwRetry = 0, startedAt = 0;
function ensureFallback() {
  if (!ports.size || pumpLive() || pwAbort || pwTimer) return;
  if (Date.now() - Math.max(startedAt, pumpDataAt) < FALLBACK_AFTER_MS) return;
  startPaperwatch();
}
async function startPaperwatch() {
  if (pwAbort || !ports.size) return;
  const ac = new AbortController();
  pwAbort = ac;
  conn.paperwatch = 'connecting';
  try {
    const r = await fetch(PW_EVENTS, { credentials: 'omit', headers: { Accept: 'text/event-stream' }, signal: ac.signal, cache: 'no-store' });
    if (!r.ok || !r.body) throw new Error('HTTP ' + r.status);
    conn.paperwatch = 'open'; pwRetry = 0;
    const reader = r.body.getReader();
    const dec = new TextDecoder();
    let buf = '';
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let i;
      while ((i = buf.indexOf('\n\n')) >= 0) {
        const msg = buf.slice(0, i); buf = buf.slice(i + 2);
        if (pumpLive()) continue; // вкладка биржи работает — запасной не мешает
        const ev = (msg.match(/^event: ?(.*)$/m) || [])[1];
        const data = msg.split('\n').filter((l) => l.startsWith('data:')).map((l) => l.slice(5).trim()).join('\n');
        let j; try { j = JSON.parse(data); } catch { continue; }
        if (ev === 'pressure') { const p = C.pressureFromPaperwatch(j); if (p.ok) onPressure(p); }
        else if (ev === 'mid' && j.mids && (mids.src !== 'hl' || Date.now() - mids.t > 10000)) {
          const b = C.num(j.mids.BTC), e = C.num(j.mids.ETH);
          if (b && e) mids = { BTC: b, ETH: e, t: Date.now(), src: 'paperwatch' };
        }
        else if (ev === 'oi') { const t = C.tradingFromPaperwatch(j, mids); if (t.ok) onTrading(t, false); }
        else if (ev === 'fills' && Array.isArray(j)) {
          addFills(j.map((f) => ({ seq: 'pw' + f.seq, t: C.num(f.t), sym: String(f.sym || '').toUpperCase(), side: f.long ? 'long' : 'short', kind: f.kind === 'open' ? 'open' : /liq/i.test(f.kind) ? 'liq' : 'close', notional: C.num(f.notional), lev: C.num(f.lev) }))
            .filter((f) => f.t !== null && f.notional !== null));
        }
      }
    }
    throw new Error('поток закрыт');
  } catch (e) {
    if (pwAbort === ac) pwAbort = null;
    if (ac.signal.aborted) { conn.paperwatch = 'off'; return; }
    conn.paperwatch = 'error';
    if (!pumpLive()) lastError = 'paperwatch: ' + e.message;
    if (!pwTimer && ports.size && !pumpLive()) pwTimer = setTimeout(() => { pwTimer = null; startPaperwatch(); }, Math.min(30000, 2000 * Math.pow(2, pwRetry++)));
  }
}
function stopPaperwatch() {
  if (pwTimer) { clearTimeout(pwTimer); pwTimer = null; }
  if (pwAbort) { const a = pwAbort; pwAbort = null; a.abort(); }
  conn.paperwatch = 'off';
}

// ---------- место моих заявок в очереди (по адресу кошелька, если указан) ----------
let rankTimer = null, rankBusy = false;
async function pollRank() {
  if (rankBusy || !ports.size) return;
  const { wallet } = await chrome.storage.local.get('wallet');
  const w = typeof wallet === 'string' ? wallet.trim().toLowerCase() : '';
  if (!/^0x[0-9a-f]{40}$/.test(w)) { my = { wallet: '', entries: null, t: 0, error: null }; return; }
  rankBusy = true;
  try {
    const r = await fetch(RANK_URL(w), { credentials: 'omit', headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(5000), cache: 'no-store' });
    if (r.status === 429) throw new Error('платформа просит реже (429)');
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const j = await r.json();
    if (!j || !Array.isArray(j.entries)) throw new Error('непонятный ответ');
    const entries = j.entries.map((e) => ({ id: String(e.intentId || ''), position: C.num(e.position) })).filter((e) => e.position !== null).sort((a, b) => a.position - b.position);
    my = { wallet: w, entries, t: Date.now(), error: null };
  } catch (e) {
    my = { ...my, wallet: w, error: e.message };
  } finally { rankBusy = false; }
}

function startAll() {
  if (!startedAt || (!rankTimer && !summaryTimer)) startedAt = Date.now();
  pickPump();
  startHl();
  startBinance();
  if (!rankTimer) rankTimer = setInterval(pollRank, 2000);
  if (!summaryTimer) { pollSummary(); summaryTimer = setInterval(pollSummary, 60000); }
}
function stopAll() {
  if (activePump) { try { activePump.postMessage({ type: 'stop' }); } catch {} activePump = null; }
  conn.pump = 'off';
  stopHl(); stopPaperwatch(); stopBinance();
  if (rankTimer) { clearInterval(rankTimer); rankTimer = null; }
  if (summaryTimer) { clearInterval(summaryTimer); summaryTimer = null; }
  queue.reset(); timing.reset(); trading = null; fills = []; seen.clear();
}
