'use strict';
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const { privateRuntime, atomicPrivateJSON } = require('./runtime.cjs');
const safety = require('./safety.cjs');
const { marginCapacity } = require('./execution-state.cjs');
const { authorizedControl } = require('./trading-controls.cjs');
const { marketData } = require('./market-data.cjs');
const { transportStatus } = require('./market-stream.cjs');
const { InstrumentSelection } = require('./instrument-selection.cjs');
const { ResearchFeed } = require('./research-feed.cjs');
const { readResearchStatus } = require('./research-status.cjs');
const { LiveSettingsStore, makeSchedulerPlan, MAX_BODY_BYTES: MAX_LIVE_SETTINGS_BODY } = require('./live-settings.cjs');
const { liveSettingsContext } = require('./live-settings-context.cjs');
const { createShadowRuntime, historyView, clearHistoryCache } = require('./shadow-runtime.cjs');
const { decisionDetail, diagnosticSummary } = require('./shadow-decision-view.cjs');
safety.assertLaunchConfig(safety.config);
const root = privateRuntime();
const instruments = new InstrumentSelection({ file: path.join(root, 'instrument-selection.json'), save: atomicPrivateJSON });
const liveSettings = new LiveSettingsStore({ file: path.join(root, 'live-settings.json'), defaults: safety.config.strategy });
const lock = path.join(root, 'runner.lock');
function acquireLock() {
  try {
    const fd = fs.openSync(lock, 'wx', 0o600);
    fs.writeFileSync(fd, String(process.pid)); fs.closeSync(fd);
  } catch (e) {
    if (e.code !== 'EEXIST') throw e;
    const pid = Number(fs.readFileSync(lock, 'utf8'));
    if (!Number.isInteger(pid) || pid <= 0) throw new Error('Invalid runner lock; inspect manually');
    try { process.kill(pid, 0); } catch (error) {
      if (error.code === 'ESRCH') { fs.unlinkSync(lock); return acquireLock(); }
      throw error;
    }
    throw new Error('Another runner already holds this wallet runtime lock');
  }
}
acquireLock();
const shadowRuntime = createShadowRuntime();
let shadowStartError = null;
const cleanup = () => { try { if (fs.readFileSync(lock, 'utf8') === String(process.pid)) fs.unlinkSync(lock); } catch {} };
process.on('exit', cleanup);
const startedAt = new Date().toISOString();
const pt = require('./scanner/papertrade').getAccount('btcy2');
const researchFeed = new ResearchFeed({ connection: () => ({ publicTradesConnected: !!pt.status().tradesConnected }) }).attach(marketData, pt);
const FG = require('./scanner/funding-guard');
FG.setCfg(safety.config.independentGuards);
const bot = require('./scanner/btcy2-lag');
// Dashboard upgrade is read-only with respect to exchange execution. Never resume legacy LIVE while guard/shadow are not integrated.
if (instruments.startBlockers().length) bot.setConfig({ live: false, enabled: false });
const { db } = require('./db');
const btcHistory = db.prepare("SELECT position_id,side,entry,exit,closed_at,real_pnl,reason FROM btcy2_lag_trades WHERE live=1 AND status='closed' AND position_id IS NOT NULL ORDER BY closed_at DESC LIMIT 12");
const btcTotals = db.prepare("SELECT COUNT(*) AS closedTrades, SUM(real_pnl) AS adjustedPnlUsd, SUM(real_pnl>0) AS wins, SUM(real_pnl<0) AS losses FROM btcy2_lag_trades WHERE live=1 AND status='closed' AND position_id IS NOT NULL");
const btcDayTotals = db.prepare("SELECT COUNT(*) AS closedTrades, COUNT(real_pnl) AS pricedTrades, SUM(real_pnl) AS realisedUsd FROM btcy2_lag_trades WHERE live=1 AND status='closed' AND position_id IS NOT NULL AND closed_at>=? AND closed_at<=?");
function status() {
  const now = Date.now(), state = bot.getState({ light: true }), provider = pt.status(), cfg = state.config;
  const marketFresh = ['hl', 'bf'].every(v => state.feeds[v].ageMs !== null && state.feeds[v].ageMs <= cfg.maxFeedAgeMs);
  const providerFresh = provider.wallet.t > 0 && now - provider.wallet.t < 120000;
  const capacity = marginCapacity(provider.wallet.available, cfg.margin);
  const blockers = [];
  if (!provider.key.active) blockers.push({ code: 'SESSION_KEY_NOT_ACTIVE', detail: 'Session key not registered/active on PaperTrade — no orders can be sent.' });
  if (!providerFresh) blockers.push({ code: 'WALLET_STATE_NOT_FRESH', detail: 'Wallet stream silent >120 s — no new entries.' });
  else if (!capacity.ready) blockers.push({ code: 'INSUFFICIENT_FULL_MARGIN', detail: `Available ${provider.wallet.available} USDC; need ${capacity.required.toFixed(2)} including unchanged 5% reserve for full ${cfg.margin} margin.` });
  if (!marketFresh) blockers.push({ code: 'MARKET_FEEDS_STALE', detail: 'HL/Binance feed older than maxFeedAgeMs.' });
  if (safety.cutoffReached(now)) blockers.push({ code: 'TRADE_UNTIL_REACHED', detail: `Trading window ended at ${safety.config.independentGuards.tradeUntilAbsolute}.` });
  if (state.guard && state.guard.tripped) blockers.push({ code: 'DAY_HIGH_WATER_TRIPPED', detail: 'Day drawdown limit hit — LIVE switched off.' });
  if (!cfg.live) blockers.push({ code: 'LIVE_OFF', detail: 'cfg.live=false (guard or kill switch).' });
  if (!state.executionSync.ready || now - state.executionSync.t > 30000) blockers.push({ code: 'EXECUTION_SYNC_NOT_READY', detail: 'Initial or current execution history reconciliation required.' });
  if (provider.entrySafety?.unknown?.length) blockers.push({ code: 'ENTRY_INTENT_UNRESOLVED', detail: 'Есть entry intent с неизвестным исходом в durable ledger. Новые входы и смена набора тикеров запрещены.' });
  if (state.candidate && (state.candidate.error || state.candidate.warmup || !state.candidate.liveInputsReady)) blockers.push({ code: 'BTC_LONG_CANDIDATE_NOT_READY', detail: state.candidate.error || state.candidate.reason });
  if (state.mainStrategy && !state.mainStrategy.ready) blockers.push({ code: 'MAIN_BTC_NOT_READY', detail: state.mainStrategy.reason });
  if (state.y2Strategy && !state.y2Strategy.ready) blockers.push({ code: 'Y2_NOT_READY', detail: state.y2Strategy.reason });
  const handoffBlockers = instruments.startBlockers();
  if (handoffBlockers.length) blockers.push({ code: 'GUARD_SHADOW_INTEGRATION_PENDING', detail: handoffBlockers.join(' ') });
  const markets = marketData.snapshot(cfg.maxFeedAgeMs), selection = instruments.snapshot();
  for (const [symbol, market] of Object.entries(markets)) {
    market.execution = instruments.context(symbol);
    market.positions = provider.wallet.openPositions.filter(p => p.instrumentId === market.instrumentId);
    market.history = symbol === 'BTC' ? btcHistory.all().map(row => ({ ...row, symbol, instrumentId: market.instrumentId })) : [];
    market.stats = symbol === 'BTC' ? { ...btcTotals.get(), source: 'Canonical closed LIVE BTC bot journal; adjusted PnL, not a second net-fee calculation' }
      : { closedTrades: 0, adjustedPnlUsd: null, wins: 0, losses: 0, source: 'ETH bot journal not yet started; account-wide/manual ETH history is not attributed to this bot' };
  }
  const selectedMarketFresh = selection.selectedSymbols.every(symbol => markets[symbol].marketFresh && markets[symbol].paperFresh);
  if (!selectedMarketFresh) blockers.push({ code: 'SELECTED_MARKET_FEEDS_STALE', detail: 'Не все выбранные BTC/ETH потоки HL, Binance Futures и Paper свежие.' });
  const liveTrading = cfg.live && cfg.enabled && provider.key.active && providerFresh;
  const hw = safety.currentHighWater();
  const moscow = new Date(now + 3 * 3600000);
  const dayStartMs = Date.UTC(moscow.getUTCFullYear(), moscow.getUTCMonth(), moscow.getUTCDate()) - 3 * 3600000;
  const day = btcDayTotals.get(dayStartMs, now);
  const realisedUsd = day.closedTrades === 0 ? 0 : day.realisedUsd;
  const pnlDay = { symbol: 'BTC', dayStartMs, realisedUsd, missingRealised: day.closedTrades - day.pricedTrades, basis: 'Canonical closed LIVE BTC trades, Europe/Moscow; adjusted realised PnL only' };
  const flat = !state.open.length && !provider.wallet.openPositions.length && !provider.wallet.operations.length;
  const displayHw = hw && flat && hw.dayKey === dayStartMs && pnlDay.missingRealised === 0 && Number.isFinite(realisedUsd) ? { ...hw, netPnl: realisedUsd, drawdown: Math.max(0, hw.best - realisedUsd) } : hw;
  const liveContext = liveSettingsContext({ LIVE: !!cfg.live, entriesEnabled: !!cfg.live && cfg.enabled, providerFresh, provider, openManaged: state.open });
  return {
    observedAt: new Date(now).toISOString(), assignment_id: safety.config.assignment_id,
    pid: process.pid, startedAt, host: safety.config.host, port: safety.config.port,
    strategy: state.liveStrategies?.length ? state.liveStrategies.map(runtime => runtime.label).join(' + ') : state.candidate ? 'BTC LONG · quiet v1 · спред и стаканы' : 'Vchera2 / btcy2-lag (спред + лимитки), isolated app',
    candidate: state.candidate,
    mainStrategy: state.mainStrategy,
    y2Strategy: state.y2Strategy,
    liveStrategies: state.liveStrategies,
    mode: safety.config.mode, LIVE: !!cfg.live, entriesEnabled: !!cfg.enabled, exchangeMutationsEnabled: liveTrading,
    technicalReady: selectedMarketFresh && providerFresh, marketFresh: selectedMarketFresh, legacyBtcMarketFresh: marketFresh, providerFresh,
    authorizationReady: provider.key.active, liveReady: liveTrading && blockers.length === 0, specReady: true,
    fullMarginReady: providerFresh && capacity.ready, requiredForFullMargin: capacity.required, marginShortfall: capacity.shortfall,
    liveExecutionIntegrated: true,
    actualWallet: safety.config.wallet,
    maximumAuthorizedNotionalUsd: safety.config.independentGuards.maximumAggregateNotionalUsd,
    config: cfg, protectionSpecification: safety.config.independentGuards,
    liveSession: state.liveSession,
    funding: { settings: FG.getCfg(), noEntryReason: FG.noEntry(now), mustClose: FG.mustClose(now) },
    walls: safety.walls.state(now),
    highWater: { armed: true, limitUsd: safety.config.independentGuards.dayHighWaterDrawdownUsd, state: displayHw, dayPnl: state.guard ? state.guard.dayPnl : null, displayOnlyReconciliation: displayHw !== hw },
    timer: { armed: true, absoluteCutoff: safety.config.independentGuards.tradeUntilAbsolute, reached: safety.cutoffReached(now) },
    blockers, provider: { ...provider, wallet: { ...provider.wallet, address: safety.config.wallet } },
    feeds: state.feeds, regime: state.regime, oi: state.oi,
    openManaged: state.open, decisions: state.decisions, events: (state.events || []).slice(0, 30), guard: state.guard,
    executionSync: state.executionSync,
    markets, instrumentSelection: selection,
    pnlDay,
    researchFeed: researchFeed.status(),
    marketTransport: transportStatus(),
    researchRuns: readResearchStatus(now),
    liveSettings: liveSettings.snapshot(liveContext, now),
    shadowProcess: { ...shadowRuntime.status(), startError: shadowStartError },
  };
}
let controlBusy = false;
const server = http.createServer((req, res) => {
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'");
  if (req.method === 'POST' && req.url === '/api/live/settings') {
    if (!authorizedControl(req, false)) { res.writeHead(403); return res.end(JSON.stringify({ ok: false, message: 'Same-origin loopback control required.' })); }
    let body = '', size = 0, tooLarge = false;
    req.on('data', chunk => { size += chunk.length; if (size > MAX_LIVE_SETTINGS_BODY) tooLarge = true; else body += chunk.toString('utf8'); });
    req.on('error', () => {});
    req.on('end', () => {
      if (controlBusy) { res.writeHead(409); return res.end(JSON.stringify({ ok: false, reasons: ['CONTROL_BUSY'] })); }
      controlBusy = true;
      try {
        if (tooLarge) { res.writeHead(413); return res.end(JSON.stringify({ ok: false, reasons: ['SETTINGS_BODY_TOO_LARGE'] })); }
        let parsed; try { parsed = JSON.parse(body); } catch { res.writeHead(400); return res.end(JSON.stringify({ ok: false, reasons: ['INVALID_JSON'] })); }
        const result = liveSettings.update(parsed, liveSettingsContext(status()), Date.now());
        res.writeHead(result.status || (result.ok ? 200 : 409)); res.end(JSON.stringify(result));
      } catch { res.writeHead(503); res.end(JSON.stringify({ ok: false, reasons: ['SETTINGS_UNAVAILABLE'], actualLiveActivation: false })); }
      finally { controlBusy = false; }
    });
    return;
  }
  if (req.method === 'POST' && req.url === '/api/shadow/strategies') {
    if (!authorizedControl(req, false)) { res.writeHead(403); return res.end(JSON.stringify({ ok: false, message: 'Same-origin loopback control required.' })); }
    let body = '', size = 0;
    req.on('data', chunk => { size += chunk.length; if (size <= 4096) body += chunk.toString('utf8'); });
    req.on('error', () => {});
    req.on('end', async () => {
      if (size > 4096) { res.writeHead(413); return res.end(JSON.stringify({ ok: false, message: 'STRATEGY_BODY_TOO_LARGE' })); }
      let definition; try { definition = JSON.parse(body); require('../papertrade-shadow/strategy-registry.cjs').validateDefinition(definition); }
      catch (error) { res.writeHead(400); return res.end(JSON.stringify({ ok: false, message: error.message })); }
      try { const result = await shadowRuntime.addStrategy(definition); clearHistoryCache(); res.end(JSON.stringify(result)); }
      catch (error) { res.writeHead(409); res.end(JSON.stringify({ ok: false, message: error.message, mode: 'SIM' })); }
    });
    return;
  }
  if (req.method === 'POST' && req.url === '/api/instruments/selection') {
    if (!authorizedControl(req, false)) { res.writeHead(403); return res.end(JSON.stringify({ ok: false, message: 'Same-origin loopback control required.' })); }
    if (controlBusy) { res.writeHead(409); return res.end(JSON.stringify({ ok: false, message: 'Другая команда ещё выполняется.' })); }
    let body = '', size = 0, tooLarge = false;
    req.on('data', chunk => { size += chunk.length; if (size > 1024) tooLarge = true; else body += chunk.toString('utf8'); });
    req.on('error', () => {});
    req.on('end', () => {
      if (controlBusy) { res.writeHead(409); res.end(JSON.stringify({ ok: false, message: 'Другая команда ещё выполняется.' })); return; }
      controlBusy = true;
      try {
        if (tooLarge) { res.writeHead(413); res.end(JSON.stringify({ ok: false, message: 'Request too large' })); return; }
        let parsed; try { parsed = JSON.parse(body); } catch { res.writeHead(400); res.end(JSON.stringify({ ok: false, message: 'Invalid JSON' })); return; }
        const result = instruments.change(parsed, status());
        if (!result.ok) res.writeHead(result.status || 409);
        res.end(JSON.stringify(result));
      } catch { res.writeHead(503); res.end(JSON.stringify({ ok: false, message: 'Выбор не сохранён; LIVE остаётся выключенным.' })); }
      finally { controlBusy = false; }
    });
    return;
  }
  if (req.method === 'POST' && ['/api/kill', '/api/trading/start', '/api/trading/stop'].includes(req.url)) {
    if (!authorizedControl(req, req.url === '/api/kill')) { res.writeHead(403); return res.end(JSON.stringify({ ok: false, message: 'Control requires same-origin loopback request and explicit header.' })); }
    if (controlBusy) { res.writeHead(409); return res.end(JSON.stringify({ ok: false, message: 'Уже выполняется другая команда. Проверьте состояние.' })); }
    controlBusy = true;
    const starting = req.url === '/api/trading/start';
    if (starting && instruments.startBlockers().length) { controlBusy = false; res.writeHead(409); return res.end(JSON.stringify({ ok: false, reasons: instruments.startBlockers() })); }
    Promise.resolve().then(() => starting ? bot.startTrading() : bot.killSwitch()).then(result => {
      if (result.ok === false) { res.writeHead(409); res.end(JSON.stringify(result)); }
      else res.end(JSON.stringify({ ok: true, result, message: starting ? 'LIVE включён с прежними защитами.' : 'LIVE выключен. Свои заявки снимаются, позиции закрываются; панель остаётся работать.' }));
    }).catch(() => { res.writeHead(500); res.end(JSON.stringify({ ok: false, message: 'Команда не подтверждена. Проверьте актуальное состояние.' })); }).finally(() => { controlBusy = false; });
    return;
  }
  if (req.method !== 'GET') { res.writeHead(405); return res.end(JSON.stringify({ error: 'UNSUPPORTED_CONTROL' })); }
  if (req.url.startsWith('/api/shadow/decision') || req.url === '/api/shadow/diagnostics') {
    try {
      if (req.url === '/api/shadow/diagnostics') return res.end(JSON.stringify(diagnosticSummary()));
      const url = new URL(req.url, 'http://127.0.0.1'), id = url.searchParams.get('id') || '';
      if (url.pathname !== '/api/shadow/decision' || [...url.searchParams.keys()].some(key => key !== 'id') || !/^\d+$/.test(id)) throw new Error('INVALID_DECISION_ID');
      return res.end(JSON.stringify(decisionDetail(Number(id))));
    } catch { res.writeHead(400); return res.end(JSON.stringify({ mode: 'SIM', error: 'DECISION_DETAILS_UNAVAILABLE' })); }
  }
  if (req.url.startsWith('/api/shadow/history')) {
    try {
      const url = new URL(req.url, 'http://127.0.0.1');
      if (url.pathname !== '/api/shadow/history' || [...url.searchParams.keys()].some(key => key !== 'limit')) throw new Error('Invalid history request');
      const raw = url.searchParams.get('limit') ?? '40'; if (!/^\d+$/.test(raw)) throw new Error('Invalid limit');
      return res.end(JSON.stringify({ ...historyView(Number(raw)), process: shadowRuntime.status() }));
    } catch { res.writeHead(503); return res.end(JSON.stringify({ mode: 'SIM', status: 'HISTORY_NOT_READY', trades: [], positions: [], intents: [], decisions: [] })); }
  }
  if (req.url.startsWith('/api/research/events')) {
    try {
      const url = new URL(req.url, 'http://127.0.0.1');
      if (url.pathname !== '/api/research/events' || [...url.searchParams.keys()].some(key => !['after', 'limit'].includes(key))) throw new Error('Invalid research request');
      const after = url.searchParams.get('after') ?? '0', limit = url.searchParams.get('limit') ?? '128';
      if (!/^\d+$/.test(after) || !/^\d+$/.test(limit)) throw new Error('Invalid cursor');
      return res.end(JSON.stringify(researchFeed.read(Number(after), Number(limit))));
    } catch { res.writeHead(400); return res.end(JSON.stringify({ error: 'INVALID_RESEARCH_CURSOR', mode: 'READ_ONLY_RESEARCH_NO_ORDERS' })); }
  }
  const staticFiles = { '/': ['dashboard.html', 'text/html; charset=utf-8'], '/dashboard.js': ['dashboard.js', 'text/javascript; charset=utf-8'], '/instrument-dashboard.js': ['instrument-dashboard.js', 'text/javascript; charset=utf-8'], '/live-settings.js': ['live-settings.js', 'text/javascript; charset=utf-8'], '/multi-strategy.js': ['multi-strategy.js', 'text/javascript; charset=utf-8'], '/dashboard.css': ['dashboard.css', 'text/css; charset=utf-8'] };
  if (Object.hasOwn(staticFiles, req.url)) {
    const [file, contentType] = staticFiles[req.url];
    try { res.setHeader('Content-Type', contentType); return res.end(fs.readFileSync(path.join(__dirname, file))); }
    catch { res.writeHead(503); return res.end('Dashboard unavailable'); }
  }
  if (!['/health', '/state', '/api/markets', '/api/research/status', '/api/live/settings', '/api/live/schedule-plan', '/api/btcy2lag/state', '/api/btcy2lag/config'].includes(req.url)) { res.writeHead(404); return res.end('{}'); }
  try {
    const current = status();
    if (req.url === '/health') res.end(JSON.stringify({ ok: true, ...Object.fromEntries(['pid', 'mode', 'LIVE', 'entriesEnabled', 'exchangeMutationsEnabled', 'technicalReady', 'liveReady', 'actualWallet', 'observedAt'].map(k => [k, current[k]])) }));
    else if (req.url === '/api/markets') res.end(JSON.stringify({ observedAt: current.observedAt, instrumentSelection: current.instrumentSelection, markets: current.markets }));
    else if (req.url === '/api/research/status') res.end(JSON.stringify(current.researchRuns));
    else if (req.url === '/api/live/settings') res.end(JSON.stringify(current.liveSettings));
    else if (req.url === '/api/live/schedule-plan') res.end(JSON.stringify(makeSchedulerPlan(current.liveSettings, Date.now())));
    else if (req.url.endsWith('/config')) res.end(JSON.stringify({ ...current.config, independentGuards: current.protectionSpecification }));
    else res.end(JSON.stringify(current));
  } catch { res.writeHead(503); res.end(JSON.stringify({ error: 'STATE_UNAVAILABLE', LIVE: false })); }
});
server.on('error', () => { console.error('Listener unavailable'); process.exit(1); });
server.listen(safety.config.port, safety.config.host, () => {
  atomicPrivateJSON(path.join(root, 'process.json'), { pid: process.pid, startedAt, host: safety.config.host, port: safety.config.port, mode: safety.config.mode });
  bot.start();
  try { shadowRuntime.start(); } catch (error) { shadowStartError = error.message; console.error('SHADOW_START_BLOCKED:', error.message); }
  console.log(`VCHERA2_LISTENING ${safety.config.host}:${safety.config.port}; mode=${safety.config.mode}`);
});
// Stop = stop process only; live positions stay on the exchange and are re-adopted on restart. Use /api/kill to flatten.
for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, async () => { await shadowRuntime.stop(); server.close(); process.exit(0); });
