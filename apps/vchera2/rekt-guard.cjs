'use strict';
// LIVE test watchdog (user approval 20.10 21:33 MSK: "мониторинг, остановка при ректе моментальная").
// Polls the loopback dashboard every second and calls the existing /api/kill (LIVE off, cancel own entries,
// close own positions) on any trigger. It never starts LIVE, never signs, never sends orders itself.
// A kill close still goes through the PaperTrade exit queue: this bounds the NEXT seconds, not past slippage.
const fs = require('node:fs'), path = require('node:path');
const BASE = 'http://127.0.0.1:18762';
const T = Object.freeze({
  openLossUsd: 250,          // unrealised loss of open position(s) at HL/Paper mid (~$26 BTC at 1.26M)
  bustBufferUsd: 50,         // kill when price is within $50 of the position bust price (600x bust ~ $98 from entry)
  sessionLossUsd: 300,       // realised + unrealised since guard start
  singleCloseLossUsd: 300,   // one realised drop between polls
  unknownIntentSec: 120,     // > signed deadline 19 s + 30 s grace + wallet heartbeat ~10 s      // entry intent with unknown outcome
  staleWithPositionSec: 20,  // market feeds stale while a position is open
  maxClosedTrades: 10,       // experiment cap: LIVE off after this many closed BTC positions
});
const dir = path.resolve(process.argv[2] || path.join(__dirname, '../../reports/live-test-20261020'));
fs.mkdirSync(dir, { recursive: true });
function thresholds() { try { return Object.freeze({ ...T, ...JSON.parse(fs.readFileSync(path.join(dir, 'guard-overrides.json'), 'utf8')) }); } catch { return T; } }
const log = row => fs.appendFileSync(path.join(dir, 'guard.jsonl'), JSON.stringify({ t: Date.now(), ...row }) + '\n');
let closedTrades = 0, prevIds = new Set(), baseline = null, lastRealised = null, unknownSince = null, staleSince = null, killed = null, lastSnap = 0, misses = 0;
async function kill(reason, detail) {
  if (killed) return;
  killed = { atMs: Date.now(), reason, detail };
  log({ kind: 'KILL_REQUESTED', reason, detail });
  try {
    const r = await fetch(BASE + '/api/kill', { method: 'POST', headers: { 'x-vchera2-kill': '1' }, signal: AbortSignal.timeout(8000) });
    const body = await r.text(); log({ kind: 'KILL_RESPONSE', status: r.status, body: body.slice(0, 500) });
    if (!r.ok) { killed = null; } // retry on next poll
  } catch (e) { log({ kind: 'KILL_ERROR', message: e.message }); killed = null; }
}
async function tick() {
  const T = thresholds();
  let s;
  try { const r = await fetch(BASE + '/state', { signal: AbortSignal.timeout(4000) }); s = await r.json(); misses = 0; }
  catch (e) { misses++; if (misses % 10 === 1) log({ kind: 'STATE_UNREACHABLE', misses, message: e.message }); return; }
  const now = Date.now(), w = s.provider?.wallet || {}, positions = (w.openPositions || []).filter(p => p.instrumentId === 0);
  const mid = s.markets?.BTC?.feeds?.paper?.mid ?? s.markets?.BTC?.feeds?.hl?.mid;
  let unreal = 0, minBust = Infinity;
  for (const p of positions) {
    if (!(mid > 0) || !(p.entry > 0)) continue;
    unreal += p.margin * p.leverage * (mid - p.entry) / p.entry * (p.isLong ? 1 : -1);
    if (p.bust > 0) minBust = Math.min(minBust, Math.abs(mid - p.bust));
  }
  const ids = new Set(positions.map(p => String(p.positionId)));
  for (const id of prevIds) if (!ids.has(id)) { closedTrades++; log({ kind: 'POSITION_CLOSED', id, closedTrades }); }
  prevIds = ids;
  const realised = Number(s.pnlDay?.realisedUsd ?? NaN), net = realised + unreal;
  if (baseline === null && Number.isFinite(realised)) { baseline = realised; lastRealised = realised; log({ kind: 'GUARD_START', baseline, thresholds: T, LIVE: s.LIVE }); }
  const session = Number.isFinite(net) && baseline !== null ? net - baseline : 0;
  if (now - lastSnap >= 5000) { lastSnap = now; log({ kind: 'SNAP', LIVE: s.LIVE, positions: positions.length, mid, unreal: Math.round(unreal * 100) / 100, realised, session: Math.round(session * 100) / 100, minBust: Number.isFinite(minBust) ? Math.round(minBust * 10) / 10 : null, unknown: s.provider?.entrySafety?.unknown?.length || 0 }); }
  fs.writeFileSync(path.join(dir, 'guard-status.json'), JSON.stringify({ updatedAtMs: now, LIVE: s.LIVE, positions: positions.length, mid, unreal, realised, baseline, session, minBustDistanceUsd: Number.isFinite(minBust) ? minBust : null, killed, closedTrades, thresholds: T }, null, 1));
  if (!s.LIVE && !positions.length) { unknownSince = staleSince = null; if (Number.isFinite(realised)) lastRealised = realised; return; }
  if (T.maxClosedTrades > 0 && closedTrades >= T.maxClosedTrades && s.LIVE) return kill('TRADE_CAP_REACHED', { closedTrades });
  if (positions.length && unreal <= -T.openLossUsd) return kill('OPEN_LOSS', { unreal });
  if (positions.length && minBust <= T.bustBufferUsd) return kill('NEAR_BUST', { minBust, mid });
  if (T.sessionLossUsd > 0 && session <= -T.sessionLossUsd) return kill('SESSION_LOSS', { session });
  if (Number.isFinite(realised) && lastRealised !== null && realised - lastRealised <= -T.singleCloseLossUsd) return kill('SINGLE_CLOSE_LOSS', { drop: realised - lastRealised });
  if (Number.isFinite(realised)) lastRealised = realised;
  const unknown = s.provider?.entrySafety?.unknown?.length || 0;
  unknownSince = unknown ? (unknownSince ?? now) : null;
  if (unknownSince && now - unknownSince >= T.unknownIntentSec * 1000) return kill('UNKNOWN_INTENT', { unknown });
  staleSince = positions.length && s.marketFresh === false ? (staleSince ?? now) : null;
  if (staleSince && now - staleSince >= T.staleWithPositionSec * 1000) return kill('STALE_WITH_POSITION', {});
  if ((s.events || []).some(e => e.t >= now - 5000 && /ликвид|liquidat|платформа закрыла/i.test(e.text || ''))) return kill('LIQUIDATION_EVENT', {});
}
let stop = false; for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => { stop = true; });
(async () => { log({ kind: 'GUARD_PROCESS_START', pid: process.pid }); while (!stop) { await tick().catch(e => log({ kind: 'TICK_ERROR', message: e.message })); await new Promise(r => setTimeout(r, 1000)); } log({ kind: 'GUARD_PROCESS_STOP' }); })();
