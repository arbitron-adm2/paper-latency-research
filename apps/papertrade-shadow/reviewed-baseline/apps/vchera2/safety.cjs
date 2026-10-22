'use strict';
const fs = require('node:fs');
const path = require('node:path');
const config = require('./launch-config.json');
const { privateRuntime, atomicPrivateJSON } = require('./runtime.cjs');

const G = config.independentGuards;
const HW_FILE = () => path.join(privateRuntime(), 'high-water.json');

function assertLaunchConfig(c) {
  if (c.host !== '127.0.0.1') throw new Error('Listener must stay on loopback');
  if (!['LIVE', 'PAPER'].includes(c.mode)) throw new Error('Unknown mode');
  const s = c.strategy;
  if (s.margin !== 2100 || s.leverage !== 600 || s.maxPositions !== 1 || s.splitParts !== 1 || s.noLongs || s.sizeToOi || s.useMainKey) throw new Error('Unapproved exposure/config');
  if (s.maxDayLossUsd !== 0) throw new Error('Absolute day-loss guard is NOT the requested high-water guard');
  if (s.autoTune || s.refEnabled || s.tgEnabled) throw new Error('Automation not requested');
  const g = c.independentGuards;
  if (!(g.dayHighWaterDrawdownUsd > 0) || !g.dayHighWaterArmed) throw new Error('High-water guard must be armed');
  if (absoluteCutoff(g.tradeUntilAbsolute, 0).reason === 'STOP_TIME_TIMEZONE_UNRESOLVED') throw new Error('Cutoff must be absolute with timezone');
  return true;
}

function absoluteCutoff(value, now) {
  if (typeof value !== 'string' || !/(Z|[+-]\d{2}:\d{2})$/.test(value) || !Number.isFinite(Date.parse(value))) return { blocked: true, reason: 'STOP_TIME_TIMEZONE_UNRESOLVED' };
  return { blocked: now >= Date.parse(value), reason: now >= Date.parse(value) ? 'TRADE_UNTIL_REACHED' : null };
}
const cutoffReached = (now = Date.now()) => absoluteCutoff(G.tradeUntilAbsolute, now).blocked;
function tradingWindowReason(now = Date.now()) {
  return cutoffReached(now) ? `таймер: торговать до ${G.tradeUntilAbsolute} — время вышло` : null;
}

// Pure step: best point of the day latches; drawdown from it >= limit trips and stays tripped for that day.
function highWaterStep(previous, { dayKey, netPnl, limit = G.dayHighWaterDrawdownUsd }) {
  if ((typeof dayKey !== 'string' && typeof dayKey !== 'number') || dayKey === '' || !Number.isFinite(netPnl) || !(limit > 0)) throw new Error('Explicit day and finite net PnL required');
  const same = previous && previous.dayKey === dayKey;
  const best = Math.max(same ? previous.best : 0, netPnl);
  const drawdown = best - netPnl;
  return { dayKey, netPnl, best, drawdown, limit, tripped: !!(same && previous.tripped) || drawdown >= limit, at: Date.now() };
}
let hwState = null;
function loadHighWater() {
  if (hwState) return hwState;
  try { hwState = JSON.parse(fs.readFileSync(HW_FILE(), 'utf8')); } catch { hwState = null; }
  return hwState;
}
function highWater(dayKey, netPnl) {
  const prev = loadHighWater();
  const next = highWaterStep(prev, { dayKey, netPnl });
  const changed = !prev || prev.dayKey !== next.dayKey || prev.best !== next.best || prev.tripped !== next.tripped;
  hwState = next;
  if (changed) atomicPrivateJSON(HW_FILE(), next);
  return next;
}
function highWaterTrippedToday(dayKey) { const s = loadHighWater(); return !!(s && s.dayKey === dayKey && s.tripped); }
function currentHighWater() { const s = loadHighWater(); return s ? { ...s } : null; }
function manualStopped() {
  try { return JSON.parse(fs.readFileSync(path.join(privateRuntime(), 'trading-control.json'), 'utf8')).stopped !== false; }
  catch (e) { return e.code === 'ENOENT' ? false : true; }
}
function setManualStop(stopped) { atomicPrivateJSON(path.join(privateRuntime(), 'trading-control.json'), { stopped: !!stopped, at: Date.now(), source: 'explicit-user-control' }); }

function lockedStrategyConfig(defaults, dayKey) {
  assertLaunchConfig(config);
  const c = { ...defaults, ...config.strategy, ptWallet: config.wallet, haircut: defaults.haircut };
  if (config.mode !== 'LIVE') c.live = false;
  if (cutoffReached()) { c.live = false; c.enabled = false; }
  if (dayKey != null && highWaterTrippedToday(dayKey)) c.live = false;
  if (manualStopped()) { c.live = false; c.enabled = false; }
  return c;
}
// Runtime may only switch itself OFF; nothing external can raise exposure or switch LIVE on.
function rejectUnsafePatch(patch) {
  for (const [k, v] of Object.entries(patch)) {
    if (!(['live', 'enabled'].includes(k) && v === false)) throw new Error('Configuration locked: only live/enabled=false allowed at runtime');
  }
}
function roundBlocked(px, step = 1000, below = 50, above = 100) {
  if (!(Number.isFinite(px) && px > 0 && step > 0)) return true;
  const level = Math.round(px / step) * step;
  const offset = px - level;
  return offset >= 0 ? offset < above : -offset < below;
}
function fullMarginAllowed({ available, openCount, pendingCount, margin = 2100, leverage = 600 }) {
  return Number.isFinite(available) && available >= margin && openCount === 0 && pendingCount === 0 && margin === 2100 && leverage === 600;
}
class WallGuard {
  constructor(settings = G) { this.settings = settings; this.until = { L: 0, S: 0, any: 0 }; this.last = []; }
  update(feeds, now = Date.now()) {
    const c = this.settings;
    const found = [];
    for (const venue of ['hl', 'bf', 'bs']) {
      const f = feeds[venue];
      if (!f || now - f.bookT > (venue === 'hl' ? 10000 : 3000)) continue;
      const mid = (f.bid + f.ask) / 2;
      if (!Number.isFinite(mid)) continue;
      // bids under the price block shorts; asks above the price block longs ("за нас — хорошо, против нас — плохо").
      for (const [book, against] of [['bids', 'S'], ['asks', 'L']]) {
        for (const [price, size] of f[book] || []) {
          const dist = Math.abs(price - mid), usd = price * size;
          if (dist > c.wallRangeUsd) continue;
          if (usd >= c.wallAgainstUsd) { this.until[against] = Math.max(this.until[against], now + c.wallCooldownMs); found.push({ venue, side: book, usd, dist, against }); }
          if (usd >= c.anyWallPauseUsd) this.until.any = Math.max(this.until.any, now + c.anyWallPauseMs);
        }
      }
    }
    this.last = found;
    return this.state(now);
  }
  reason(side, now = Date.now()) {
    return this.until.any > now ? 'лимитка от $5M у цены — пауза минуту' : this.until[side] > now ? 'стена против нас от $3M ближе $30 (ждём 60 с после исчезновения)' : null;
  }
  state(now = Date.now()) { return { observedAt: now, walls: this.last, until: { ...this.until }, blockL: this.reason('L', now), blockS: this.reason('S', now) }; }
}
const walls = new WallGuard();
module.exports = { config, assertLaunchConfig, lockedStrategyConfig, rejectUnsafePatch, roundBlocked, fullMarginAllowed, absoluteCutoff, cutoffReached, tradingWindowReason, highWaterStep, highWater, highWaterTrippedToday, WallGuard, walls, manualStopped, setManualStop, currentHighWater };
