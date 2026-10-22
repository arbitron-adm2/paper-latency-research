'use strict';
// ETH basket ("grid") SIM after wallet 0x6ec3…558e, strategy B of
// ~/Projects/_tmp/paper-wallet-strategies-20261020/wallet-strategies.md. SIM ONLY:
// reads the existing loopback research feed, never signs, never sends orders, no external sockets.
// Execution follows the "vlad" preset: entry delay from Vlad queue_samples pool, exit delay by
// hold-time bucket, fill at HL mid at execution time. Money via apps/papertrade-shadow/money.cjs.
const fs = require('node:fs'), path = require('node:path');
const { DatabaseSync } = require('node:sqlite');
const { settle, UNIT } = require('../papertrade-shadow/money.cjs');
const ROOT = path.resolve(__dirname, '../..');
const PRESET = JSON.parse(fs.readFileSync(path.join(ROOT, 'apps/papertrade-shadow/queue-pools/vlad-preset.json'), 'utf8'));
const PARAMS = JSON.parse(fs.readFileSync(path.join(ROOT, 'apps/papertrade-shadow/native-parameters.json'), 'utf8'));
const ETH = PARAMS.instruments.find(r => r[1] === 'ETH');
const SCALE = 100n; // ETH price scale on Paper
const FEED = 'http://127.0.0.1:18762/api/research/events';

// Frozen before the independent window (AGENTS.md). Values from the pC specification of strategy B.
const RULES = Object.freeze({ fadeMoveUsd: 0.8, cushionMaxUsd: -0.1, basketCooldownMs: 120000, addIntervalMs: 24000,
  addMaxAgainstUsd: 0.9, exitProfitUsd: 0.7, exitBf30Usd: 0.7, exitMaxHoldMs: 300000, exitStopUsd: 1.0, warmupMs: 130000, quoteMaxAgeMs: 1000 });
const MODELS = Object.freeze([
  { id: 'eth_grid_4x500_500x', label: 'ETH сетка 0x6ec3 · 4×500$ · 500×', margin: 500, leverage: 500, maxPositions: 4 },
  { id: 'eth_grid_4x250_500x', label: 'ETH сетка 0x6ec3 · 4×250$ · 500×', margin: 250, leverage: 500, maxPositions: 4 },
  { id: 'eth_single_500_500x', label: 'ETH первый вход без доборов · 500$ · 500× (контроль)', margin: 500, leverage: 500, maxPositions: 1 },
]);
const CAPITAL = 2300n * UNIT;
const MONEY = { payoutMode: 'reconstructed-deadband-haircut', instrumentRow: ETH, winFeeRaw: PARAMS.winFeeRaw, fundingBps: 0 };

class Rng { constructor(seed) { this.s = seed >>> 0; } next() { this.s = (this.s * 1664525 + 1013904223) >>> 0; return this.s / 4294967296; } }
const pick = (rng, a) => a[Math.floor(rng.next() * a.length)];
const toRaw = px => BigInt(Math.round(px * Number(SCALE)));

class Tape {
  constructor() { this.v = { hl: [], bf: [] }; this.sec = []; this.lastSec = 0; }
  push(venue, t, mid) { const a = this.v[venue]; if (!a) return; a.push([t, mid]); while (a.length && a[0][0] < t - 320000) a.shift(); }
  at(venue, t, maxAge = RULES.quoteMaxAgeMs) {
    const a = this.v[venue]; let lo = 0, hi = a.length - 1, best = -1;
    while (lo <= hi) { const m = (lo + hi) >> 1; if (a[m][0] <= t) { best = m; lo = m + 1; } else hi = m - 1; }
    return best >= 0 && t - a[best][0] <= maxAge ? a[best][1] : null;
  }
  second(t) { // one (hl-bf) sample per second for the 120 s median, like pC median_diff
    const s = Math.floor(t / 1000); if (s === this.lastSec) return; this.lastSec = s;
    const h = this.at('hl', t), b = this.at('bf', t); if (h == null || b == null) return;
    this.sec.push([t, h - b]); while (this.sec.length && this.sec[0][0] < t - 120000) this.sec.shift();
  }
  median120() { if (this.sec.length <= 10) return null; const v = this.sec.map(x => x[1]).sort((x, y) => x - y); return v[v.length >> 1]; }
}

class BasketModel {
  constructor(cfg, db, seed) {
    Object.assign(this, { cfg, db, rng: new Rng(seed), cash: CAPITAL, reserved: 0n, basket: null, pending: [], closing: null, lastBasketEnd: 0,
      counts: { baskets: 0, entries: 0, adds: 0, cancelled: 0, lateFills: 0, closedBaskets: 0, liquidations: 0, skippedNoCapital: 0, priceRetries: 0, gapsDuringExposure: 0 } });
    const saved = db.prepare('SELECT state_json FROM basket_models WHERE model_id=?').get(cfg.id);
    if (saved) { const s = JSON.parse(saved.state_json); this.cash = BigInt(s.cash); this.counts = { ...this.counts, ...s.counts }; this.lastBasketEnd = s.lastBasketEnd || 0; }
  }
  margin() { return BigInt(this.cfg.margin) * UNIT; }
  available() { return this.cash - this.reserved; }
  note(t, kind, data = {}) { this.db.prepare('INSERT INTO basket_events(model_id,t_ms,kind,payload_json) VALUES(?,?,?,?)').run(this.cfg.id, t, kind, JSON.stringify(data)); }
  submit(t, side, kind) {
    if (this.available() < this.margin()) { this.counts.skippedNoCapital++; return; }
    this.reserved += this.margin();
    const delay = pick(this.rng, PRESET.entryMs);
    this.pending.push({ id: `${this.cfg.id}-${t}-${this.pending.length}`, side, kind, createdAt: t, fillAt: t + delay, firstMiss: null });
    this.note(t, 'entry_submitted', { side, kind, delayMs: delay });
  }
  onSignal(t, side) {
    if (this.basket || this.closing || this.pending.length) return;
    if (t - this.lastBasketEnd < RULES.basketCooldownMs) return;
    this.basket = { side, startedAt: t, positions: [], firstEntry: null, lastAddAt: t, id: `${this.cfg.id}-b${t}` };
    this.counts.baskets++; this.submit(t, side, 'first'); this.note(t, 'basket_open', { side });
  }
  avgEntry() { const p = this.basket.positions.filter(x => !x.closed); return p.length ? p.reduce((s, x) => s + x.entryPx, 0) / p.length : null; }
  step(t, tape, gapFlag) {
    const hl = tape.at('hl', t), bfNow = tape.at('bf', t);
    if (gapFlag && (this.basket || this.pending.length)) this.counts.gapsDuringExposure++;
    // fills
    for (const q of [...this.pending]) {
      if (t < q.fillAt) continue;
      const px = tape.at('hl', t, 30000); // fallback age as in the preset
      if (px == null) { q.firstMiss ??= t; if (t - q.firstMiss < PRESET.exitPriceRetryMs) { this.counts.priceRetries++; continue; } this.reserved -= this.margin(); this.pending.splice(this.pending.indexOf(q), 1); this.note(t, 'entry_unknown_released', { id: q.id }); continue; }
      this.pending.splice(this.pending.indexOf(q), 1);
      if (q.cancelled) { this.reserved -= this.margin(); continue; }
      const late = !!q.cancelRequested; if (late) this.counts.lateFills++;
      if (!this.basket) { // late fill after basket closed: close at once
        this.reserved -= this.margin(); this.cash -= 0n; this.note(t, 'late_fill_without_basket', { id: q.id, px }); continue;
      }
      const pos = { id: q.id, side: q.side, entryPx: px, entryRaw: toRaw(px), openedAt: t, closed: false, late };
      this.basket.positions.push(pos); this.basket.firstEntry ??= px; this.basket.lastAddAt = t;
      q.kind === 'first' ? this.counts.entries++ : this.counts.adds++;
      this.note(t, 'entry_fill', { id: q.id, kind: q.kind, px, late });
    }
    if (!this.basket || hl == null) return this.closingStep(t, tape);
    const d = this.basket.side === 'L' ? 1 : -1;
    // per-position liquidation (engine formula with bustBufferRaw)
    for (const p of this.basket.positions) {
      if (p.closed) continue;
      const buffer = BigInt(ETH[16]), lev = BigInt(this.cfg.leverage), e = p.entryRaw, px = toRaw(hl);
      const thr = p.side === 'L' ? e * (lev - 1n) * UNIT / (lev * (UNIT - buffer)) : e * (lev + 1n) * UNIT / (lev * (UNIT + buffer));
      if (p.side === 'L' ? px <= thr : px >= thr) { this.settlePosition(t, p, px, true, 'liquidation'); }
    }
    const open = this.basket.positions.filter(p => !p.closed);
    if (!this.closing && open.length) {
      const avg = this.avgEntry(), first = this.basket.firstEntry, bf30 = tape.at('bf', t - 30000);
      const favor = d * (hl - avg), against1 = d * (first - hl), held = t - this.basket.positions[0].openedAt;
      const bfFavor = bf30 != null && bfNow != null ? d * (bfNow - bf30) : null;
      let why = null;
      if (favor >= RULES.exitProfitUsd && bfFavor != null && bfFavor >= RULES.exitBf30Usd) why = 'profit+binance30';
      else if (held >= RULES.exitMaxHoldMs) why = 'max_hold_300s';
      else if (-favor >= RULES.exitStopUsd) why = 'stop_avg_minus_1';
      if (why) return this.requestClose(t, why);
      // adds on pullback against the first entry, bounded
      if (open.length + this.pending.length < this.cfg.maxPositions && t - this.basket.lastAddAt >= RULES.addIntervalMs && against1 > 0 && against1 <= RULES.addMaxAgainstUsd) {
        this.basket.lastAddAt = t; this.submit(t, this.basket.side, 'add');
      }
    } else if (!open.length && !this.pending.length && !this.closing) { this.endBasket(t, 'empty'); }
    this.closingStep(t, tape);
  }
  requestClose(t, why) {
    const held = t - this.basket.positions[0].openedAt;
    const b = PRESET.exitBuckets.find(x => x.maxHoldMs == null || held < x.maxHoldMs) ?? PRESET.exitBuckets.at(-1);
    this.closing = { at: t, execAt: t + b.medianMs, why, firstMiss: null };
    for (const q of this.pending) { if (q.fillAt - t >= PRESET.cancelMinLeadMs) { q.cancelled = true; this.counts.cancelled++; } else q.cancelRequested = true; }
    this.note(t, 'close_requested', { why, execDelayMs: b.medianMs });
  }
  closingStep(t, tape) {
    if (!this.closing || t < this.closing.execAt) return;
    const px = tape.at('hl', t, 30000);
    if (px == null) { this.closing.firstMiss ??= t; if (t - this.closing.firstMiss < PRESET.exitPriceRetryMs) { this.counts.priceRetries++; return; } }
    const price = px ?? this.basket.positions.find(p => !p.closed)?.entryPx;
    for (const p of this.basket.positions) if (!p.closed) this.settlePosition(t, p, toRaw(price), false, this.closing.why + (px == null ? '|price_unknown_flat' : ''));
    if (this.pending.some(q => !q.cancelled)) { this.closing = { ...this.closing, execAt: t + 1000 }; return; } // late fills get closed on the next pass
    this.endBasket(t, this.closing.why);
  }
  settlePosition(t, p, exitRaw, liquidated, reason) {
    const m = this.margin(), r = settle({ margin: m, leverage: this.cfg.leverage, entry: p.entryRaw, exit: exitRaw, long: p.side === 'L', liquidated }, MONEY);
    p.closed = true; this.reserved -= m; this.cash += r.cash; if (liquidated) this.counts.liquidations++;
    this.db.prepare('INSERT INTO basket_positions(model_id,basket_id,position_id,side,opened_ms,closed_ms,entry_px,exit_px,margin,leverage,cash_raw,liquidated,late,reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
      .run(this.cfg.id, this.basket.id, p.id, p.side, p.openedAt, t, p.entryPx, Number(exitRaw) / Number(SCALE), this.cfg.margin, this.cfg.leverage, r.cash.toString(), liquidated ? 1 : 0, p.late ? 1 : 0, reason);
  }
  endBasket(t, why) {
    const b = this.basket, ids = b.positions.map(p => p.id);
    const cash = ids.length ? this.db.prepare(`SELECT COALESCE(SUM(CAST(cash_raw AS REAL)),0) c FROM basket_positions WHERE basket_id=?`).get(b.id).c / 1e18 : 0;
    this.db.prepare('INSERT INTO basket_baskets(model_id,basket_id,side,started_ms,ended_ms,positions,cash_usdc,reason) VALUES(?,?,?,?,?,?,?,?)').run(this.cfg.id, b.id, b.side, b.startedAt, t, ids.length, cash, why);
    this.counts.closedBaskets++; this.basket = null; this.closing = null; this.lastBasketEnd = t; this.note(t, 'basket_closed', { why, positions: ids.length, cash });
  }
  save() { this.db.prepare('INSERT INTO basket_models(model_id,state_json) VALUES(?,?) ON CONFLICT(model_id) DO UPDATE SET state_json=excluded.state_json').run(this.cfg.id, JSON.stringify({ cash: this.cash.toString(), counts: this.counts, lastBasketEnd: this.lastBasketEnd })); }
  summary() { return { id: this.cfg.id, label: this.cfg.label, cashUsdc: Number(this.cash) / 1e18, netUsdc: Number(this.cash - CAPITAL) / 1e18, openBasket: !!this.basket, pending: this.pending.length, counts: this.counts }; }
}

function openDb(dir) {
  fs.mkdirSync(dir, { recursive: true });
  const db = new DatabaseSync(path.join(dir, 'basket.sqlite'));
  db.exec(`PRAGMA journal_mode=WAL;CREATE TABLE IF NOT EXISTS basket_models(model_id TEXT PRIMARY KEY,state_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS basket_events(id INTEGER PRIMARY KEY,model_id TEXT,t_ms REAL,kind TEXT,payload_json TEXT);
CREATE TABLE IF NOT EXISTS basket_positions(model_id TEXT,basket_id TEXT,position_id TEXT,side TEXT,opened_ms REAL,closed_ms REAL,entry_px REAL,exit_px REAL,margin REAL,leverage INTEGER,cash_raw TEXT,liquidated INTEGER,late INTEGER,reason TEXT);
CREATE TABLE IF NOT EXISTS basket_baskets(model_id TEXT,basket_id TEXT PRIMARY KEY,side TEXT,started_ms REAL,ended_ms REAL,positions INTEGER,cash_usdc REAL,reason TEXT);
CREATE TABLE IF NOT EXISTS basket_gaps(t_ms REAL,reason TEXT);`);
  return db;
}

async function run({ dir, seed = 20261020 }) {
  const db = openDb(dir), tape = new Tape(), models = MODELS.map((c, i) => new BasketModel(c, db, seed + i));
  let cursor = null, boot = null, startedAt = null, lastEval = 0, lastSave = 0, stop = false, events = 0, gaps = 0;
  for (const s of ['SIGTERM', 'SIGINT']) process.on(s, () => { stop = true; });
  const status = t => fs.writeFileSync(path.join(dir, 'status.json.tmp'), JSON.stringify({ mode: 'SIM_ONLY_NO_ORDERS', updatedAtMs: Date.now(), clockMs: t, events, gaps, warm: startedAt != null && t - startedAt >= RULES.warmupMs, rules: RULES, models: models.map(m => m.summary()) }, null, 1)) || fs.renameSync(path.join(dir, 'status.json.tmp'), path.join(dir, 'status.json'));
  while (!stop) {
    let body;
    try {
      const url = cursor == null ? `${FEED}?limit=1` : `${FEED}?after=${cursor}&limit=256`;
      const r = await fetch(url, { signal: AbortSignal.timeout(5000) }); if (!r.ok) throw new Error('feed HTTP ' + r.status); body = await r.json();
      if (typeof body.bootId !== 'string' || !Array.isArray(body.events)) throw new Error('feed envelope');
    } catch (e) { await new Promise(r => setTimeout(r, 1000)); continue; }
    if (cursor == null || body.bootId !== boot) { const fresh = cursor != null; boot = body.bootId; cursor = body.lastSequence; if (fresh) { gaps++; db.prepare('INSERT INTO basket_gaps VALUES(?,?)').run(Date.now(), 'boot_changed'); } continue; }
    let gapFlag = false;
    if (body.gap === true) { gaps++; gapFlag = true; db.prepare('INSERT INTO basket_gaps VALUES(?,?)').run(Date.now(), 'history_gap'); if (body.events.length === 0) cursor = Math.max(cursor, (body.firstSequence ?? cursor + 1) - 1); }
    for (const e of body.events || []) {
      if (e.sequence <= cursor) continue; cursor = e.sequence; events++;
      if (e.kind !== 'market' || e.symbol !== 'ETH' || !['hl', 'bf'].includes(e.venue) || e.payload?.frameKind !== 'quote') continue;
      const bid = Number(e.payload.bid), ask = Number(e.payload.ask), t = e.recv_ms; if (!(bid > 0 && ask >= bid)) continue;
      if (e.venue === 'hl' && ask - bid > 0.2) continue; // HL spread guard ($0.2 on ETH)
      tape.push(e.venue, t, (bid + ask) / 2); tape.second(t); startedAt ??= t;
      if (t - lastEval < 100) continue; lastEval = t;
      const warm = t - startedAt >= RULES.warmupMs;
      if (warm) {
        const b0 = tape.at('bf', t), b10 = tape.at('bf', t - 10000), h0 = tape.at('hl', t), med = tape.median120();
        if (b0 != null && b10 != null && h0 != null && med != null) {
          const mv = b0 - b10;
          if (Math.abs(mv) >= RULES.fadeMoveUsd) { const d = mv > 0 ? -1 : 1, cushion = d * ((b0 - h0) + med); if (cushion <= RULES.cushionMaxUsd) for (const m of models) m.onSignal(t, d > 0 ? 'L' : 'S'); }
        }
      }
      for (const m of models) m.step(t, tape, gapFlag); gapFlag = false;
      if (t - lastSave >= 5000) { lastSave = t; for (const m of models) m.save(); status(t); }
    }
    if (!(body.events || []).length) await new Promise(r => setTimeout(r, 200));
  }
  for (const m of models) m.save(); status(Date.now()); db.close();
}

if (require.main === module) {
  const [cmd, ...rest] = process.argv.slice(2), o = {};
  for (let i = 0; i < rest.length; i += 2) o[rest[i].replace(/^--/, '')] = rest[i + 1];
  const dir = path.resolve(o['state-dir'] || path.join(ROOT, 'reports/basket-sim-20261020'));
  if (cmd === 'start') run({ dir }).catch(e => { console.error(e); process.exit(1); });
  else if (cmd === 'status') process.stdout.write(fs.readFileSync(path.join(dir, 'status.json'), 'utf8') + '\n');
  else { console.error('usage: basket-sim.cjs start|status [--state-dir DIR]'); process.exit(2); }
}
module.exports = { RULES, MODELS, Tape, BasketModel, openDb };
