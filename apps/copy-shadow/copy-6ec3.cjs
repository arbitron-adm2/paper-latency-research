'use strict';
// SIM-only copy of public wallet 0x6ec3…558e ETH legs (reports/wallet-6ec3-proof-20261021).
// Reads the existing loopback research feed (same as basket-sim); never signs, never sends orders,
// opens no external sockets. We learn of the wallet's open/close from Paper public:trades at recv_ms,
// then fill at HL mid after a modelled queue delay. Money via apps/papertrade-shadow/money.cjs.
const fs = require('node:fs'), path = require('node:path'), { spawnSync } = require('node:child_process');
const { DatabaseSync } = require('node:sqlite');
const { settle, UNIT } = require('../papertrade-shadow/money.cjs');
const ROOT = path.resolve(__dirname, '../..');
const PRESET = JSON.parse(fs.readFileSync(path.join(ROOT, 'apps/papertrade-shadow/queue-pools/vlad-preset.json'), 'utf8'));
const PARAMS = JSON.parse(fs.readFileSync(path.join(ROOT, 'apps/papertrade-shadow/native-parameters.json'), 'utf8'));
const ETH = PARAMS.instruments.find(r => r[1] === 'ETH');
const SCALE = 100n; // ETH price scale on Paper
const FEED = 'http://127.0.0.1:18762/api/research/events';
const WALLET = '0x5555555555555555555555555555555555555555';

// Frozen before the shadow window (AGENTS.md); delays match forward_plan.json / copy_robust.py.
const RULES = Object.freeze({ quoteMaxAgeMs: 5000, hlSpreadMaxUsd: 0.2, priceRetryMs: PRESET.exitPriceRetryMs, cancelMinLeadMs: PRESET.cancelMinLeadMs,
  closeNotSeenMs: 900000, marginUsdc: 1000, capitalUsdc: 10000 });
const MODELS = Object.freeze([
  { id: 'copy_6ec3_fixed_6_10', label: 'Копия 0x6ec3 · вход +6 с / выход +10 с', entry: () => 6000, exit: () => 10000 },
  { id: 'copy_6ec3_vlad_queue', label: 'Копия 0x6ec3 · очередь Vlad preset', entry: rng => pick(rng, PRESET.entryMs), exit: (rng, held) => (PRESET.exitBuckets.find(b => b.maxHoldMs == null || held < b.maxHoldMs) ?? PRESET.exitBuckets.at(-1)).medianMs },
  { id: 'copy_6ec3_stress_12_15', label: 'Копия 0x6ec3 · стресс +12 с / +15 с', entry: () => 12000, exit: () => 15000 },
]);
const CAPITAL = BigInt(RULES.capitalUsdc) * UNIT;
const MONEY = { payoutMode: 'reconstructed-deadband-haircut', instrumentRow: ETH, winFeeRaw: PARAMS.winFeeRaw, fundingBps: 0 };
const OPEN_STATES = ['pending', 'open', 'closing'];

class Rng { constructor(seed) { this.s = seed >>> 0; } next() { this.s = (this.s * 1664525 + 1013904223) >>> 0; return this.s / 4294967296; } }
const pick = (rng, a) => a[Math.floor(rng.next() * a.length)];
const toRaw = px => BigInt(Math.round(px * Number(SCALE)));

class Hl { // HL ETH mid by receive time, last 10 min
  constructor() { this.a = []; }
  push(t, mid) { this.a.push([t, mid]); while (this.a.length && this.a[0][0] < t - 600000) this.a.shift(); }
  at(t, maxAge = RULES.quoteMaxAgeMs) {
    let lo = 0, hi = this.a.length - 1, best = -1;
    while (lo <= hi) { const m = (lo + hi) >> 1; if (this.a[m][0] <= t) { best = m; lo = m + 1; } else hi = m - 1; }
    return best >= 0 && t - this.a[best][0] <= maxAge ? this.a[best][1] : null;
  }
}

class CopyModel {
  constructor(cfg, db, seed) {
    Object.assign(this, { cfg, db, rng: new Rng(seed), cash: CAPITAL, reserved: 0n, legs: new Map(),
      counts: { walletOpens: 0, walletCloses: 0, fills: 0, closed: 0, wins: 0, losses: 0, cancelled: 0, lateFills: 0, liquidations: 0,
        skippedNoCapital: 0, unknownEntry: 0, unknownExit: 0, closeNotObserved: 0, gapsDuringExposure: 0, censoredRestart: 0 } });
    const saved = db.prepare('SELECT state_json FROM copy_models WHERE model_id=?').get(cfg.id);
    if (saved) { const s = JSON.parse(saved.state_json); this.cash = BigInt(s.cash); this.counts = { ...this.counts, ...s.counts }; }
    // Exposure across a restart is uncertain (missed closes/prices): censor it, never invent an outcome.
    for (const r of db.prepare(`SELECT leg_id FROM copy_legs WHERE model_id=? AND status IN ('pending','open','closing')`).all(cfg.id)) {
      db.prepare(`UPDATE copy_legs SET status='censored_restart', reason='service restart with exposure' WHERE model_id=? AND leg_id=?`).run(cfg.id, r.leg_id);
      this.counts.censoredRestart++;
    }
  }
  margin() { return BigInt(RULES.marginUsdc) * UNIT; }
  available() { return this.cash - this.reserved; }
  note(t, kind, data = {}) { this.db.prepare('INSERT INTO copy_events(model_id,t_ms,kind,payload_json) VALUES(?,?,?,?)').run(this.cfg.id, t, kind, JSON.stringify(data)); }
  persist(l) {
    this.db.prepare(`INSERT INTO copy_legs(model_id,leg_id,wallet_position_id,side,leverage,margin,status,wallet_open_ms,seen_open_ms,fill_at_ms,filled_ms,entry_px,
      wallet_close_ms,seen_close_ms,exit_at_ms,closed_ms,exit_px,cash_raw,reason,late,gap) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(model_id,leg_id) DO UPDATE SET status=excluded.status,filled_ms=excluded.filled_ms,entry_px=excluded.entry_px,wallet_close_ms=excluded.wallet_close_ms,
      seen_close_ms=excluded.seen_close_ms,exit_at_ms=excluded.exit_at_ms,closed_ms=excluded.closed_ms,exit_px=excluded.exit_px,cash_raw=excluded.cash_raw,
      reason=excluded.reason,late=excluded.late,gap=excluded.gap`)
      .run(this.cfg.id, l.id, l.pid, l.side, l.lev, RULES.marginUsdc, l.status, l.walletOpenMs, l.seenOpenMs, l.fillAt, l.filledMs ?? null, l.entryPx ?? null,
        l.walletCloseMs ?? null, l.seenCloseMs ?? null, l.exitAt ?? null, l.closedMs ?? null, l.exitPx ?? null, l.cash ?? null, l.reason ?? null, l.late ? 1 : 0, l.gap ? 1 : 0);
  }
  onWallet(t, ev) {
    const pid = String(ev.positionId);
    if (ev.kind === 'open') {
      this.counts.walletOpens++;
      if (this.legs.has(pid)) return;
      if (this.available() < this.margin()) { this.counts.skippedNoCapital++; this.note(t, 'skip_no_capital', { pid }); return; }
      this.reserved += this.margin();
      const l = { id: `${this.cfg.id}-${pid}`, pid, side: ev.side === 'long' ? 'L' : 'S', lev: Number(ev.leverage), status: 'pending', walletOpenMs: ev.ts, seenOpenMs: t, fillAt: t + this.cfg.entry(this.rng) };
      this.legs.set(pid, l); this.persist(l); this.note(t, 'entry_submitted', { pid, side: l.side, delayMs: l.fillAt - t });
      return;
    }
    // close or liquidation of the wallet position
    this.counts.walletCloses++;
    const l = this.legs.get(pid); if (!l || !OPEN_STATES.includes(l.status)) return;
    l.walletCloseMs = ev.ts; l.seenCloseMs = t; l.reason = 'wallet_' + ev.kind;
    if (l.status === 'pending') {
      if (l.fillAt - t >= RULES.cancelMinLeadMs) { l.status = 'cancelled'; this.reserved -= this.margin(); this.counts.cancelled++; this.legs.delete(pid); this.persist(l); return; }
      l.closeAfterFill = true; l.late = true; this.persist(l); return;
    }
    this.requestClose(t, l);
  }
  requestClose(t, l) { l.status = 'closing'; l.exitAt = t + this.cfg.exit(this.rng, t - l.filledMs); this.persist(l); }
  onGap(t) { for (const l of this.legs.values()) if (OPEN_STATES.includes(l.status)) { l.gap = true; this.counts.gapsDuringExposure++; this.persist(l); } }
  step(t, hl) {
    for (const l of [...this.legs.values()]) {
      if (l.status === 'pending' && t >= l.fillAt) {
        const px = hl.at(t);
        if (px == null) { if (t - l.fillAt < RULES.priceRetryMs) continue; l.status = 'unknown_entry'; this.reserved -= this.margin(); this.counts.unknownEntry++; this.legs.delete(l.pid); this.persist(l); continue; }
        Object.assign(l, { status: 'open', filledMs: t, entryPx: Math.floor(px * 100 + 1e-7) / 100 }); l.entryRaw = toRaw(l.entryPx); this.counts.fills++;
        if (l.late) this.counts.lateFills++;
        this.persist(l); if (l.closeAfterFill) this.requestClose(t, l);
      }
      if (l.status !== 'open' && l.status !== 'closing') continue;
      const mid = hl.at(t);
      if (mid != null) { // liquidation on observed HL mid, bust with buffer as in basket-sim / engine
        const buffer = BigInt(ETH[16]), lev = BigInt(l.lev), e = l.entryRaw, px = toRaw(mid);
        const thr = l.side === 'L' ? e * (lev - 1n) * UNIT / (lev * (UNIT - buffer)) : e * (lev + 1n) * UNIT / (lev * (UNIT + buffer));
        if (l.side === 'L' ? px <= thr : px >= thr) { this.settle(t, l, px, true, 'liquidation'); continue; }
      }
      if (l.status === 'open' && t - l.filledMs >= RULES.closeNotSeenMs) { this.counts.closeNotObserved++; l.reason = 'close_not_observed'; this.requestClose(t, l); }
      if (l.status === 'closing' && t >= l.exitAt) {
        if (mid == null) { if (t - l.exitAt < RULES.priceRetryMs) continue; l.status = 'unknown_exit'; this.reserved -= this.margin(); this.counts.unknownExit++; this.legs.delete(l.pid); this.persist(l); continue; }
        this.settle(t, l, toRaw(Math.floor(mid * 100 + 1e-7) / 100), false, l.reason);
      }
    }
  }
  settle(t, l, exitRaw, liquidated, reason) {
    const r = settle({ margin: this.margin(), leverage: l.lev, entry: l.entryRaw, exit: exitRaw, long: l.side === 'L', liquidated }, MONEY);
    this.reserved -= this.margin(); this.cash += r.cash; this.counts.closed++;
    if (liquidated) this.counts.liquidations++; if (r.cash > 0n) this.counts.wins++; else if (r.cash < 0n) this.counts.losses++;
    Object.assign(l, { status: liquidated ? 'liquidated' : 'closed', closedMs: t, exitPx: Number(exitRaw) / Number(SCALE), cash: r.cash.toString(), reason });
    this.legs.delete(l.pid); this.persist(l); this.note(t, 'leg_closed', { pid: l.pid, cash: Number(r.cash) / 1e18, reason });
  }
  exposure() { return [...this.legs.values()].filter(l => OPEN_STATES.includes(l.status)).length; }
  save() { this.db.prepare('INSERT INTO copy_models(model_id,state_json) VALUES(?,?) ON CONFLICT(model_id) DO UPDATE SET state_json=excluded.state_json').run(this.cfg.id, JSON.stringify({ cash: this.cash.toString(), counts: this.counts })); }
  summary() { return { id: this.cfg.id, label: this.cfg.label, cashUsdc: Number(this.cash) / 1e18, netUsdc: Number(this.cash - CAPITAL) / 1e18, reservedUsdc: Number(this.reserved) / 1e18, openLegs: this.exposure(), counts: this.counts }; }
}

function openDb(file) {
  if (file !== ':memory:') fs.mkdirSync(path.dirname(file), { recursive: true });
  const db = new DatabaseSync(file);
  db.exec(`PRAGMA journal_mode=WAL;CREATE TABLE IF NOT EXISTS copy_models(model_id TEXT PRIMARY KEY,state_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS copy_events(id INTEGER PRIMARY KEY,model_id TEXT,t_ms REAL,kind TEXT,payload_json TEXT);
CREATE TABLE IF NOT EXISTS copy_legs(model_id TEXT,leg_id TEXT,wallet_position_id TEXT,side TEXT,leverage INTEGER,margin REAL,status TEXT,wallet_open_ms REAL,seen_open_ms REAL,
 fill_at_ms REAL,filled_ms REAL,entry_px REAL,wallet_close_ms REAL,seen_close_ms REAL,exit_at_ms REAL,closed_ms REAL,exit_px REAL,cash_raw TEXT,reason TEXT,late INTEGER,gap INTEGER,
 PRIMARY KEY(model_id,leg_id));
CREATE TABLE IF NOT EXISTS copy_wallet_events(t_ms REAL,wallet_ts REAL,position_id TEXT,kind TEXT,side TEXT,leverage INTEGER,notional REAL,price REAL);
CREATE TABLE IF NOT EXISTS copy_gaps(t_ms REAL,reason TEXT);`);
  return db;
}

class Runner { // feed-agnostic: live loop and replay push the same normalised events
  constructor(db, seed = 20261021) { Object.assign(this, { db, hl: new Hl(), models: MODELS.map((c, i) => new CopyModel(c, db, seed + i)), clock: 0 }); }
  quote(t, bid, ask) { this.clock = Math.max(this.clock, t); if (!(bid > 0 && ask >= bid) || ask - bid > RULES.hlSpreadMaxUsd) return; this.hl.push(t, (bid + ask) / 2); this.tick(t); }
  trade(t, p) {
    this.clock = Math.max(this.clock, t);
    if (String(p.wallet).toLowerCase() !== WALLET || p.instrument !== 'ETH') return this.tick(t);
    this.db.prepare('INSERT INTO copy_wallet_events VALUES(?,?,?,?,?,?,?,?)').run(t, p.ts, String(p.positionId), p.kind, p.side, p.leverage, p.notionalUsd, p.entry ?? p.exit ?? null);
    for (const m of this.models) m.onWallet(t, p);
    this.tick(t);
  }
  gap(t, reason) { this.db.prepare('INSERT INTO copy_gaps VALUES(?,?)').run(t, reason); for (const m of this.models) m.onGap(t); }
  tick(t) { for (const m of this.models) m.step(t, this.hl); }
  save() { for (const m of this.models) m.save(); }
  summary() { return this.models.map(m => m.summary()); }
}

async function live({ dir }) {
  const db = openDb(path.join(dir, 'copy.sqlite')), R = new Runner(db);
  let cursor = null, boot = null, lastSave = 0, lastErr = 0, stop = false, events = 0, gaps = 0;
  for (const s of ['SIGTERM', 'SIGINT']) process.on(s, () => { stop = true; });
  const status = () => { const f = path.join(dir, 'status.json'); fs.writeFileSync(f + '.tmp', JSON.stringify({ mode: 'SIM_ONLY_NO_ORDERS', wallet: WALLET, updatedAtMs: Date.now(), clockMs: R.clock, events, gaps, rules: RULES, models: R.summary() }, null, 1)); fs.renameSync(f + '.tmp', f); };
  while (!stop) {
    let body;
    try {
      const url = cursor == null ? `${FEED}?limit=1` : `${FEED}?after=${cursor}&limit=256`;
      const r = await fetch(url, { signal: AbortSignal.timeout(5000) }); if (!r.ok) throw new Error('feed HTTP ' + r.status); body = await r.json();
      if (typeof body.bootId !== 'string' || !Array.isArray(body.events)) throw new Error('feed envelope');
    } catch (e) { if (Date.now() - lastErr > 60000) { lastErr = Date.now(); console.error('feed error:', e.message); } await new Promise(r => setTimeout(r, 1000)); continue; }
    if (cursor == null || body.bootId !== boot) { const fresh = cursor != null; boot = body.bootId; cursor = body.lastSequence; if (fresh) { gaps++; R.gap(Date.now(), 'boot_changed'); } continue; }
    if (body.gap === true) { gaps++; R.gap(Date.now(), 'history_gap'); if (body.events.length === 0) cursor = Math.max(cursor, (body.firstSequence ?? cursor + 1) - 1); }
    for (const e of body.events) {
      if (e.sequence <= cursor) continue; cursor = e.sequence; events++;
      if (e.kind === 'trade' && e.venue === 'paper' && e.payload) R.trade(e.recv_ms, e.payload);
      else if (e.kind === 'market' && e.venue === 'hl' && e.symbol === 'ETH' && e.payload?.frameKind === 'quote') R.quote(e.recv_ms, Number(e.payload.bid), Number(e.payload.ask));
      else R.tick(e.recv_ms);
    }
    if (Date.now() - lastSave >= 5000) { lastSave = Date.now(); R.save(); status(); }
    if (!body.events.length) await new Promise(r => setTimeout(r, 200));
  }
  R.save(); status(); db.close();
}

// Offline check on our own continuous capture (data/live) with the same Runner. Not evidence of future profit.
function replay({ from, to, db = ':memory:' }) {
  const read = src => {
    const p = spawnSync('python', ['-I', path.join(ROOT, 'tools/read_capture.py'), '--source', src, '--since-ms', String(from), '--until-ms', String(to), '--limit', '0'], { maxBuffer: 1 << 30, encoding: 'utf8' });
    if (p.status !== 0) throw new Error(p.stderr); return p.stdout.split('\n').filter(Boolean).map(l => JSON.parse(l));
  };
  const ev = [];
  for (const m of read('hl')) { const d = m.data; if (d?.channel === 'bbo' && d.data?.coin === 'ETH') ev.push([m.recv_ms, 'q', Number(d.data.bbo[0].px), Number(d.data.bbo[1].px)]); }
  for (const m of read('paper')) { const p = m.data?.push; if (p?.channel === 'public:trades') for (const t of p.pub.data) ev.push([m.recv_ms, 't', t]); }
  ev.sort((a, b) => a[0] - b[0]);
  const R = new Runner(openDb(db));
  for (const e of ev) e[1] === 'q' ? R.quote(e[0], e[2], e[3]) : R.trade(e[0], e[2]);
  R.tick(to + 600000); R.save();
  return { from, to, events: ev.length, models: R.summary(), legs: R.db.prepare('SELECT model_id,status,reason,COUNT(*) n,SUM(CAST(cash_raw AS REAL))/1e18 cash FROM copy_legs GROUP BY 1,2,3').all() };
}

if (require.main === module) {
  const [cmd, ...rest] = process.argv.slice(2), o = {};
  for (let i = 0; i < rest.length; i += 2) o[rest[i].replace(/^--/, '')] = rest[i + 1];
  const dir = path.resolve(o['state-dir'] || path.join(ROOT, 'reports/copy-6ec3-shadow-20261021'));
  if (cmd === 'start') live({ dir }).catch(e => { console.error(e); process.exit(1); });
  else if (cmd === 'status') process.stdout.write(fs.readFileSync(path.join(dir, 'status.json'), 'utf8') + '\n');
  else if (cmd === 'replay') process.stdout.write(JSON.stringify(replay({ from: Number(o.from), to: Number(o.to), db: o.db || ':memory:' }), null, 1) + '\n');
  else { console.error('usage: copy-6ec3.cjs start|status [--state-dir DIR] | replay --from MS --to MS [--db FILE]'); process.exit(2); }
}
module.exports = { RULES, MODELS, WALLET, Hl, CopyModel, Runner, openDb, replay };
