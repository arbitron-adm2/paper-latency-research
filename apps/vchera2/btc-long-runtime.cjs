'use strict';
const fs = require('node:fs'), crypto = require('node:crypto');
const { ID, PROFILE_HASH, Candidate, SecondView } = require('./btc-long-candidate.cjs');
class CandidateRuntime {
  constructor({ db, marketData, seedFile, seedSha256, now = Date.now }) {
    this.db = db; this.now = now; this.logic = new Candidate(); this.error = null; this.processedT = null;
    let lastRow = null;
    try {
      const data = fs.readFileSync(seedFile);
      if (crypto.createHash('sha256').update(data).digest('hex') !== seedSha256) throw new Error('SEED_HASH');
      const seed = JSON.parse(data);
      if (seed.profileHash !== PROFILE_HASH || seed.symbol !== 'BTC' || seed.scope !== 'OWN_CANONICAL_ONLY' || !Array.isArray(seed.rows) || seed.rows.length < 1200 || seed.rows.length > 1800 || seed.cutMs > now()) throw new Error('SEED_SCOPE');
      this.logic.seed(seed.rows); lastRow = seed.rows.at(-1); this.seedCutMs = seed.cutMs;
    } catch (e) { this.error = 'CANDIDATE_' + e.message; }
    this.view = new SecondView({ onRow: row => this.logic.step(row), lastRow });
    this.detach = marketData.onEvent(e => { try { this.view.ingest(e); } catch (error) { this.error = error.message; } });
    db.exec(`CREATE TABLE IF NOT EXISTS btc_long_candidate_intents(
      intent_id TEXT PRIMARY KEY,signal_ms INTEGER NOT NULL,valid_until_ms INTEGER NOT NULL,
      ctx_json TEXT NOT NULL,margin REAL NOT NULL,leverage INTEGER NOT NULL,state TEXT NOT NULL,position_id TEXT)`);
  }
  advance(now = this.now()) { try { this.view.advance(now); } catch (e) { this.error = e.message; } }
  takeRow() {
    const row = this.logic.last;
    if (!row || row.t === this.processedT) return null;
    this.processedT = row.t;
    return row;
  }
  blockers() {
    const r = this.logic.last, reasons = [];
    if (this.error) reasons.push(this.error);
    if (!this.view.liveReady()) reasons.push('CANDIDATE_WAITING_FOR_LIVE_QUOTES_AND_BOOKS');
    if (!r || r.warmup) reasons.push('CANDIDATE_WARMUP_20M');
    if (!r?.ready || this.now() - r.t > 1500) reasons.push('CANDIDATE_PRICE_NOT_READY');
    if (!Number.isFinite(r?.book)) reasons.push('CANDIDATE_BOOK_NOT_READY');
    const missing = this.db.prepare("SELECT COUNT(*) n FROM btcy2_lag_trades WHERE live=1 AND status='closed' AND qty>0 AND strategy=? AND real_pnl IS NULL").get(ID).n;
    if (missing) reasons.push('CANDIDATE_CLOSED_PNL_NOT_RECONCILED');
    return reasons;
  }
  nextEntryAt() {
    const last = this.db.prepare("SELECT MAX(closed_at) t FROM btcy2_lag_trades WHERE live=1 AND status='closed' AND qty>0 AND strategy=?").get(ID).t;
    return last ? last + 32000 : 0;
  }
  remember(q) {
    this.db.prepare('INSERT INTO btc_long_candidate_intents VALUES(?,?,?,?,?,?,?,NULL)').run(
      q.intentId.toLowerCase(), q.createdAt, q.validUntilMs, JSON.stringify(q.ctx), q.margin, q.leverage, 'prepared');
  }
  mark(intent, state, position = null) {
    if (intent) this.db.prepare('UPDATE btc_long_candidate_intents SET state=?,position_id=COALESCE(?,position_id) WHERE intent_id=?').run(state, position, intent.toLowerCase());
  }
  recover({ pending, positions, cancelledLive, provider }) {
    const ids = new Set([...(provider.entrySafety?.unknown || []).map(r => String(r.intentId).toLowerCase()),
      ...(provider.wallet.operations || []).map(op => String(op[0]).toLowerCase()),
      ...(provider.wallet.openPositions || []).map(p => String(p.intentId).toLowerCase())]);
    for (const r of this.db.prepare("SELECT * FROM btc_long_candidate_intents WHERE state='prepared'").all()) {
      if (!ids.has(r.intent_id) || pending.some(q => q.intentId?.toLowerCase() === r.intent_id) || cancelledLive.has(r.intent_id) || [...positions.values()].some(p => p.intentId?.toLowerCase() === r.intent_id)) continue;
      const ctx = JSON.parse(r.ctx_json);
      pending.push({ strategy: ID, side: 'L', ctx, sig: ctx.sig, margin: r.margin, leverage: r.leverage,
        createdAt: r.signal_ms, validUntilMs: r.valid_until_ms, fillAt: Infinity, px: ctx.hl,
        live: true, intentId: r.intent_id, rank: null, recovered: true });
    }
  }
  snapshot() {
    return { ...this.logic.snapshot(), error: this.error, seedCutMs: this.seedCutMs,
      liveInputsSeen: [...this.view.liveSeen], liveInputsReady: this.view.liveReady(),
      acceptedEvents: this.view.accepted, rejectedEvents: this.view.rejected, nextEntryAt: this.nextEntryAt() };
  }
}
module.exports = { CandidateRuntime };
