'use strict';
// Close PnL/fee arrive from account history (5 s poll, 60 s on 429) after the exit fill (21.10 21:06: LIVE stopped 1 s after close #380):
// an unresolved input blocks entries at once via reason() and stops LIVE only if it persists.
const INPUT_GRACE_MS = 120000;
function validateSession(c) {
  if (!c) return null;
  const iso = x => typeof x === 'string' && /(Z|[+-]\d{2}:\d{2})$/.test(x) && Number.isFinite(Date.parse(x));
  if (!/^[a-zA-Z0-9_-]+$/.test(c.id || '') || !iso(c.startAt) || !iso(c.endAt) ||
      Date.parse(c.endAt) <= Date.parse(c.startAt) || !(c.lossLimitUsd > 0) || !Number.isFinite(c.lossLimitUsd)) throw new Error('Invalid fixed LIVE session');
  return { ...c, startMs: Date.parse(c.startAt), endMs: Date.parse(c.endAt) };
}
class SessionRisk {
  constructor({ config, db, load = () => null, save = () => {} }) {
    this.config = validateSession(config); this.save = save; this.state = null;
    if (!this.config) return;
    const old = load();
    if (old && old.id === this.config.id) {
      if (old.startAt !== config.startAt || old.endAt !== config.endAt || old.lossLimitUsd !== config.lossLimitUsd) throw new Error('Existing session identity/config mismatch');
      this.state = old;
    }
    this.db = db; this.closed = null;
  }
  reason(now = Date.now()) {
    const c = this.config; if (!c) return null;
    if (this.state?.tripped) return this.state.reason;
    if (now < c.startMs) return 'LIVE_SESSION_NOT_STARTED';
    if (now >= c.endMs) return 'LIVE_SESSION_ENDED';
    return this.state?.inputError || null;
  }
  evaluate({ now = Date.now(), positions = [], mid, marketFresh = true }) {
    const c = this.config; if (!c) return null;
    let realised = 0, openLoss = 0, inputError = null;
    this.closed ||= this.db.prepare("SELECT real_pnl,close_evidence FROM btcy2_lag_trades WHERE live=1 AND status='closed' AND position_id IS NOT NULL AND opened_at>=? AND closed_at>=?");
    for (const row of this.closed.all(c.startMs, c.startMs)) {
      let e; try { e = JSON.parse(row.close_evidence); } catch {}
      if (!Number.isFinite(row.real_pnl) || !e || !Number.isFinite(e.fee) || e.fee < 0) { inputError = 'LIVE_SESSION_PNL_UNRESOLVED'; break; }
      realised += row.real_pnl - e.fee;
    }
    for (const p of positions.filter(p => p.live)) {
      if (!marketFresh || !(mid > 0) || !(p.entry > 0) || !(p.qty > 0)) { inputError = 'LIVE_SESSION_MARK_UNAVAILABLE'; break; }
      // Do not fund extra loss allowance with unconfirmed, haircut-prone open profit.
      openLoss += Math.min(0, (p.side === 'L' ? 1 : -1) * (mid - p.entry) * p.qty);
    }
    const net = realised + openLoss, wasTripped = !!this.state?.tripped;
    const reason = wasTripped ? this.state.reason : now >= c.endMs ? 'LIVE_SESSION_ENDED' : now >= c.startMs && !inputError && net <= -c.lossLimitUsd ? 'LIVE_SESSION_LOSS_LIMIT' : null;
    const next = { id: c.id, startAt: c.startAt, endAt: c.endAt, lossLimitUsd: c.lossLimitUsd,
      at: now, realisedNetUsd: realised, openLossUsd: openLoss, netRiskUsd: net, inputError,
      tripped: !!reason, reason, trippedAt: this.state?.trippedAt || (reason ? now : null),
      basis: 'Confirmed own closes minus explicit fees plus negative open PnL; open profits excluded; fixed session across midnight' };
    if (!this.state || (!wasTripped && next.tripped)) this.save(next);
    this.inputSince = inputError ? (this.inputSince ?? now) : null;
    this.state = next; return next;
  }
  shouldStop(now = Date.now()) { const s = this.state; return !!s && (s.tripped || (!!s.inputError && now - this.inputSince >= INPUT_GRACE_MS)); }
  snapshot() { return this.config ? { ...this.state, configured: true, startAt: this.config.startAt, endAt: this.config.endAt, lossLimitUsd: this.config.lossLimitUsd, entryBlockReason: this.reason() } : null; }
}
module.exports = { SessionRisk, validateSession, INPUT_GRACE_MS };
