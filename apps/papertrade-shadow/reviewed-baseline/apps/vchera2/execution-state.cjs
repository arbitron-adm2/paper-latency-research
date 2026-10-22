'use strict';
// Shared, deterministic reconciliation rules. A missing wallet row is NEVER execution proof.
const E18 = value => Number(BigInt(value || '0') / 1000000000000n) / 1e6;
const pid = value => String(value);
const intent = value => String(value || '').toLowerCase();
function parsePosition(p) {
  if (!Array.isArray(p) || p.length < 10) throw new Error('Invalid position tuple');
  return { positionId: pid(p[0]), instrumentId: Number(p[1]), isLong: !!p[2], entry: Number(p[4]) / (Number(p[1]) === 1 ? 100 : 10),
    margin: E18(p[5]), leverage: Number(p[6]), bust: Number(p[7]) / (Number(p[1]) === 1 ? 100 : 10), openedAt: Number(p[8]), intentId: intent(p[9]) };
}
class WalletView {
  constructor(address) {
    this.address = address.toLowerCase();
    this.closed = new Map();
    this.wallet = { address: this.address, t: 0, connected: false, balance: NaN, available: NaN, locked: NaN, sessionKeys: [], openPositions: [], operations: [], err: null, fullSnapshotAt: 0 };
  }
  confirmClosed(positionId, evidence) {
    if (!evidence || !Number.isFinite(evidence.px) || !Number.isFinite(evidence.ts) || !evidence.source) throw new Error('Explicit close execution evidence required');
    const id = pid(positionId), old = this.closed.get(id);
    // History may enrich public-trade evidence with the realised payout.
    this.closed.set(id, { ...old, ...evidence });
    this.wallet.openPositions = this.wallet.openPositions.filter(p => p.positionId !== id);
  }
  apply(j, { initial = false, now = Date.now() } = {}) {
    if (j.address && j.address.toLowerCase() !== this.address) throw new Error('Wrong wallet');
    if (initial && (!Array.isArray(j.openPositions) || !Array.isArray(j.operations) || !Array.isArray(j.sessionKeys) || j.balance == null)) throw new Error('Incomplete initial wallet snapshot');
    const w = this.wallet;
    const positions = new Map((Array.isArray(j.openPositions) ? j.openPositions.map(parsePosition) : w.openPositions).map(p => [p.positionId, p]));
    for (const raw of j.addedPositions || []) { const p = parsePosition(raw); positions.set(p.positionId, p); }
    for (const raw of j.closedPositions || []) positions.delete(pid(Array.isArray(raw) ? raw[0] : raw));
    // Delayed full snapshots and duplicated addedPositions cannot resurrect a confirmed close.
    for (const id of this.closed.keys()) positions.delete(id);
    const operations = new Map((Array.isArray(j.operations) ? j.operations : w.operations).map(op => [intent(op[0]), op]));
    for (const op of j.addedOperations || []) operations.set(intent(op[0]), op);
    for (const id of j.removedOperationIds || []) operations.delete(intent(id));
    if (j.balance != null) w.balance = E18(j.balance);
    if (j.funds && j.funds.availableBalance != null) w.available = E18(j.funds.availableBalance);
    if (j.locked != null) w.locked = E18(j.locked);
    if (Array.isArray(j.sessionKeys)) w.sessionKeys = j.sessionKeys.map(([address, expires]) => ({ address: intent(address), expiresAt: Number(expires) * 1000 }));
    w.openPositions = [...positions.values()]; w.operations = [...operations.values()];
    if (j.indexedThroughBlock != null) w.indexedThroughBlock = j.indexedThroughBlock;
    if (initial) w.fullSnapshotAt = now;
    w.t = now; w.connected = true; w.err = null;
    return w;
  }
}
function parseCloseTrade(x) {
  if (x.kind !== 'PositionClosed' || x.positionId == null) return null;
  const scale = Number(x.priceScaleRaw) || 10;
  const result = { px: Number(x.exitPriceRaw) / scale, ts: Number(x.timestampMs), source: 'account-history', realPnl: E18(x.adjustedPnlRaw), fee: E18(x.userPaidFeeRaw) };
  if (!Number.isFinite(result.px) || result.px <= 0 || !Number.isFinite(result.ts)) throw new Error('Invalid history close');
  return result;
}
function recoverClosing(row) {
  if (!row.close_state) return null;
  const cl = JSON.parse(row.close_state);
  if (!cl || typeof cl.why !== 'string' || !Number.isFinite(cl.decidedAt)) throw new Error('Corrupt saved close state');
  return { ...cl, sending: false };
}
function marginCapacity(available, margin = 2100, reserve = 0.05) {
  const required = margin / (1 - reserve);
  return { required, shortfall: Number.isFinite(available) ? Math.max(0, required - available) : null, ready: Number.isFinite(available) && available >= required };
}
function buildLiveFill(q, filled) {
  const invalidatedReason = q.cancelReason || (q.signalGone ? 'сигнал пропал в очереди' : q.missed ? 'HL ушёл без нас' : null);
  return { ...filled, intentId: q.intentId, decidedAt: q.createdAt, decidedPx: q.px, rank0: q.rank0, rank: q.rank, group: q.group,
    invalidatedReason, signalGone: !!q.signalGone, cancelFailed: q.cancelFailed || null, cancelRequestedAt: q.cancelRequestedAt || null, cancelTries: q.cancelTries || 0 };
}
function trailingHit(cfg, best, current) {
  return cfg.trailUsd > 0 && best > 0 && best >= Math.max(0, cfg.trailStartUsd || 0) && best - current >= cfg.trailUsd;
}
module.exports = { WalletView, parsePosition, parseCloseTrade, recoverClosing, marginCapacity, E18, buildLiveFill, trailingHit };
