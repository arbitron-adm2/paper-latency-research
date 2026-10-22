'use strict';
// Entry expiry only. No secret material or network imports. A close uses its own independent sender.
// Signed entry validity. Default 5 s (previous policy). Vlad/site policy signs for 3600 s and relies on
// strategy cancels; raising it requires an explicit launch-config value within these bounds.
const MIN_VALIDITY_MS = 5000, MAX_VALIDITY_MS = 3600000;
function validityBound(v) {
  if (v === undefined) return MIN_VALIDITY_MS;
  if (!Number.isSafeInteger(v) || v < MIN_VALIDITY_MS || v > MAX_VALIDITY_MS) throw new Error('entrySignedValidityMs must be an integer 5000..3600000');
  return v;
}
function configuredValidityMs(launch = require('./launch-config.json')) { return validityBound(launch.independentGuards && launch.independentGuards.entrySignedValidityMs); }
function entryDeadline({ signalAtMs, validUntilMs, maxValidityMs = MIN_VALIDITY_MS }, now = Date.now()) {
  const limit = validityBound(maxValidityMs);
  if (![signalAtMs, validUntilMs, now].every(Number.isSafeInteger)) throw new Error('Entry signal times must be integer milliseconds');
  if (signalAtMs > now || now - signalAtMs > 1000 || validUntilMs <= now || validUntilMs <= signalAtMs || validUntilMs - signalAtMs > limit) throw new Error(`Original entry signal expired or outside ${limit} ms policy`);
  // The contract accepts equality, hence one complete second of reserve.
  const deadline = Math.floor(validUntilMs / 1000) - 1;
  if (deadline * 1000 <= now) throw new Error('Insufficient signed entry lifetime after seconds reserve');
  return { signalAtMs, validUntilMs, maxValidityMs: limit, deadline: String(deadline), safetySeconds: 1 };
}
function createEntrySender({ master, takeNonce, sign, post, record, unresolved = () => [], now = Date.now, maxValidityMs = MIN_VALIDITY_MS }) {
  const validity = validityBound(maxValidityMs);
  if (typeof record !== 'function') throw new Error('Durable entry metadata writer required');
  return async function send(input, { onPrepared = () => {}, beforeSend } = {}) {
    let invoked = false, metadata = null;
    try {
      if (input.instrumentId !== undefined && input.instrumentId !== 0) throw new Error('ETH is observe-only');
      const plan = entryDeadline({ ...input, maxValidityMs: validity }, now());
      if (unresolved().length) throw new Error('Unresolved entry intent; no additional entry allowed');
      if (!(input.margin > 0) || !Number.isFinite(input.margin) || !Number.isSafeInteger(input.leverage) || input.leverage < 1 || input.leverage > 1000) throw new Error('Invalid entry sizing');
      if (typeof beforeSend !== 'function') throw new Error('Fresh runtime entry preflight required');
      const message = { user: master, instrumentId: 0, isLong: !!input.isLong,
        size: (BigInt(Math.round(input.margin * 1e6)) * 10n ** 12n).toString(), leverage: input.leverage,
        useDebt: false, nonce: takeNonce(), deadline: plan.deadline };
      const signedResult = await sign(message);
      entryDeadline(plan, now());
      metadata = { intentId: signedResult.intentId, action: 'openPosition', instrumentId: 0, state: 'unknown',
        signalAtMs: plan.signalAtMs, validUntilMs: plan.validUntilMs, deadline: plan.deadline, preparedAtMs: now() };
      await record(metadata); // Failure aborts; no signature/body is ever written to this ledger.
      await onPrepared({ ...metadata });
      await beforeSend({ ...metadata });
      entryDeadline(plan, now());
      if (Number(plan.deadline) * 1000 <= now()) throw new Error('Signed entry expired before POST');
      invoked = true;
      const result = await post({ action: 'openPosition', signed: signedResult.signed });
      const actual = result.data && result.data.intentId;
      const mismatch = actual && actual !== metadata.intentId;
      const code = result.data?.code;
      const rejected = !result.ok && !result.uncertain && !mismatch &&
        ((result.status === 409 && code === 'admission_rejected') || (result.status === 422 && code === 'deadline_expired'));
      if (rejected) await record({ ...metadata, state: 'rejected', proof: 'EXPLICIT_PROVIDER_PRE_ADMISSION_REJECTION',
        providerCode: code, httpStatus: result.status, rejectedAtMs: now(), reason: code });
      return { ...result, intentId: metadata.intentId, actualIntentId: actual || null,
        rejected, uncertain: !rejected && (!!result.uncertain || !result.ok || !!mismatch), signedDeadline: plan.deadline };
    } catch (error) {
      if (invoked) return { ok: false, status: 0, intentId: metadata.intentId, uncertain: true, data: { code: 'transport_unknown' } };
      if (metadata) await record({ ...metadata, state: 'not_sent', proof: 'LOCAL_ABORT_BEFORE_FIRST_TRANSPORT_CALL', reason: error.message });
      error.knownNotSent = true;
      throw error;
    }
  };
}
class EntryIntentLedger {
  constructor({ wallet, read, save }) {
    this.wallet = wallet.toLowerCase(); this.save = save; this.records = new Map(); this.error = null;
    try {
      const data = read();
      if (data.schemaVersion !== 1 || data.wallet !== this.wallet || !Array.isArray(data.records)) throw new Error('Invalid entry ledger');
      for (const record of data.records) {
        if (!/^0x[0-9a-f]{64}$/.test(record.intentId) || !['unknown', 'not_sent', 'filled', 'expired', 'rejected'].includes(record.state)) throw new Error('Invalid entry record');
        this.records.set(record.intentId, record);
      }
    } catch (error) { if (error.code !== 'ENOENT') this.error = 'Entry ledger unavailable/corrupt; entries blocked, do not delete it'; }
  }
  unresolved() { return this.error ? [{ state: 'unknown', error: this.error }] : [...this.records.values()].filter(row => row.state === 'unknown').map(row => ({ ...row })); }
  record(input) {
    if (this.error) throw new Error(this.error);
    if (!/^0x[0-9a-f]{64}$/.test(input.intentId) || !['unknown', 'not_sent', 'filled', 'expired', 'rejected'].includes(input.state)) throw new Error('Invalid entry metadata');
    const old = this.records.get(input.intentId);
    if (old && old.state !== 'unknown' && input.state === 'unknown') return;
    const row = Object.fromEntries(['intentId', 'action', 'instrumentId', 'state', 'signalAtMs', 'validUntilMs', 'deadline', 'preparedAtMs', 'proof', 'reason', 'positionId', 'providerCode', 'httpStatus', 'rejectedAtMs'].filter(key => input[key] !== undefined).map(key => [key, input[key]]));
    if (row.state === 'not_sent' && row.proof !== 'LOCAL_ABORT_BEFORE_FIRST_TRANSPORT_CALL') throw new Error('No proof transport was never invoked');
    if (row.state === 'filled' && row.proof !== 'CURRENT_ACCOUNT_POSITION_EXACT_INTENT_ID') throw new Error('No exact entry execution evidence');
    if (row.state === 'expired' && row.proof !== 'SIGNED_DEADLINE_PASSED_NO_POSITION_FRESH_WALLET') throw new Error('No signed-deadline expiry evidence');
    if (row.state === 'rejected' && (row.proof !== 'EXPLICIT_PROVIDER_PRE_ADMISSION_REJECTION' ||
      !((row.httpStatus === 409 && row.providerCode === 'admission_rejected') || (row.httpStatus === 422 && row.providerCode === 'deadline_expired')))) throw new Error('No explicit provider rejection evidence');
    const next = new Map(this.records); next.set(row.intentId, { ...old, ...row });
    this.save({ schemaVersion: 1, wallet: this.wallet, records: [...next.values()] });
    this.records = next;
  }
  // The contract executes an entry only while block.timestamp <= signed deadline. A wallet snapshot taken
  // more than graceMs after that deadline without a position or a pending operation for the exact intentId
  // proves the entry can no longer execute; the unknown record is resolved as expired (no exposure).
  resolveExpired({ walletT, positions = [], operationIds = [], graceMs = 30000 }) {
    const ids = new Set([...positions.map(p => String(p.intentId || '').toLowerCase()), ...operationIds.map(x => String(x).toLowerCase())]);
    for (const row of [...this.records.values()]) {
      if (row.state !== 'unknown' || !Number.isSafeInteger(Number(row.deadline))) continue;
      if (!(walletT >= (Number(row.deadline) + 1) * 1000 + graceMs) || ids.has(row.intentId)) continue;
      this.record({ ...row, state: 'expired', proof: 'SIGNED_DEADLINE_PASSED_NO_POSITION_FRESH_WALLET', reason: `wallet ${walletT} > deadline ${row.deadline} + ${graceMs} ms, no position/operation` });
    }
  }
  observePositions(positions) {
    for (const position of positions) if (position.intentId && this.records.get(position.intentId)?.state === 'unknown') this.record({ ...this.records.get(position.intentId), state: 'filled', positionId: position.positionId, proof: 'CURRENT_ACCOUNT_POSITION_EXACT_INTENT_ID' });
  }
}
module.exports = { entryDeadline, createEntrySender, EntryIntentLedger, configuredValidityMs, MIN_VALIDITY_MS, MAX_VALIDITY_MS };
