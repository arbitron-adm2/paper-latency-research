'use strict';
// Portable evidence adapter: fixed GET feed only. No Chrome, DOM, signer or order APIs.
const crypto = require('node:crypto');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { TimingGate, number } = require('./latency.js');
const ENDPOINT = 'http://127.0.0.1:18762/api/research/events';
const MODE = 'READ_ONLY_RESEARCH_NO_ORDERS';
const finite = n => typeof n === 'number' && Number.isFinite(n);
const integer = n => Number.isSafeInteger(n) && n >= 0;
function canonical(v) {
  if (v === undefined || typeof v === 'function' || typeof v === 'bigint') throw new Error('UNSUPPORTED_JSON');
  if (v === null || typeof v !== 'object') {
    if (typeof v === 'number' && !Number.isFinite(v)) throw new Error('NONFINITE_JSON');
    return JSON.stringify(v);
  }
  if (Array.isArray(v)) return '[' + v.map(canonical).join(',') + ']';
  return '{' + Object.keys(v).sort().map(k => JSON.stringify(k) + ':' + canonical(v[k])).join(',') + '}';
}
const hash = v => crypto.createHash('sha256').update(canonical(v)).digest('hex');
// The fixed producer hashes its normalised JSON in insertion order. Internal
// config/input hashes remain recursively sorted; neither is a raw-wire hash.
function payloadHash(v) { canonical(v); return crypto.createHash('sha256').update(JSON.stringify(v)).digest('hex'); }
function assertEndpoint(url) { if (url !== ENDPOINT) throw new Error('FEED_ALLOWLIST'); return url; }
function fetchBatch(after, limit = 128, transport = http, timeoutMs = 1500) {
  if (!integer(after) || !Number.isInteger(limit) || limit < 1 || limit > 256) return Promise.reject(new Error('BAD_CURSOR_OR_LIMIT'));
  assertEndpoint(ENDPOINT);
  return new Promise((resolve, reject) => {
    let size = 0, settled = false, deadline;
    const done = (error, value) => { if (settled) return; settled = true; clearTimeout(deadline); error ? reject(error) : resolve(value); };
    const url = new URL(ENDPOINT); url.searchParams.set('after', String(after)); url.searchParams.set('limit', String(limit));
    const req = transport.request(url, { method: 'GET', agent: false, headers: { Accept: 'application/json' } }, res => {
      if (res.statusCode !== 200) { done(new Error(res.statusCode >= 300 && res.statusCode < 400 ? 'REDIRECT_REFUSED' : 'FEED_HTTP_STATUS')); res.resume(); req.destroy(); return; }
      const chunks = [];
      res.on('data', bytes => { size += bytes.length; if (size > 4 * 1024 * 1024) { done(new Error('FEED_BODY_LIMIT')); req.destroy(); } else chunks.push(bytes); });
      res.on('end', () => { if (settled) return; try { done(null, JSON.parse(Buffer.concat(chunks).toString('utf8'))); } catch { done(new Error('FEED_JSON_INVALID')); } });
      res.on('error', () => done(new Error('FEED_RESPONSE_ERROR')));
    });
    const expire = () => { done(new Error('FEED_TIMEOUT')); req.destroy(); };
    req.setTimeout(timeoutMs, expire); deadline = setTimeout(expire, timeoutMs);
    req.on('error', () => done(new Error('FEED_UNAVAILABLE'))); req.end();
  });
}
function atomicWrite(file, data) {
  const target = path.resolve(file); fs.mkdirSync(path.dirname(target), { recursive: true, mode: 0o700 });
  const tmp = target + '.' + process.pid + '.' + crypto.randomUUID() + '.tmp';
  let fd;
  try { fd = fs.openSync(tmp, 'wx', 0o600); fs.writeFileSync(fd, JSON.stringify(data, null, 2) + '\n'); fs.fsyncSync(fd); fs.closeSync(fd); fd = null; fs.renameSync(tmp, target); }
  finally { if (fd !== null && fd !== undefined) fs.closeSync(fd); if (fs.existsSync(tmp)) fs.unlinkSync(tmp); }
}
function support(summary, selector = {}) {
  if (!summary || summary.schemaVersion !== 1 || summary.kind !== 'guard_empirical_summary') return { status: 'UNKNOWN_NO_EMPIRICAL_CASEBOOK', cohort: null };
  const asset = selector.asset;
  const candidates = (summary.cohorts || []).filter(c => c.asset === asset && c.notional_usd === selector.notional_usd);
  if (!['BTC', 'ETH'].includes(asset) || !finite(selector.notional_usd)) return { status: 'UNKNOWN_REQUEST_ASSET_OR_SIZE', cohort: null };
  if (!candidates.length) return { status: 'OUT_OF_SUPPORT_ASSET_OR_EXACT_SIZE', cohort: null, asset, notional_usd: selector.notional_usd };
  const found = candidates.find(c => c.cohort_id === selector.cohort_id);
  if (!found) return { status: 'OUT_OF_SUPPORT_COHORT_REQUIRED', cohort: null, candidate_count: candidates.length };
  return { status: 'DESCRIPTIVE_ONLY_NOT_CALIBRATED', cohort: found, calibration: summary.calibration?.status || 'INCONCLUSIVE', prediction_permission: false };
}
const NEW_POSITIONLESS_OBSERVATION = Object.freeze({ mode: MODE, newRun: true, resume: false,
  openPositionCount: 0, pendingRequestCount: 0, accountingScope: 'OBSERVATIONAL_ONLY' });
function metadataError(batch) {
  if (!batch || batch.schemaVersion !== 1 || batch.mode !== MODE || typeof batch.bootId !== 'string' || !batch.bootId || batch.bootId.length > 160 ||
      !finite(batch.observedAtMs) || !integer(batch.firstSequence) || !integer(batch.lastSequence) || typeof batch.gap !== 'boolean' || !Array.isArray(batch.events) || batch.events.length > 256) return 'FEED_SCHEMA_INVALID';
  return batch.firstSequence > batch.lastSequence + 1 ? 'FEED_RANGE_INVALID' : null;
}
class FeedGuard {
  constructor({ runId, config = {}, empirical = null, selector = {}, maxFeedAgeMs = 3000, onPrediction = null } = {}) {
    if (typeof runId !== 'string' || !runId || runId.length > 160) throw new Error('RUN_ID_REQUIRED');
    if (!finite(maxFeedAgeMs) || maxFeedAgeMs <= 0 || maxFeedAgeMs > 60000) throw new Error('INVALID_FEED_AGE');
    this.runId = runId; this.config = config; this.maxAge = maxFeedAgeMs; this.gate = new TimingGate(config);
    this.empirical = empirical; this.selector = selector; this.onPrediction = onPrediction;
    this.boot = null; this.cursor = 0; this.first = null; this.last = null; this.lastLocal = null; this.lastServer = null; this.lastEventRecv = null;
    this.seen = new Map(); this.issues = new Map(); this.activeFetchError = null; this.lastRevision = null; this.revisionWork = null;
    this.global = null; this.connections = null; this.latestMarket = {}; this.predictions = 0;
    this.bootstrapRecord = null; this.notices = [];
    this.counts = { batches: 0, bootstrap_metadata: 0, market: 0, pressure: 0, trade: 0, duplicates: 0, rejected: 0, boot_changes: 0 };
    this.modelHash = hash({ model: 'paper_workload_heuristic_v1', config: this.gate.config });
    this.implementationHash = hash(Object.fromEntries([__filename, path.join(__dirname, 'latency.js')].map(file => [path.basename(file), crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex')])));
  }
  issue(code, now, sticky = true, category = 'continuity') {
    const previous = this.issues.get(code);
    if (previous) { previous.last_seen_ms = now; previous.count++; previous.active = true; }
    else this.issues.set(code, { code, first_seen_ms: now, last_seen_ms: now, count: 1, sticky, category, active: true });
  }
  fetchError(code, now = Date.now()) { this.activeFetchError = code; this.issue(code, now, false, 'fetch'); }
  bootstrapAtCurrent(batch, now = Date.now(), declaration) {
    const keys = Object.keys(NEW_POSITIONLESS_OBSERVATION);
    if (!declaration || Object.keys(declaration).length !== keys.length || keys.some(k => declaration[k] !== NEW_POSITIONLESS_OBSERVATION[k])) throw new Error('EXPLICIT_NEW_POSITIONLESS_OBSERVATION_REQUIRED');
    if (this.bootstrapRecord || this.boot !== null || this.cursor !== 0 || this.counts.batches !== 0 || [...this.issues.values()].some(x => x.sticky)) throw new Error('BOOTSTRAP_REQUIRES_PRISTINE_NEW_GUARD');
    const error = metadataError(batch);
    if (error || !finite(now)) throw new Error(error || 'BOOTSTRAP_CLOCK_INVALID');
    this.boot = batch.bootId; this.cursor = batch.lastSequence; this.first = batch.firstSequence; this.last = batch.lastSequence;
    this.lastServer = batch.observedAtMs; this.lastLocal = now; this.counts.bootstrap_metadata++;
    this.activeFetchError = null;
    for (const problem of this.issues.values()) if (!problem.sticky && problem.category === 'fetch') { problem.active = false; problem.resolved_at_ms = now; }
    if (batch.observedAtMs > now + 1000) this.issue('SERVER_CLOCK_FUTURE', now);
    this.bootstrapRecord = { policy: 'NEW_POSITIONLESS_RESEARCH_START_AT_CURRENT', bootId: batch.bootId,
      initial_cursor: batch.lastSequence, future_sequence_begins_at: batch.lastSequence + 1,
      initial_retained_first: batch.firstSequence, initial_reported_gap: batch.gap,
      initial_prior_history_discarded: true, prior_history_claimed_read: false,
      metadata_events_ignored: batch.events.length, server_observed_ms: batch.observedAtMs, local_received_ms: now,
      declaration: { ...declaration }, position_scope: 'THIS_NEW_RESEARCH_RUN_ONLY_NOT_WALLET_FLATNESS_PROOF',
      warmup: { state: 'WAITING_FOR_FUTURE_PRESSURE', observed_blocks: 0, required_blocks: this.gate.config.min_blocks,
        block_scope: 'ACTUAL_BLOCKS_OBSERVED_IN_POST_ATTACH_PRESSURE_NOT_NECESSARILY_MINED_AFTER_ATTACH' } };
    this.notices.push({ code: 'INITIAL_PRIOR_HISTORY_DISCARDED', severity: 'INFO', sticky: false, active: false,
      through_sequence: batch.lastSequence, recorded_at_ms: now, reason: 'Explicit new observational run; not a history recovery or accounting resume' });
    return JSON.parse(JSON.stringify(this.bootstrapRecord));
  }
  ingest(batch, now = Date.now()) {
    const fail = code => { this.issue(code, now); return false; };
    const error = metadataError(batch);
    if (error) return fail(error);
    if (this.boot !== null && batch.bootId !== this.boot) {
      this.issue('FEED_BOOT_CHANGED', now); this.counts.boot_changes++; this.cursor = 0; this.seen.clear(); this.gate.reset(); this.lastEventRecv = null; this.lastServer = null; this.global = null; this.lastRevision = null; this.revisionWork = null;
      this.latestMarket = {}; this.connections = null;
      if (this.bootstrapRecord) this.bootstrapRecord.warmup.state = 'INVALIDATED_BY_FEED_RESTART';
    }
    this.boot = batch.bootId; this.first = batch.firstSequence; this.last = batch.lastSequence;
    if (batch.gap || (this.cursor === 0 && batch.firstSequence > 1) || (this.cursor > 0 && batch.firstSequence > this.cursor + 1)) this.issue('FEED_GAP', now);
    if (this.cursor > batch.lastSequence) this.issue('CURSOR_AHEAD', now);
    if (this.lastServer !== null && batch.observedAtMs < this.lastServer) this.issue('SERVER_CLOCK_REGRESSION', now);
    if (batch.observedAtMs > now + 1000) this.issue('SERVER_CLOCK_FUTURE', now);
    this.lastServer = batch.observedAtMs; this.lastLocal = now; this.counts.batches++; this.activeFetchError = null;
    if (batch.connection && typeof batch.connection === 'object') this.connections = { present: true, payload_sha256: hash(batch.connection) };
    for (const problem of this.issues.values()) if (!problem.sticky && problem.category === 'fetch') { problem.active = false; problem.resolved_at_ms = now; }
    for (const event of batch.events) {
      if (!event || event.schemaVersion !== 1 || !integer(event.sequence) || event.sequence < 1 || event.sequence < batch.firstSequence || event.sequence > batch.lastSequence ||
          !['market', 'pressure', 'trade'].includes(event.kind) || !['hl', 'bf', 'bs', 'paper'].includes(event.venue) || ![null, 'BTC', 'ETH'].includes(event.symbol) ||
          !finite(event.recv_ms) || !(event.source_ms === null || finite(event.source_ms)) || !event.payload || typeof event.payload !== 'object' || Array.isArray(event.payload) ||
          !((typeof event.provenance === 'string' && event.provenance.length > 0) || (event.provenance && typeof event.provenance === 'object')) ||
          !/^[0-9a-f]{64}$/.test(event.payload_sha256 || '')) { this.issue('EVENT_SCHEMA_INVALID', now); this.counts.rejected++; continue; }
      let digest;
      try { digest = payloadHash(event.payload); } catch { this.issue('PAYLOAD_JSON_INVALID', now); this.counts.rejected++; continue; }
      const identity = hash({ sequence: event.sequence, recv_ms: event.recv_ms, source_ms: event.source_ms, kind: event.kind, venue: event.venue, symbol: event.symbol, payload_sha256: event.payload_sha256, provenance_sha256: hash(event.provenance) });
      if (digest !== event.payload_sha256) { this.issue('PAYLOAD_HASH_MISMATCH', now); this.counts.rejected++; continue; }
      if (event.sequence <= this.cursor) {
        if (this.seen.get(event.sequence) === identity) this.counts.duplicates++;
        else { this.issue(this.seen.has(event.sequence) ? 'SEQUENCE_CONFLICT' : 'UNVERIFIABLE_DUPLICATE', now); this.counts.rejected++; }
        continue;
      }
      if (event.sequence !== this.cursor + 1) this.issue('SEQUENCE_GAP', now);
      if (this.lastEventRecv !== null && event.recv_ms < this.lastEventRecv) this.issue('RECV_CLOCK_REGRESSION', now);
      if (event.recv_ms > batch.observedAtMs + 1000) this.issue('EVENT_RECV_FUTURE', now);
      this.lastEventRecv = event.recv_ms; this.cursor = event.sequence; this.seen.set(event.sequence, identity);
      if (this.seen.size > 4096) this.seen.delete(this.seen.keys().next().value);
      this.counts[event.kind]++;
      if (event.kind === 'pressure') this.pressure(event, now);
      else if (event.kind === 'market' && ['BTC', 'ETH'].includes(event.symbol)) {
        this.latestMarket[event.symbol + ':' + event.venue] = { recv_ms: event.recv_ms, source_ms: event.source_ms, frame_kind: event.payload.frameKind || null,
          bid: typeof event.payload.bid === 'string' ? event.payload.bid : null, ask: typeof event.payload.ask === 'string' ? event.payload.ask : null,
          observed_depth: Array.isArray(event.payload.bids) && Array.isArray(event.payload.asks), payload_sha256: digest };
      }
    }
    return true;
  }
  pressure(event, now) {
    const data = event.payload, s = data.snapshot || data;
    const work = { pending: number(s.totalPendingGas), queued: number(s.totalQueuedGas), submitted: number(s.totalSubmittedGas), count: number(s.pendingCount) };
    const revision = typeof s.queueRevision === 'string' ? s.queueRevision : typeof data.queueRevision === 'string' ? data.queueRevision : null;
    if (revision !== null && revision === this.lastRevision && this.revisionWork !== hash(work)) this.issue('PRESSURE_REVISION_CONFLICT', now);
    this.lastRevision = revision; this.revisionWork = hash(work);
    for (const problem of this.issues.values()) if (!problem.sticky && problem.category === 'pressure') { problem.active = false; problem.resolved_at_ms = now; }
    if (s.chain && (s.chain.ok === false || s.chain.stale === true)) this.issue('CHAIN_PRESSURE_UNHEALTHY', now, false, 'pressure');
    if (number(s.ts) !== null && event.source_ms !== null && Math.abs(number(s.ts) - event.source_ms) > 1) this.issue('PRESSURE_SOURCE_CLOCK_MISMATCH', now);
    this.gate.observe(data, event.recv_ms);
    for (const code of this.gate.snapshot?.invalid || []) this.issue(code, now, ['PRESSURE_SOURCE_REGRESSION', 'PRESSURE_RECEIVE_REGRESSION', 'CONFLICTING_PRESSURE_REVISION', 'AMBIGUOUS_PRESSURE_REVISION', 'BLOCK_REVISION_CONFLICT', 'FUTURE_BLOCK'].includes(code), 'pressure');
    if (this.bootstrapRecord && this.bootstrapRecord.bootId === this.boot) {
      const warmup = this.bootstrapRecord.warmup;
      warmup.observed_blocks = this.gate.blocks.size;
      warmup.state = !this.gate.snapshot.invalid.length && warmup.observed_blocks >= warmup.required_blocks ? 'READY' : 'WAITING_FOR_FUTURE_PRESSURE';
    }
    const blocks = Array.isArray(s.chain?.blocks) ? s.chain.blocks : [];
    this.global = { scope: 'OBSERVED_GLOBAL_NOT_PERSONAL_RANK', bootId: this.boot, sequence: event.sequence, recv_ms: event.recv_ms, source_ms: event.source_ms,
      payload_sha256: event.payload_sha256, hash_kind: 'CANONICAL_LOCAL_JSON_NOT_WIRE', pending_gas: work.pending, queued_gas: work.queued, submitted_gas: work.submitted,
      pending_count: work.count, block_kinds: blocks.reduce((a, b) => { const k = typeof b?.kind === 'string' ? b.kind : 'unknown'; a[k] = (a[k] || 0) + 1; return a; }, {}) };
    const prediction = { schemaVersion: 1, runId: this.runId, bootId: this.boot, sequence: event.sequence,
      prediction_at_ms: now, information_available_server_recv_ms: event.recv_ms, source_ms: event.source_ms,
      payload_sha256: event.payload_sha256, hash_kind: 'CANONICAL_LOCAL_JSON_NOT_WIRE', payload_hash_representation: 'JSON_STRINGIFY_NORMALIZED_PAYLOAD_V1',
      input_sha256: hash({ bootId: this.boot, sequence: event.sequence, recv_ms: event.recv_ms, payload: data }), input_hash_representation: 'RECURSIVE_SORTED_LOCAL_JSON_V1',
      model_config_sha256: this.modelHash, implementation_sha256: this.implementationHash, provenance_sha256: hash(event.provenance), input_pressure: JSON.parse(JSON.stringify(data)),
      clocks: { prediction: 'guard_local_wall_ms', availability: 'dashboard_server_recv_ms', causal_order: 'bootId_and_sequence', clock_offset_verified: false },
      mode: MODE, heuristic_timing: this.gate.evaluate(now, true), financial_mutations: 0 };
    this.predictions++;
    if (this.onPrediction) { try { this.onPrediction(prediction); } catch { this.issue('EVIDENCE_SINK_FAILED', now); } }
  }
  status(now = Date.now()) {
    const timing = this.gate.evaluate(now, true), reasons = [...timing.reasons];
    if (this.lastLocal === null) reasons.push('FEED_MISSING');
    else if (now - this.lastLocal > this.maxAge || now < this.lastLocal) reasons.push('FEED_STALE_OR_LOCAL_CLOCK_REGRESSION');
    if (this.lastServer !== null && now - this.lastServer > this.maxAge) reasons.push('SERVER_RECEIVE_CLOCK_STALE');
    if (this.activeFetchError) reasons.push(this.activeFetchError);
    if (this.bootstrapRecord && this.bootstrapRecord.warmup.state !== 'READY') reasons.push('BOOTSTRAP_PRESSURE_WARMUP');
    for (const issue of this.issues.values()) if (issue.active) reasons.push(issue.code);
    const active = [...new Set(reasons)].sort(), pass = timing.allow && !active.length;
    const measured = support(this.empirical, this.selector);
    return { schemaVersion: 1, runId: this.runId, mode: MODE, updated_at_ms: now, financial_mutations: 0, live_permission: false,
      status: pass ? 'ALLOW_SHADOW' : 'SUPPRESSED', meaning: 'TIMING_FILTER_PASS_ONLY_NOT_PROFITABLE_SAFE_OR_LIVE_PERMISSION', reason_codes: active,
      source: { endpoint: ENDPOINT, method: 'GET', redirects: false, bootId: this.boot, cursor: this.cursor, firstSequence: this.first, lastSequence: this.last,
        server_observed_ms: this.lastServer, local_received_ms: this.lastLocal, clock_domains: 'Server receipt, source clock, local receipt remain distinct; offset unverified', connection: this.connections },
      continuity: { sticky_quarantine: [...this.issues.values()].some(x => x.active && x.sticky), issues: [...this.issues.values()],
        notices: this.notices.map(x => ({ ...x })), bootstrap: this.bootstrapRecord ? JSON.parse(JSON.stringify(this.bootstrapRecord)) : null },
      observed_global_pressure: this.global, heuristic_timing: timing,
      pressure_freshness: { local_age_ms: this.lastLocal === null ? null : now - this.lastLocal, server_pressure_recv_age_ms: this.global ? now - this.global.recv_ms : null,
        source_age_ms: this.global?.source_ms === null || !this.global ? null : now - this.global.source_ms },
      measured_cohort: measured, measured_catalog: this.empirical ? { sample_rows: this.empirical.sample_rows, unique_qualified_intervals: this.empirical.unique_qualified_intervals,
        cohort_count: this.empirical.cohorts?.length, calibration: this.empirical.calibration, data_sha256: this.empirical.input_manifest_sha256 } : null,
      request_specific: { status: 'UNKNOWN_NO_MATCHED_REQUEST_FEED', asset: this.selector.asset || null, notional_usd: this.selector.notional_usd ?? null, intent_id: null, rank: null, accepted_at_ms: null, measured_submit_to_execute_ms: null },
      data_support_gaps: ['NO_PERSONAL_REQUEST_QUEUE_OR_RANK', 'NO_MATCHED_DECISION_AND_TERMINAL_IN_PUBLIC_FEED', 'NO_VALIDATED_EXPECTED_PROFIT_MODEL', ...(measured.status.startsWith('OUT_OF_SUPPORT') || measured.status.startsWith('UNKNOWN') ? [measured.status] : [])],
      deadline: this.empirical?.deadline_protection || { status: 'SEPARATE_ONCHAIN_PROTECTION_NOT_IMPLEMENTED_HERE' },
      latest_market: this.latestMarket, counts: this.counts, predictions: this.predictions };
  }
}
module.exports = { FeedGuard, fetchBatch, atomicWrite, assertEndpoint, support, canonical, hash, payloadHash, ENDPOINT, MODE, NEW_POSITIONLESS_OBSERVATION };
