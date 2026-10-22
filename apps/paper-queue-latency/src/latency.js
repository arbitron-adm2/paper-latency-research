// Timing-only browser counterpart of marketdb/latency_gate.py. No order/signing APIs.
(function (root) {
  'use strict';
  const DEFAULTS = Object.freeze({ enabled: true, max_pressure_age_ms: 3000, max_source_future_ms: 1000,
    max_block_age_ms: 5000, window_blocks: 20, min_blocks: 3, entry_floor_ms: 3000,
    exit_floor_ms: 10000, safety_factor: 1.5, entry_budget_ms: 6000, exit_budget_ms: 15000, signal_ttl_ms: 5000 });
  function number(x) {
    if (typeof x !== 'number' && typeof x !== 'string') return null;
    if (typeof x === 'string' && !/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(x.trim())) return null;
    const n = Number(x); return Number.isFinite(n) && n >= 0 ? n : null;
  }
  function validate(input = {}) {
    if (!input || typeof input !== 'object' || Array.isArray(input) || Object.keys(input).some(k => !Object.hasOwn(DEFAULTS, k))) throw new Error('INVALID_CONFIG');
    const c = { ...DEFAULTS, ...input };
    if (typeof c.enabled !== 'boolean') throw new Error('INVALID_CONFIG');
    for (const [k, v] of Object.entries(c)) if (k !== 'enabled' && (typeof v !== 'number' || !Number.isFinite(v) || v < 0)) throw new Error('INVALID_CONFIG');
    if (!Number.isInteger(c.min_blocks) || !Number.isInteger(c.window_blocks) || c.min_blocks < 2 || c.window_blocks < c.min_blocks || c.window_blocks > 100 || c.safety_factor < 1) throw new Error('INVALID_CONFIG');
    for (const k of ['max_pressure_age_ms', 'max_block_age_ms', 'entry_budget_ms', 'exit_budget_ms', 'signal_ttl_ms']) if (c[k] <= 0) throw new Error('INVALID_CONFIG');
    return c;
  }
  function median(a) { const b = [...a].sort((x, y) => x - y), n = b.length; return n % 2 ? b[(n - 1) / 2] : (b[n / 2 - 1] + b[n / 2]) / 2; }
  function empty() { return { status: 'SUPPRESSED', allow: false, reasons: [], entry_ms: null, exit_ms: null,
    method: 'paper_workload_heuristic_v1', estimate_kind: 'HEURISTIC_NOT_QUANTILE_OR_BOUND', economics: 'NOT_MODELLED',
    mode: 'SHADOW_NO_ORDERS', evidence: null, drain_gas_per_second: null, sample_blocks: 0 }; }
  class TimingGate {
    constructor(config = {}) { this.config = validate(config); this.reset(); }
    reset() {
      this.snapshot = null; this.blocks = new Map(); this.lastSource = null; this.lastRecv = null; this.seq = 0;
      this.revisionWork = new Map(); this.stickyInvalid = new Set();
    }
    observe(data, now = Date.now()) {
      const cfg = this.config, raw = data && (data.snapshot || data);
      const s = raw && typeof raw === 'object' && !Array.isArray(raw) ? raw : {};
      const bad = [], ts = number(s.ts), pending = number(s.totalPendingGas), recv = number(now);
      const queued = number(s.totalQueuedGas), submitted = number(s.totalSubmittedGas), count = number(s.pendingCount);
      const explicit = v => typeof v === 'string' && v.length > 0 ? v : null;
      const innerRevision = explicit(s.queueRevision), outerRevision = explicit(data?.queueRevision);
      const revision = innerRevision ?? outerRevision, previous = this.snapshot;
      const workload = JSON.stringify([pending, queued, submitted, count]);
      const relation = !previous ? 'INITIAL_OBSERVATION' : revision !== null && previous.queue_revision !== null
        ? (revision === previous.queue_revision ? 'SAME_EXPLICIT_REVISION' : 'DISTINCT_EXPLICIT_REVISION') : 'REVISION_UNKNOWN';
      if (s.ok !== true || s.stale !== false || ts === null || pending === null) bad.push('INVALID_OR_UNHEALTHY_PRESSURE');
      if (recv === null) bad.push('MISSING_LOCAL_PRESSURE_CLOCK');
      else if (this.lastRecv !== null && recv < this.lastRecv) bad.push('PRESSURE_RECEIVE_REGRESSION');
      if (this.lastSource !== null && ts !== null && ts < this.lastSource) bad.push('PRESSURE_SOURCE_REGRESSION');
      if (innerRevision !== null && outerRevision !== null && innerRevision !== outerRevision) bad.push('CONFLICTING_PRESSURE_REVISION');
      // Opaque versions are compared for identity only, never sorted or parsed.
      // Equal source milliseconds do not imply equal queue observations.
      if (revision !== null && this.revisionWork.has(revision) && this.revisionWork.get(revision) !== workload) bad.push('CONFLICTING_PRESSURE_REVISION');
      if (previous && ts !== null && ts === previous.source_ms && workload !== previous.workload_key &&
          (revision === null || previous.queue_revision === null)) bad.push('AMBIGUOUS_PRESSURE_REVISION');
      if (ts !== null && !bad.length) this.lastSource = ts;
      for (const k of ['totalQueuedGas', 'totalSubmittedGas', 'pendingCount']) if (k in s && number(s[k]) === null) bad.push('INVALID_GAS_OR_COUNT');
      if (queued !== null && submitted !== null && pending !== null && Math.abs(queued + submitted - pending) > 1) bad.push('INCONSISTENT_PENDING_GAS');
      if (pending === 0 && number(s.pendingCount) > 0) bad.push('INCONSISTENT_PENDING_COUNT');
      const blocks = s.chain && s.chain.blocks, normalized = new Map();
      if (blocks !== undefined && !Array.isArray(blocks)) bad.push('MALFORMED_BLOCKS');
      for (const b of Array.isArray(blocks) ? blocks : []) {
        if (!b || typeof b !== 'object') { bad.push('MALFORMED_BLOCK'); continue; }
        const ordinary = ['ordinary', 'normal', 'small'].includes(b.kind);
        if (!ordinary && !['big', 'large'].includes(b.kind)) { bad.push('UNKNOWN_BLOCK_KIND'); continue; }
        const n = number(b.number), at = number(b.timestampMs), gas = number(b.papertradeGasUsed);
        if (n === null || !Number.isSafeInteger(n) || at === null || (ordinary && gas === null)) { bad.push('MALFORMED_BLOCK'); continue; }
        if (ts !== null && at > ts + cfg.max_source_future_ms) { bad.push('FUTURE_BLOCK'); continue; }
        const item = { number: n, timestamp_ms: at, paper_gas: gas, ordinary }, old = normalized.get(n) || this.blocks.get(n);
        if (old && (old.number !== n || old.timestamp_ms !== at || old.paper_gas !== gas || (old.ordinary !== undefined && old.ordinary !== ordinary))) bad.push('BLOCK_REVISION_CONFLICT');
        normalized.set(n, item);
      }
      if (!bad.length) {
        this.lastSource = ts;
        if (revision !== null) {
          this.revisionWork.delete(revision); this.revisionWork.set(revision, workload);
          // Bounded recent identity evidence, not a claim of lifetime uniqueness.
          if (this.revisionWork.size > 512) this.revisionWork.delete(this.revisionWork.keys().next().value);
        }
        for (const [k, v] of normalized) this.blocks.set(k, v);
        const recent = [...this.blocks.values()].sort((a, b) => a.timestamp_ms - b.timestamp_ms || a.number - b.number).slice(-cfg.window_blocks);
        this.blocks = new Map(recent.map(b => [b.number, b]));
      }
      if (recv !== null) this.lastRecv = this.lastRecv === null ? recv : Math.max(this.lastRecv, recv);
      for (const code of bad) if (['PRESSURE_SOURCE_REGRESSION', 'PRESSURE_RECEIVE_REGRESSION',
        'CONFLICTING_PRESSURE_REVISION', 'AMBIGUOUS_PRESSURE_REVISION', 'BLOCK_REVISION_CONFLICT', 'FUTURE_BLOCK'].includes(code)) this.stickyInvalid.add(code);
      this.snapshot = { time_ms: now, recv_ms: now, source_ms: ts, pending_gas: pending,
        event_seq: ++this.seq, queue_revision: revision, revision_relation: relation, workload_key: workload,
        invalid: [...new Set([...bad, ...this.stickyInvalid])].sort() };
    }
    evaluate(now = Date.now(), direct = true) {
      const r = empty(), c = this.config, s = this.snapshot;
      if (!direct) { r.reasons = ['DIRECT_PRESSURE_UNAVAILABLE']; return r; }
      if (!s) { r.reasons = ['PRESSURE_MISSING']; return r; }
      r.evidence = { time_ms: s.time_ms, recv_ms: s.recv_ms, source_ms: s.source_ms, event_seq: s.event_seq, queue_revision: s.queue_revision,
        revision_relation: s.revision_relation, revision_order_basis: 'RECEIVE_OBSERVATION_SEQUENCE_NOT_OPAQUE_TOKEN_ORDER', revision_history_limit: 512 };
      if (number(now) === null) { r.reasons = ['DECISION_CLOCK_INVALID']; return r; }
      const bad = [...s.invalid];
      for (const k of ['time_ms', 'recv_ms', 'source_ms']) {
        const at = number(s[k]);
        if (at === null) bad.push('PRESSURE_CLOCK_UNKNOWN');
        else if (now - at < -c.max_source_future_ms) bad.push('PRESSURE_FROM_FUTURE');
        else if (now - at > c.max_pressure_age_ms) bad.push('PRESSURE_STALE');
      }
      if (bad.length) { r.reasons = [...new Set(bad)].sort(); return r; }
      let work = 0;
      if (s.pending_gas === 0) r.method = 'configured_floors_empty_queue_v1';
      else {
        const coverage = [...this.blocks.values()].sort((a, b) => a.number - b.number);
        if (coverage.slice(1).some((b, i) => b.number !== coverage[i].number + 1)) { r.reasons = ['BLOCK_COVERAGE_GAP']; return r; }
        // A known large block establishes coverage, but is not ordinary queue drain.
        const bs = coverage.filter(b => b.ordinary !== false).sort((a, b) => a.timestamp_ms - b.timestamp_ms || a.number - b.number);
        r.sample_blocks = bs.length;
        if (bs.length < c.min_blocks) { r.reasons = ['INSUFFICIENT_DRAIN_HISTORY']; return r; }
        if (s.source_ms - bs.at(-1).timestamp_ms > c.max_block_age_ms) { r.reasons = ['BLOCK_HISTORY_STALE']; return r; }
        const gaps = bs.slice(1).map((b, i) => b.timestamp_ms - bs[i].timestamp_ms);
        if (gaps.some(x => x <= 0) || bs.slice(1).some((b, i) => b.number <= bs[i].number)) { r.reasons = ['INVALID_BLOCK_ORDER']; return r; }
        const seconds = (bs.at(-1).timestamp_ms - bs[0].timestamp_ms + median(gaps)) / 1000;
        const drain = bs.reduce((sum, b) => sum + b.paper_gas, 0) / seconds;
        if (!(seconds > 0) || !Number.isFinite(seconds) || !Number.isFinite(drain)) { r.reasons = ['INVALID_DRAIN_ESTIMATE']; return r; }
        r.drain_gas_per_second = drain;
        if (drain <= 0) { r.reasons = ['DRAIN_STALLED']; return r; }
        work = c.safety_factor * s.pending_gas / drain * 1000;
      }
      const entry = c.entry_floor_ms + work, exit = c.exit_floor_ms + work;
      if (!Number.isFinite(entry + exit)) { r.reasons = ['ESTIMATE_OVERFLOW']; return r; }
      r.entry_ms = entry; r.exit_ms = exit;
      if (entry > c.entry_budget_ms) r.reasons.push('ENTRY_DELAY_BUDGET');
      if (entry >= c.signal_ttl_ms) r.reasons.push('EDGE_LIFETIME_BUDGET');
      if (exit > c.exit_budget_ms) r.reasons.push('EXIT_DELAY_BUDGET');
      r.allow = !r.reasons.length; r.status = r.allow ? 'ALLOW_SHADOW' : 'SUPPRESSED'; return r;
    }
  }
  const RU = Object.freeze({
    DIRECT_PRESSURE_UNAVAILABLE: 'Нет прямого потока биржи', PRESSURE_MISSING: 'Жду данные очереди',
    INVALID_CONFIG: 'Проверьте лимиты в настройках', PRESSURE_STALE: 'Данные очереди устарели',
    INVALID_OR_UNHEALTHY_PRESSURE: 'Биржа сообщает неполные или устаревшие данные',
    ENTRY_DELAY_BUDGET: 'Вход дольше заданного лимита', EDGE_LIFETIME_BUDGET: 'Вход позже срока жизни сигнала',
    EXIT_DELAY_BUDGET: 'Выход дольше заданного лимита', INSUFFICIENT_DRAIN_HISTORY: 'Недостаточно блоков для оценки',
    DRAIN_STALLED: 'Обработка очереди остановилась', BLOCK_COVERAGE_GAP: 'Разрыв в последовательности блоков',
    BLOCK_HISTORY_STALE: 'Нет свежих блоков', PRESSURE_SOURCE_REGRESSION: 'Часы потока пошли назад',
    BLOCK_REVISION_CONFLICT: 'Противоречащие данные блока', UNKNOWN_BLOCK_KIND: 'Неизвестный тип блока',
    CONFLICTING_PRESSURE_REVISION: 'Одна версия очереди содержит противоречащие данные',
    AMBIGUOUS_PRESSURE_REVISION: 'Изменение очереди в одну миллисекунду без явных версий: неизвестно',
    PRESSURE_RECEIVE_REGRESSION: 'Часы приёма потока пошли назад',
    PRESSURE_FROM_FUTURE: 'Часы компьютера и биржи расходятся', INVALID_GAS_OR_COUNT: 'Повреждены численные поля',
    INCONSISTENT_PENDING_GAS: 'Противоречащие данные объёма очереди', ESTIMATE_OVERFLOW: 'Оценка вышла за численный диапазон',
  });
  const api = { TimingGate, DEFAULTS, validate, number, empty, reason: code => RU[code] || 'Неполные данные: ' + code };
  if (typeof module !== 'undefined' && module.exports) module.exports = api; else root.PTQLatency = api;
})(typeof globalThis !== 'undefined' ? globalThis : this);
