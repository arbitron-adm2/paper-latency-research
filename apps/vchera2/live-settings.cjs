'use strict';
// Draft data only. No runtime, HTTP, wallet, signer, scheduler or strategy imports.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const MAX_BODY_BYTES = 16384, MAX_FILE_BYTES = 2 * 1024 * 1024;
const MONEY_MAX = 1000000000n * 1000000n;
const FEATURES = Object.freeze({ spreadEnabled: true, bookEnabled: true, bookUseHl: true, bookUseBf: true,
  bookUseBs: false, bookExitOnFlip: true, cushBoth: false, noLongs: false });
const FEATURE_REGISTRY = Object.freeze(Object.fromEntries(Object.keys(FEATURES).map(key => [key,
  Object.freeze({ supported: true, cfgKey: key, optional: true, source: 'launch-config.strategy override; verified scanner boolean default otherwise' })])));
const LOCKED_GUARDS = Object.freeze({ wallGuard: true, g3Enabled: true, freshness: true, unknownLedger: true,
  signedDeadline: true, marginSafety: true, highWater: true, independentClose: true, btcOnly: true, noSharedWaitLock: true });
const copy = v => JSON.parse(JSON.stringify(v));
function plain(v) {
  return !!v && typeof v === 'object' && !Array.isArray(v) && [Object.prototype, null].includes(Object.getPrototypeOf(v)) &&
    !Object.getOwnPropertySymbols(v).length && Object.values(Object.getOwnPropertyDescriptors(v)).every(d => !d.get && !d.set);
}
function keys(v, expected) { return plain(v) && Object.keys(v).length === expected.length && expected.every(k => Object.hasOwn(v, k)); }
function canonical(v) {
  if (v === null || typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) return '[' + v.map(canonical).join(',') + ']';
  return '{' + Object.keys(v).sort().map(k => JSON.stringify(k) + ':' + canonical(v[k])).join(',') + '}';
}
const configHash = settings => crypto.createHash('sha256').update(canonical(settings)).digest('hex');
function fail(code) { throw Object.assign(new Error(code), { code }); }
function clock(now) { if (!Number.isSafeInteger(now) || now < 0 || now > 8640000000000000) fail('INVALID_CLOCK'); return now; }
function units(value, { zero = false } = {}) {
  if (!['string', 'number'].includes(typeof value) || (typeof value === 'number' && !Number.isFinite(value))) fail('INVALID_DECIMAL');
  const s = String(value);
  if (!/^(?:0|[1-9]\d{0,9})(?:\.\d{1,6})?$/.test(s)) fail('INVALID_DECIMAL');
  const [a, b = ''] = s.split('.'), n = BigInt(a) * 1000000n + BigInt(b.padEnd(6, '0'));
  if (n > MONEY_MAX || (zero ? n < 0n : n <= 0n)) fail('DECIMAL_OUT_OF_BOUNDS');
  return n;
}
function decimal(n) {
  const tail = String(n % 1000000n).padStart(6, '0').replace(/0+$/, '');
  return String(n / 1000000n) + (tail ? '.' + tail : '');
}
function maybeUnits(value, zero = false) { try { return units(value, { zero }); } catch { return null; } }
function validDate(s) {
  if (typeof s !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const n = Date.parse(s + 'T00:00:00Z');
  return Number.isFinite(n) && new Date(n).toISOString().slice(0, 10) === s && Number(s.slice(0, 4)) >= 2000 && Number(s.slice(0, 4)) <= 2100;
}
function validZone(zone) {
  if (typeof zone !== 'string' || zone.length < 1 || zone.length > 100) return false;
  try { new Intl.DateTimeFormat('en-GB', { timeZone: zone }).format(0); return true; } catch { return false; }
}
function makeDefaults(defaults = {}) {
  if (!plain(defaults)) fail('INVALID_DEFAULTS');
  const strategy = Object.hasOwn(defaults, 'strategy') ? defaults.strategy : defaults;
  if (!plain(strategy)) fail('INVALID_DEFAULTS');
  const features = { ...FEATURES };
  for (const key of Object.keys(features)) if (Object.hasOwn(strategy, key)) {
    if (typeof strategy[key] !== 'boolean') fail('INVALID_DEFAULT_FEATURE');
    features[key] = strategy[key];
  }
  return { symbol: 'BTC', capital: { budgetUsdc: null, marginUsdc: null, leverage: null },
    schedule: { enabled: false, kind: 'daily', timezone: 'Europe/Moscow', start: '09:00', stop: '18:00',
      weekdays: [1, 2, 3, 4, 5, 6, 7], dateFrom: null, dateTo: null }, features };
}
function validateSchedule(s) {
  if (!keys(s, ['enabled', 'kind', 'timezone', 'start', 'stop', 'weekdays', 'dateFrom', 'dateTo'])) fail('INVALID_SCHEDULE_KEYS');
  if (typeof s.enabled !== 'boolean' || s.kind !== 'daily') fail('UNSUPPORTED_SCHEDULE_KIND');
  if (s.timezone !== null && !validZone(s.timezone)) fail('INVALID_IANA_TIMEZONE');
  for (const name of ['start', 'stop']) if (s[name] !== null && (typeof s[name] !== 'string' || !/^(?:[01]\d|2[0-3]):[0-5]\d$/.test(s[name]))) fail('INVALID_LOCAL_TIME');
  if (s.start !== null && s.start === s.stop) fail('EQUAL_START_STOP_NOT_24H');
  if (!Array.isArray(s.weekdays) || !s.weekdays.length || s.weekdays.length > 7 || s.weekdays.some(d => !Number.isInteger(d) || d < 1 || d > 7) || new Set(s.weekdays).size !== s.weekdays.length) fail('INVALID_WEEKDAYS');
  for (const name of ['dateFrom', 'dateTo']) if (s[name] !== null && !validDate(s[name])) fail('INVALID_DATE_BOUND');
  if (s.dateFrom && s.dateTo && s.dateFrom > s.dateTo) fail('REVERSED_DATE_BOUNDS');
  return { enabled: s.enabled, kind: 'daily', timezone: s.timezone, start: s.start, stop: s.stop,
    weekdays: [...s.weekdays].sort((a, b) => a - b), dateFrom: s.dateFrom, dateTo: s.dateTo };
}
function validateSettings(input) {
  try {
    if (!keys(input, ['symbol', 'capital', 'schedule', 'features'])) fail('INVALID_SETTINGS_KEYS');
    if (input.symbol !== 'BTC') fail('BTC_ONLY_ETH_OBSERVE_ONLY');
    if (!keys(input.capital, ['budgetUsdc', 'marginUsdc', 'leverage'])) fail('INVALID_CAPITAL_KEYS');
    const c = input.capital, capital = {};
    for (const key of ['budgetUsdc', 'marginUsdc']) capital[key] = c[key] === null ? null : decimal(units(c[key]));
    if (c.leverage !== null && (!Number.isInteger(c.leverage) || c.leverage < 1 || c.leverage > 1000)) fail('INVALID_LEVERAGE');
    capital.leverage = c.leverage;
    if (capital.marginUsdc !== null && capital.budgetUsdc !== null && units(capital.marginUsdc) > units(capital.budgetUsdc)) fail('MARGIN_EXCEEDS_BUDGET');
    if (!keys(input.features, Object.keys(FEATURES)) || Object.values(input.features).some(v => typeof v !== 'boolean')) fail('INVALID_OR_LOCKED_FEATURE');
    if (!input.features.spreadEnabled && !input.features.bookEnabled) fail('NO_STRATEGY_BRANCH');
    if (input.features.bookEnabled && !['bookUseHl', 'bookUseBf', 'bookUseBs'].some(k => input.features[k])) fail('BOOK_REQUIRES_SELECTED_VENUE');
    const settings = { symbol: 'BTC', capital, schedule: validateSchedule(input.schedule), features: { ...input.features } };
    return { ok: true, reasons: [], settings };
  } catch (error) { return { ok: false, reasons: [error.code || 'INVALID_SETTINGS'], settings: null }; }
}
function toStrategyPatch(settings) {
  const v = validateSettings(settings); if (!v.ok) fail(v.reasons[0]);
  const c = v.settings.capital;
  if (c.budgetUsdc === null || c.marginUsdc === null || c.leverage === null) fail('INCOMPLETE_CAPITAL');
  return { margin: Number(c.marginUsdc), leverage: c.leverage, ...v.settings.features };
}
function walletView(context, now) {
  const w = context?.provider?.wallet;
  const age = Number.isSafeInteger(w?.t) ? now - w.t : null;
  const fresh = context?.providerFresh === true && w?.connected === true && !w?.err && age !== null && age >= 0 && age <= 3000;
  const available = maybeUnits(w?.available, true), balance = maybeUnits(w?.balance, true);
  return { readonly: true, source: 'PARENT_CONTEXT_NOT_REQUEST_BODY', fresh, ageMs: age,
    availableUsdc: available === null ? null : decimal(available), balanceUsdc: balance === null ? null : decimal(balance),
    positionsCount: Array.isArray(w?.openPositions) ? w.openPositions.length : null,
    pendingCount: Array.isArray(w?.operations) ? w.operations.length : null,
    managedCount: Array.isArray(context?.openManaged) ? context.openManaged.length : null };
}
function stopFirstBlockers(context, now) {
  const r = [], w = walletView(context, now), unknown = context?.provider?.entrySafety?.unknown;
  if (context?.LIVE !== false || context?.entriesEnabled !== false) r.push('STOP_FIRST_LIVE_OR_ENTRY_STATE');
  if (!Array.isArray(unknown) || unknown.length) r.push('UNKNOWN_ENTRY_LEDGER');
  if (!w.fresh) r.push('WALLET_UNKNOWN_OR_STALE');
  for (const [key, code] of [['positionsCount', 'WALLET_POSITIONS'], ['pendingCount', 'WALLET_PENDING_OPERATIONS'], ['managedCount', 'MANAGED_EXPOSURE']]) {
    if (w[key] === null || w[key] !== 0) r.push(code);
  }
  return r;
}
function confirmed(obj, now) { return plain(obj) && obj.confirmed === true && Number.isSafeInteger(obj.updatedAtMs) && now >= obj.updatedAtMs && now - obj.updatedAtMs <= 30000; }
function readiness(settings, context, now, revision = 0, storageError = null) {
  const w = walletView(context, now), r = [...stopFirstBlockers(context, now)];
  if (storageError) r.push(storageError);
  if (!revision) r.push('DRAFT_NOT_SAVED');
  if (!settings) return { preflightReady: false, readyToStart: false, draftComplete: false, blockers: [...new Set([...r, 'SETTINGS_UNAVAILABLE'])], wallet: w, derived: null };
  const c = settings.capital, budget = c.budgetUsdc === null ? null : units(c.budgetUsdc), margin = c.marginUsdc === null ? null : units(c.marginUsdc);
  const complete = budget !== null && margin !== null && c.leverage !== null;
  if (!complete) r.push('CAPITAL_NOT_SELECTED');
  const notional = margin !== null && c.leverage !== null ? margin * BigInt(c.leverage) : null;
  const limits = context?.instrumentLimits, risk = context?.riskBudget;
  const limitsKnown = confirmed(limits, now) && limits.symbol === 'BTC' && Number.isInteger(limits.maxLeverage) && limits.maxLeverage >= 1 && limits.maxLeverage <= 1000;
  if (!limitsKnown) r.push('BTC_INSTRUMENT_LIMITS_UNKNOWN_OR_STALE');
  const riskKnown = confirmed(risk, now);
  if (!riskKnown) r.push('RISK_BUDGET_UNKNOWN_OR_STALE');
  const reserve = riskKnown ? maybeUnits(risk.reserveUsdc, true) : null;
  if (reserve === null) r.push('RESERVE_UNKNOWN');
  const available = maybeUnits(w.availableUsdc, true), balance = maybeUnits(w.balanceUsdc, true);
  if (available === null || balance === null || available > balance) r.push('ACTUAL_FUNDS_UNKNOWN_OR_INCONSISTENT');
  if (available !== null && reserve !== null) {
    if (budget !== null && budget + reserve > available) r.push('AVAILABLE_BELOW_BUDGET_PLUS_RESERVE');
    if (margin !== null && margin + reserve > available) r.push('AVAILABLE_BELOW_MARGIN_PLUS_RESERVE');
  }
  if (limitsKnown) {
    if (c.leverage !== null && c.leverage > limits.maxLeverage) r.push('LEVERAGE_EXCEEDS_CONFIRMED_LIMIT');
    for (const [key, value, minimum] of [['minMarginUsdc', margin, true], ['minNotionalUsd', notional, true], ['maxNotionalUsd', notional, false]]) {
      const limit = maybeUnits(limits[key]);
      if (limit === null) r.push('INSTRUMENT_' + key.toUpperCase() + '_UNKNOWN');
      else if (value !== null && (minimum ? value < limit : value > limit)) r.push('INSTRUMENT_' + key.toUpperCase() + '_VIOLATION');
    }
  }
  if (riskKnown) for (const [key, value] of [['maxBudgetUsdc', budget], ['maxNotionalUsd', notional]]) {
    const limit = maybeUnits(risk[key]);
    if (limit === null) r.push('RISK_' + key.toUpperCase() + '_UNKNOWN');
    else if (value !== null && value > limit) r.push('RISK_' + key.toUpperCase() + '_VIOLATION');
  }
  if (context?.contractCapability?.btcEntryWithDeadline !== true || context?.contractCapability?.independentClose !== true) r.push('CONTRACT_CAPABILITY_UNCONFIRMED');
  if (context?.safetyReady !== true) r.push('LOCKED_RUNTIME_SAFETY_NOT_CONFIRMED');
  let windowPreview = null;
  if (settings.schedule.enabled) { windowPreview = previewSchedule(settings.schedule, now); r.push(...windowPreview.blockers); }
  const draftComplete = complete && (!settings.schedule.enabled || (settings.schedule.timezone !== null && settings.schedule.start !== null && settings.schedule.stop !== null));
  return { preflightReady: r.length === 0, readyToStart: false, draftComplete, blockers: [...new Set(r)], wallet: w,
    derived: { notionalUsd: notional === null ? null : decimal(notional), reserveUsdc: reserve === null ? null : decimal(reserve),
      requiredAvailableUsdc: budget === null || reserve === null ? null : decimal(budget + reserve), budgetIsWalletBalance: false }, windowPreview };
}
const formatters = new Map();
function formatter(zone) {
  if (!formatters.has(zone)) {
    if (formatters.size >= 32) formatters.delete(formatters.keys().next().value);
    formatters.set(zone, new Intl.DateTimeFormat('en-GB-u-ca-iso8601-nu-latn', { timeZone: zone, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' }));
  }
  return formatters.get(zone);
}
function localParts(ms, zone) { return Object.fromEntries(formatter(zone).formatToParts(ms).filter(p => p.type !== 'literal').map(p => [p.type, p.value])); }
function dayAt(ms, zone) { const p = localParts(ms, zone); return `${p.year}-${p.month}-${p.day}`; }
function addDay(day, n) { return new Date(Date.parse(day + 'T00:00:00Z') + n * 86400000).toISOString().slice(0, 10); }
function weekday(day) { return ((new Date(day + 'T00:00:00Z').getUTCDay() + 6) % 7) + 1; }
function resolveLocal(day, time, zone) {
  const nominal = Date.parse(`${day}T${time}:00Z`), offsets = new Set();
  // Conservative support: decline the whole +/-36h transition vicinity, not just ambiguous minutes.
  // Supported civil dates are 2000..2100. No cron, timer, guessed fold or automatic DST correction.
  if (!validDate(day)) return { ok: false, reason: 'CIVIL_DATE_UNSUPPORTED' };
  for (let minutes = -2160; minutes <= 2160; minutes += 30) {
    const at = nominal + minutes * 60000, p = localParts(at, zone);
    offsets.add(Date.parse(`${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}:${p.second}Z`) - at);
  }
  if (offsets.size !== 1) return { ok: false, reason: 'DST_TRANSITION_WINDOW_UNSUPPORTED' };
  const at = nominal - [...offsets][0], p = localParts(at, zone);
  if (`${p.year}-${p.month}-${p.day}` !== day || `${p.hour}:${p.minute}` !== time || p.second !== '00') return { ok: false, reason: 'LOCAL_TIME_UNRESOLVED' };
  return { ok: true, atMs: at, at: new Date(at).toISOString() };
}
function previewSchedule(input, now = Date.now()) {
  try {
    clock(now); const s = validateSchedule(input);
    const result = { planOnly: true, effectiveScheduleEnabled: false, registered: false, weekdaySemantics: 'START_DAY_ISO_1_MON_7_SUN',
      dateBoundsSemantics: 'INCLUSIVE_LOCAL_START_DAY_STOP_MAY_FALL_ON_NEXT_DAY', currentWindow: null, nextWindow: null, blockers: [] };
    if (!s.enabled) return { ...result, status: 'DISABLED_DRAFT' };
    if (s.timezone === null || s.start === null || s.stop === null) return { ...result, status: 'BLOCKED', blockers: ['WINDOW_INCOMPLETE'] };
    const today = dayAt(now, s.timezone), overnight = s.stop < s.start;
    let first = addDay(today, overnight ? -1 : 0);
    if (s.dateFrom && first < s.dateFrom) first = s.dateFrom;
    for (let i = 0; i < 10; i++) {
      const day = addDay(first, i);
      if (s.dateTo && day > s.dateTo) break;
      if (!s.weekdays.includes(weekday(day))) continue;
      const stopDay = overnight ? addDay(day, 1) : day;
      if (stopDay < today) continue;
      const start = resolveLocal(day, s.start, s.timezone), stop = resolveLocal(stopDay, s.stop, s.timezone);
      if (!start.ok || !stop.ok) return { ...result, status: 'BLOCKED', blockers: [...new Set([start.reason, stop.reason].filter(Boolean))] };
      if (stop.atMs <= start.atMs) return { ...result, status: 'BLOCKED', blockers: ['NONPOSITIVE_WINDOW'] };
      const window = { startDay: day, startWeekday: weekday(day), timezone: s.timezone, overnight, start: start.at, stop: stop.at,
        startMs: start.atMs, stopMs: stop.atMs };
      if (start.atMs <= now && now < stop.atMs) result.currentWindow = window;
      if (start.atMs > now) { result.nextWindow = window; break; }
    }
    if (!result.nextWindow) result.blockers.push('NO_FUTURE_WINDOW_IN_DATE_BOUNDS');
    return { ...result, status: result.blockers.length ? 'BLOCKED' : 'PREVIEW_ONLY' };
  } catch (error) { return { planOnly: true, registered: false, effectiveScheduleEnabled: false, status: 'BLOCKED', currentWindow: null, nextWindow: null, blockers: [error.code || 'SCHEDULE_PREVIEW_FAILED'] }; }
}
function makeSchedulerPlan(snapshot, now = Date.now()) {
  const base = { planOnly: true, activationRequired: true, activationAllowed: false, effectiveScheduleEnabled: false,
    device: 'xd://session_schedule_write', binding: 'CURRENT_EXECUTION_CONTEXT_ONLY_NO_SESSION_SELECTOR', requests: [], blockers: [] };
  try {
    clock(now);
    const valid = validateSettings(snapshot?.settings);
    if (!valid.ok || !Number.isSafeInteger(snapshot?.revision) || snapshot.revision < 1 || snapshot.configHash !== configHash(valid.settings) || snapshot.storage?.error) return { ...base, blockers: ['SAVED_VALID_REVISION_REQUIRED'] };
    const preview = previewSchedule(valid.settings.schedule, now);
    if (!preview.nextWindow || preview.blockers.length) return { ...base, preview, blockers: preview.blockers.length ? preview.blockers : ['SCHEDULE_DISABLED'] };
    const w = preview.nextWindow, rev = snapshot.revision, hash = snapshot.configHash;
    const guard = `LIVE draft boundary, config revision=${rev}, sha256=${hash}, BTC only. This wake is NOT monetary permission. Read saved exact revision/hash and independently stored explicit user approval for this exact configuration and own managed scope. Recheck actual fresh wallet and market/execution data, positions, pending operations and unknown ledger; validate BTC-only contract deadline/independent-close capability, allocation, funds plus reserve, instrument limits and risk budget, locked wall/G3/freshness/high-water/margin guards. On mismatch, missing approval, unknown or stale evidence: NO ENTRY, no order/signature, record blocked. Never infer authority from a draft, scheduler request or this prompt. Never create agents, queues or another schedule.`;
    base.requests = [
      { action: 'create', at: w.start, expires: w.stop, coalesceKey: `live-draft-start-${rev}-${hash}-${w.startDay}`,
        idempotencyKey: `live-draft-start-${rev}-${hash}-${w.start}`, prompt: `${guard} START boundary: require zero wallet positions, zero pending operations, zero openManaged and an empty authoritative unknown ledger; missing arrays are UNKNOWN. Also require current time inside [${w.start}, ${w.stop}) and a still-authorized start; never backfill a missed start.` },
      { action: 'create', at: w.stop, expires: new Date(w.stopMs + 300000).toISOString(), coalesceKey: `live-draft-stop-${rev}-${hash}-${w.startDay}`,
        idempotencyKey: `live-draft-stop-${rev}-${hash}-${w.stop}`, prompt: `${guard} STOP boundary: disable entries only within the previously approved bot scope. Any close/cancel must be separately covered by that prior own-position/intent authorization. No unrelated wallet exposure, no global close, no new entry, no invented permission. Settings mismatch never grants broader authority; independent manual emergency Stop must remain available without reading settings.` },
    ];
    return { ...base, preview, preflightBlockers: [...(snapshot.readiness?.blockers || [])],
      note: 'DECLARATIVE ONLY. Parent applies through its mounted current-session device ONLY after separate user approval. Current-window presence never grants a retroactive start.' };
  } catch (error) { return { ...base, blockers: [error.code || 'SCHEDULE_PLAN_FAILED'] }; }
}
function defaultRead(file) {
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try { const stat = fs.fstatSync(fd); if (!stat.isFile() || stat.size > MAX_FILE_BYTES) fail('SETTINGS_FILE_TOO_LARGE_OR_UNSAFE'); return fs.readFileSync(fd, 'utf8'); }
  finally { fs.closeSync(fd); }
}
function atomicSave(file, value) {
  const target = path.resolve(file), dir = path.dirname(target);
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const stat = fs.lstatSync(dir); if (!stat.isDirectory() || stat.isSymbolicLink()) fail('UNSAFE_SETTINGS_DIRECTORY');
  const tmp = path.join(dir, '.' + path.basename(target) + '.' + crypto.randomUUID() + '.tmp');
  let fd;
  try {
    fd = fs.openSync(tmp, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600);
    fs.writeFileSync(fd, JSON.stringify(value) + '\n'); fs.fsyncSync(fd); fs.closeSync(fd); fd = null;
    fs.renameSync(tmp, target);
    const directory = fs.openSync(dir, fs.constants.O_RDONLY); try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
  } finally { if (fd !== undefined && fd !== null) fs.closeSync(fd); if (fs.existsSync(tmp)) fs.unlinkSync(tmp); }
}
function validateRecord(record) {
  if (!keys(record, ['schemaVersion', 'revision', 'updatedAt', 'configHash', 'settings', 'history']) || record.schemaVersion !== 1 ||
      !Number.isSafeInteger(record.revision) || record.revision < 1 || !Array.isArray(record.history) || record.history.length !== record.revision) fail('CORRUPT_SETTINGS_RECORD');
  let previousAt = -1;
  for (const [i, row] of record.history.entries()) {
    if (!keys(row, ['revision', 'updatedAt', 'configHash', 'settings']) || row.revision !== i + 1) fail('CORRUPT_SETTINGS_HISTORY');
    const v = validateSettings(row.settings), at = Date.parse(row.updatedAt);
    if (!v.ok || canonical(v.settings) !== canonical(row.settings) || !Number.isSafeInteger(at) || new Date(at).toISOString() !== row.updatedAt || at < previousAt || row.configHash !== configHash(v.settings)) fail('CORRUPT_SETTINGS_HISTORY');
    previousAt = at;
  }
  const last = record.history.at(-1);
  if (last.updatedAt !== record.updatedAt || last.configHash !== record.configHash || canonical(last.settings) !== canonical(record.settings)) fail('CORRUPT_SETTINGS_RECORD');
  return record;
}
class LiveSettingsStore {
  constructor({ file, read = defaultRead, save = atomicSave, defaults = {} } = {}) {
    if (typeof file !== 'string' || !file || typeof read !== 'function' || typeof save !== 'function') fail('INVALID_STORE_ADAPTER');
    this.file = file; this.read = read; this.save = save; this.defaults = makeDefaults(defaults);
    if (!validateSettings(this.defaults).ok) fail('INVALID_DEFAULTS');
    this.record = null; this.error = null; this.busy = false; this._refresh();
  }
  _refresh() {
    if (this.error) return;
    try {
      const raw = this.read(this.file);
      if (raw && typeof raw.then === 'function') fail('ASYNC_READ_UNSUPPORTED');
      if (!(typeof raw === 'string' || Buffer.isBuffer(raw)) || Buffer.byteLength(raw) > MAX_FILE_BYTES) fail('INVALID_OR_OVERSIZED_SETTINGS_FILE');
      const next = validateRecord(JSON.parse(String(raw)));
      if (this.record) {
        if (next.revision < this.record.revision || (next.revision === this.record.revision && canonical(next) !== canonical(this.record))) fail('PERSISTED_REVISION_CONFLICT');
        if (next.revision > this.record.revision && canonical(next.history.slice(0, this.record.history.length)) !== canonical(this.record.history)) fail('PERSISTED_HISTORY_CONFLICT');
      }
      this.record = copy(next);
    } catch (error) {
      if (error.code === 'ENOENT' && !this.record) return;
      this.error = error.code === 'ENOENT' ? 'SAVED_SETTINGS_DISAPPEARED' : 'CORRUPT_OR_UNREADABLE_SETTINGS';
    }
  }
  snapshot(context = {}, now = Date.now()) {
    let validClock = true; try { clock(now); } catch { validClock = false; }
    this._refresh();
    const settings = this.record ? copy(this.record.settings) : this.error ? null : copy(this.defaults);
    const revision = this.record?.revision || 0;
    const state = validClock ? readiness(settings, context, now, revision, this.error) : { preflightReady: false, readyToStart: false, draftComplete: false, blockers: ['INVALID_CLOCK'], wallet: null, derived: null };
    if (this.record && validClock && Date.parse(this.record.updatedAt) > now) { state.preflightReady = false; state.blockers.push('SAVED_CONFIG_CLOCK_FUTURE'); }
    return { schemaVersion: 1, revision, updatedAt: this.record?.updatedAt || null, configHash: settings ? configHash(settings) : null,
      settings, storage: { state: this.error ? 'ERROR_REQUIRES_REPAIR_AND_RELOAD' : revision ? 'SAVED_DRAFT' : 'NEW_UNSAVED_DRAFT', error: this.error },
      history: this.record ? this.record.history.map(({ revision, updatedAt, configHash }) => ({ revision, updatedAt, configHash })) : [],
      capitalMeaning: 'BUDGET_IS_BOT_ALLOCATION_LIMIT_NOT_EDITABLE_WALLET_BALANCE', readiness: state,
      features: { supported: copy(FEATURE_REGISTRY), locked: { ...LOCKED_GUARDS }, lockedMeaning: 'MANDATORY_POLICY_NOT_RUNTIME_MEASUREMENT' },
      activation: { allowed: false, requiresSeparateUserPermission: true, effectiveScheduleEnabled: false, saveIsPermission: false },
      requiresSeparateUserPermission: true, effectiveActivation: false, effectiveScheduleEnabled: false, readyToStart: false, stagedOnly: true,
      emergencyStop: { independent: true, settingsRequired: false }, runtimeApplied: false };
  }
  update(body, context = {}, now = Date.now()) {
    const respond = (ok, status, reasons) => ({ ok, status, reasons, snapshot: this.snapshot(context, now), stagedOnly: true, runtimeApplied: false, approvalInvalidated: ok });
    if (this.busy) return respond(false, 409, ['STORE_TRANSACTION_IN_PROGRESS']);
    this.busy = true;
    try {
      clock(now); this._refresh();
      if (this.error) return respond(false, 503, [this.error]);
      if (!keys(body, ['expectedRevision', 'settings']) || !Number.isSafeInteger(body.expectedRevision) || body.expectedRevision < 0) return respond(false, 400, ['INVALID_UPDATE_KEYS_OR_REVISION']);
      const valid = validateSettings(body.settings);
      if (!valid.ok) return respond(false, 400, valid.reasons);
      if (Buffer.byteLength(JSON.stringify(body)) > MAX_BODY_BYTES) return respond(false, 413, ['SETTINGS_BODY_TOO_LARGE']);
      if (body.expectedRevision !== (this.record?.revision || 0)) return respond(false, 409, ['REVISION_CONFLICT']);
      const blocked = stopFirstBlockers(context, now);
      if (blocked.length) return respond(false, 409, blocked);
      if (this.record && now < Date.parse(this.record.updatedAt)) return respond(false, 409, ['UPDATE_CLOCK_REGRESSION']);
      const limits = context?.instrumentLimits;
      if (confirmed(limits, now) && limits.symbol === 'BTC' && Number.isInteger(limits.maxLeverage) && limits.maxLeverage >= 1 && limits.maxLeverage <= 1000 && valid.settings.capital.leverage !== null && valid.settings.capital.leverage > limits.maxLeverage) return respond(false, 400, ['LEVERAGE_EXCEEDS_CONFIRMED_LIMIT']);
      const revision = (this.record?.revision || 0) + 1, updatedAt = new Date(now).toISOString(), hash = configHash(valid.settings);
      const history = [...(this.record?.history || []), { revision, updatedAt, configHash: hash, settings: valid.settings }];
      const next = { schemaVersion: 1, revision, updatedAt, configHash: hash, settings: valid.settings, history };
      if (!Number.isSafeInteger(revision) || Buffer.byteLength(JSON.stringify(next)) > MAX_FILE_BYTES) return respond(false, 413, ['HISTORY_CAPACITY_REQUIRES_EXPLICIT_ARCHIVAL']);
      try {
        const saved = this.save(this.file, copy(next));
        if (saved && typeof saved.then === 'function') fail('ASYNC_SAVE_UNSUPPORTED');
        const raw = this.read(this.file);
        if (!(typeof raw === 'string' || Buffer.isBuffer(raw)) || Buffer.byteLength(raw) > MAX_FILE_BYTES || canonical(validateRecord(JSON.parse(String(raw)))) !== canonical(next)) fail('PERSISTENCE_READBACK_MISMATCH');
      } catch { this.error = 'PERSISTENCE_UNCERTAIN_RELOAD_REQUIRED'; return respond(false, 503, [this.error]); }
      this.record = copy(next);
      return respond(true, 200, ['SAVED_DRAFT_ONLY_SEPARATE_PERMISSION_REQUIRED']);
    } catch (error) { return respond(false, 400, [error.code || 'INVALID_UPDATE']); }
    finally { this.busy = false; }
  }
}
module.exports = { LiveSettingsStore, validateSettings, validateSchedule, makeDefaults, toStrategyPatch, previewSchedule,
  makeSchedulerPlan, readiness, stopFirstBlockers, configHash, atomicSave, FEATURE_REGISTRY, LOCKED_GUARDS, MAX_BODY_BYTES, MAX_FILE_BYTES };
