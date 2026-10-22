'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { LiveSettingsStore, makeDefaults, validateSettings, validateSchedule, previewSchedule, makeSchedulerPlan,
  toStrategyPatch, configHash, FEATURE_REGISTRY, LOCKED_GUARDS } = require('../../apps/vchera2/live-settings.cjs');
const NOW = Date.parse('2026-10-20T08:00:00Z');
const clone = v => JSON.parse(JSON.stringify(v));
function context() {
  return { LIVE: false, entriesEnabled: false, providerFresh: true, openManaged: [], safetyReady: true,
    provider: { entrySafety: { unknown: [] }, wallet: { connected: true, err: null, t: NOW, balance: '5000', available: '4000', openPositions: [], operations: [] } },
    instrumentLimits: { confirmed: true, updatedAtMs: NOW, symbol: 'BTC', maxLeverage: 1000, minMarginUsdc: '10', minNotionalUsd: '1000', maxNotionalUsd: '1260000' },
    riskBudget: { confirmed: true, updatedAtMs: NOW, reserveUsdc: '10', maxBudgetUsdc: '3000', maxNotionalUsd: '1260000' },
    contractCapability: { btcEntryWithDeadline: true, independentClose: true }, pnlDay: { realisedUsd: -334.97, highWater: 100 } };
}
function selected() { const d = makeDefaults(); d.capital = { budgetUsdc: '1000', marginUsdc: '100', leverage: 10 }; return d; }
function harness() {
  let text = null, failure = null, calls = 0;
  const read = () => { if (text === null) throw Object.assign(new Error('missing'), { code: 'ENOENT' }); return text; };
  const save = (file, value) => { calls++; if (failure === 'before') throw new Error('disk failed'); text = JSON.stringify(value); if (failure === 'after') throw new Error('sync failed after rename'); };
  const create = () => new LiveSettingsStore({ file: '/virtual/live-settings.json', read, save });
  const store = create();
  return { store, create, read, save, get text() { return text; }, set text(v) { text = v; }, get calls() { return calls; }, set failure(v) { failure = v; } };
}
const update = (h, settings = selected(), expectedRevision = 0, ctx = context(), now = NOW) => h.store.update({ expectedRevision, settings }, ctx, now);
function assertOff(s) {
  assert.equal(s.activation.allowed, false); assert.equal(s.activation.requiresSeparateUserPermission, true);
  assert.equal(s.activation.effectiveScheduleEnabled, false); assert.equal(s.effectiveScheduleEnabled, false);
  assert.equal(s.readyToStart, false); assert.equal(s.runtimeApplied, false); assert.equal(s.emergencyStop.settingsRequired, false);
  assert.equal(s.effectiveActivation, false); assert.equal(s.stagedOnly, true);
}

test('factory capital is unselected, never old launch exposure or permission', () => {
  const launch = JSON.parse(fs.readFileSync(path.resolve(__dirname, '../../apps/vchera2/launch-config.json')));
  const d = makeDefaults(launch);
  assert.deepEqual(d.capital, { budgetUsdc: null, marginUsdc: null, leverage: null });
  assert.equal(d.features.noLongs, launch.strategy.noLongs); assert.equal(d.features.spreadEnabled, true);
  const h = harness(), snap = h.store.snapshot(context(), NOW);
  assert.equal(snap.revision, 0); assert.equal(snap.readiness.preflightReady, false); assertOff(snap);
});
test('selected complete draft persists canonical decimal capital without editing real balance or PnL', () => {
  const h = harness(), ctx = context(), before = JSON.stringify(ctx), d = selected();
  d.capital.budgetUsdc = '1000.000000'; d.capital.marginUsdc = 100;
  const r = update(h, d, 0, ctx);
  assert.equal(r.ok, true); assert.equal(r.status, 200); assert.equal(r.snapshot.revision, 1);
  assert.equal(r.snapshot.settings.capital.budgetUsdc, '1000'); assert.equal(r.snapshot.settings.capital.marginUsdc, '100');
  assert.equal(r.snapshot.readiness.derived.notionalUsd, '1000'); assert.equal(r.snapshot.readiness.derived.requiredAvailableUsdc, '1010');
  assert.equal(r.snapshot.readiness.wallet.availableUsdc, '4000'); assert.equal(r.snapshot.readiness.wallet.balanceUsdc, '5000');
  assert.equal(r.snapshot.readiness.derived.budgetIsWalletBalance, false); assert.equal(r.snapshot.readiness.preflightReady, true);
  assert.equal(JSON.stringify(ctx), before); assertOff(r.snapshot); assert.equal(h.calls, 1); assert.equal(r.approvalInvalidated, true);
});
test('incomplete capital draft can be saved but is never ready', () => {
  const h = harness(), r = update(h, makeDefaults());
  assert.equal(r.ok, true); assert.equal(r.snapshot.revision, 1); assert.equal(r.snapshot.readiness.draftComplete, false);
  assert.ok(r.snapshot.readiness.blockers.includes('CAPITAL_NOT_SELECTED')); assertOff(r.snapshot);
});
test('CAS protects two instances and stale clients with full version history across restart', () => {
  const h = harness(), second = h.create();
  assert.equal(update(h).ok, true);
  const stale = second.update({ expectedRevision: 0, settings: selected() }, context(), NOW + 1);
  assert.equal(stale.status, 409); assert.ok(stale.reasons.includes('REVISION_CONFLICT')); assert.equal(h.calls, 1);
  const changed = selected(); changed.capital.budgetUsdc = '1200';
  assert.equal(second.update({ expectedRevision: 1, settings: changed }, context(), NOW + 2).ok, true);
  const restarted = h.create(), s = restarted.snapshot(context(), NOW + 2);
  assert.equal(s.revision, 2); assert.equal(s.history.length, 2); assert.equal(s.settings.capital.budgetUsdc, '1200');
  assert.equal(JSON.parse(h.text).history[0].settings.capital.budgetUsdc, '1000'); assertOff(s);
  assert.equal(update(h, selected(), 1, context(), NOW + 3).status, 409);
});
test('identical settings still advance explicit revision, invalidating old exact-revision plan', () => {
  const h = harness(); assert.equal(update(h).ok, true); const first = h.store.snapshot(context(), NOW);
  assert.equal(update(h, selected(), 1, context(), NOW).ok, true);
  const next = h.store.snapshot(context(), NOW); assert.equal(next.revision, 2); assert.equal(next.configHash, first.configHash); assertOff(next);
});
test('failed save before write preserves last acknowledged revision and latches fail closed until reload', () => {
  const h = harness(); update(h); const before = h.text; h.failure = 'before';
  const r = update(h, selected(), 1, context(), NOW + 1);
  assert.equal(r.status, 503); assert.equal(r.snapshot.revision, 1); assert.equal(h.text, before);
  assert.equal(r.snapshot.readiness.preflightReady, false); assertOff(r.snapshot);
  assert.equal(update(h, selected(), 1, context(), NOW + 2).status, 503);
  h.failure = null; assert.equal(h.create().snapshot(context(), NOW + 3).revision, 1);
});
test('failure after rename is explicitly uncertain; restart reconciles durable revision without automatic activation', () => {
  const h = harness(); update(h); h.failure = 'after';
  const r = update(h, selected(), 1, context(), NOW + 1);
  assert.equal(r.status, 503); assert.equal(r.snapshot.revision, 1); assert.equal(JSON.parse(h.text).revision, 2);
  h.failure = null; const s = h.create().snapshot(context(), NOW + 2);
  assert.equal(s.revision, 2); assert.equal(s.storage.error, null); assertOff(s);
});
test('async and non-persisting save adapters are rejected without claiming a committed revision', () => {
  for (const mode of ['async', 'noop']) {
    const h = harness(), store = new LiveSettingsStore({ file: '/virtual/settings', read: h.read, save: () => mode === 'async' ? Promise.resolve() : undefined });
    const r = store.update({ expectedRevision: 0, settings: selected() }, context(), NOW);
    assert.equal(r.status, 503); assert.equal(r.snapshot.revision, 0); assert.equal(r.snapshot.settings, null); assertOff(r.snapshot);
  }
});
test('corrupt file never silently resets to defaults and cannot gate or mutate independent emergency Stop', () => {
  for (const corrupt of ['{broken', '{}', 'null', JSON.stringify({ revision: 1, settings: selected(), activation: { allowed: true } })]) {
    const h = harness(); h.text = corrupt; const store = h.create(), snap = store.snapshot(context(), NOW);
    assert.equal(snap.settings, null); assert.equal(snap.revision, 0); assert.ok(snap.storage.error);
    assert.equal(store.update({ expectedRevision: 0, settings: selected() }, context(), NOW).status, 503);
    assert.equal(h.calls, 0); assertOff(snap);
  }
});
test('checksum/history tampering, disappearance and revision rollback remain blocked instead of resetting', () => {
  for (const corruption of ['hash', 'history', 'delete', 'rollback']) {
    const h = harness(); update(h); const first = h.text; update(h, selected(), 1, context(), NOW + 1);
    if (corruption === 'delete') h.text = null;
    else if (corruption === 'rollback') h.text = first;
    else { const row = JSON.parse(h.text); if (corruption === 'hash') row.configHash = '0'.repeat(64); else row.history[0].settings.capital.marginUsdc = '101'; h.text = JSON.stringify(row); }
    const snap = h.store.snapshot(context(), NOW + 2);
    assert.equal(snap.revision, 2); assert.ok(snap.storage.error); assert.equal(snap.readiness.preflightReady, false);
    assert.equal(update(h, selected(), 2, context(), NOW + 3).status, 503); assertOff(snap);
  }
});
test('invalid decimal representations, nonfinite/negative/oversize values never mutate the stored revision', () => {
  for (const value of [0, -1, NaN, Infinity, true, false, '', ' ', ' 2', '2 ', '1e3', '0x10', '00.5', '1.0000001', '1000000001', {}, [], 1n]) {
    const h = harness(), d = selected(); d.capital.marginUsdc = value;
    const r = update(h, d); assert.equal(r.ok, false, String(value)); assert.equal(r.status, 400);
    assert.equal(h.calls, 0); assert.equal(r.snapshot.revision, 0);
  }
});
test('six-place decimals are exact, leverage is integer 1..1000 and known lower max is enforced', () => {
  const d = selected(); d.capital.marginUsdc = '10.123456'; d.capital.budgetUsdc = '1000.000001';
  assert.equal(validateSettings(d).ok, true);
  for (const leverage of [0, -1, 1001, 2.5, '10', NaN, true]) { const q = selected(); q.capital.leverage = leverage; assert.equal(validateSettings(q).ok, false); }
  const h = harness(), ctx = context(); ctx.instrumentLimits.maxLeverage = 5;
  assert.equal(update(h, selected(), 0, ctx).status, 400); assert.equal(h.calls, 0);
});
test('margin above allocation and ETH/BOTH instruments are rejected, not coerced into BTC', () => {
  const h = harness(), d = selected(); d.capital.marginUsdc = '1001'; assert.equal(update(h, d).ok, false);
  for (const symbol of ['ETH', 'BOTH', 0, null]) { const q = selected(); q.symbol = symbol; assert.equal(update(h, q).ok, false); }
  assert.equal(h.calls, 0);
});
test('unknown keys, locked flags, wallet balance, auth, Telegram and autoTune fields are rejected', () => {
  for (const key of ['wallGuard', 'g3Enabled', 'freshness', 'signedDeadline', 'unknownLedger', 'highWater', 'independentClose', 'live', 'enabled', 'useMainKey', 'tgEnabled', 'autoTune', 'wallet', 'keys']) {
    const h = harness(), d = selected(); d.features[key] = false;
    assert.equal(update(h, d).ok, false, key); assert.equal(h.calls, 0);
  }
  const h = harness(), d = selected(); d.capital.balanceUsdc = '999999'; assert.equal(update(h, d).ok, false);
  assert.equal(h.store.update({ expectedRevision: 0, settings: selected(), context: context() }, context(), NOW).status, 400);
  const polluted = JSON.parse(JSON.stringify(selected()).replace('"symbol":"BTC"', '"symbol":"BTC","__proto__":{"live":true}'));
  assert.equal(update(h, polluted).ok, false); assert.equal({}.live, undefined);
});
test('oversized requests reject without persistence and accessor/prototype payloads are not trusted', () => {
  const h = harness(), d = selected(); d.schedule.timezone = 'x'.repeat(20000); assert.equal(update(h, d).ok, false);
  const accessor = selected(); Object.defineProperty(accessor.capital, 'marginUsdc', { get() { throw new Error('must not run'); }, enumerable: true });
  assert.equal(update(h, accessor).ok, false);
  const inherited = Object.create({ symbol: 'BTC' }); assert.equal(validateSettings(inherited).ok, false); assert.equal(h.calls, 0);
});
test('at least one strategic branch and one enabled book venue are mandatory', () => {
  const h = harness(), d = selected(); d.features.spreadEnabled = false; d.features.bookEnabled = false;
  assert.equal(update(h, d).ok, false);
  const q = selected(); q.features.bookUseHl = q.features.bookUseBf = q.features.bookUseBs = false;
  assert.equal(update(h, q).ok, false); q.features.bookEnabled = false; assert.equal(update(h, q).ok, true);
});
test('pure cfg mapping exports only margin/leverage and supported optional flags, never budget/live/auth', () => {
  const d = selected(), original = clone(d), patch = toStrategyPatch(d);
  assert.deepEqual(Object.keys(patch).sort(), ['margin', 'leverage', ...Object.keys(FEATURE_REGISTRY)].sort());
  assert.equal(patch.margin, 100); assert.equal(patch.leverage, 10);
  for (const key of ['enabled', 'live', 'auth', 'budgetUsdc', 'wallGuard', 'g3Enabled', 'tgEnabled']) assert.equal(Object.hasOwn(patch, key), false);
  assert.deepEqual(d, original); assert.throws(() => toStrategyPatch(makeDefaults()), /INCOMPLETE_CAPITAL/);
  assert.equal(LOCKED_GUARDS.wallGuard, true); assert.equal(LOCKED_GUARDS.highWater, true);
});
test('strict stop-first forbids LIVE, entries, every exposure, unknown ledger and unknown/stale wallet', () => {
  const mutate = [c => { c.LIVE = true; }, c => { c.entriesEnabled = true; }, c => { delete c.LIVE; },
    c => { c.provider.wallet.openPositions.push({ positionId: '1' }); }, c => { c.provider.wallet.operations.push({ intentId: 'x' }); },
    c => { c.openManaged.push({ unknown: true }); }, c => { c.provider.entrySafety.unknown.push('unknown'); },
    c => { delete c.provider.entrySafety; }, c => { delete c.provider.wallet.openPositions; }, c => { delete c.openManaged; },
    c => { c.providerFresh = false; }, c => { c.provider.wallet.connected = false; }, c => { c.provider.wallet.t = NOW - 3001; },
    c => { c.provider.wallet.t = NOW + 1; }];
  for (const edit of mutate) {
    const h = harness(), ctx = context(); edit(ctx); const before = JSON.stringify(ctx), r = update(h, selected(), 0, ctx);
    assert.equal(r.status, 409); assert.equal(h.calls, 0); assert.equal(JSON.stringify(ctx), before); assertOff(r.snapshot);
  }
});
test('body cannot serve as context authority and even forged parent approval cannot activate this module', () => {
  const h = harness(); assert.equal(update(h, selected(), 0, {}).status, 409);
  const ctx = context(); ctx.authorized = true; ctx.approval = { allowed: true }; ctx.activation = { allowed: true };
  const r = update(h, selected(), 0, ctx); assert.equal(r.ok, true); assertOff(r.snapshot);
});
test('funds, reserve, actual minimums and risk/contract capability remain readiness blockers not editable balances', () => {
  const checks = [
    [c => { c.provider.wallet.available = '500'; }, 'AVAILABLE_BELOW_BUDGET_PLUS_RESERVE'],
    [c => { c.provider.wallet.available = '6000'; }, 'ACTUAL_FUNDS_UNKNOWN_OR_INCONSISTENT'],
    [c => { c.provider.wallet.available = NaN; }, 'ACTUAL_FUNDS_UNKNOWN_OR_INCONSISTENT'],
    [c => { delete c.riskBudget.reserveUsdc; }, 'RESERVE_UNKNOWN'],
    [c => { c.riskBudget.maxBudgetUsdc = '900'; }, 'RISK_MAXBUDGETUSDC_VIOLATION'],
    [c => { c.riskBudget.maxNotionalUsd = '900'; }, 'RISK_MAXNOTIONALUSD_VIOLATION'],
    [c => { c.instrumentLimits.minMarginUsdc = '101'; }, 'INSTRUMENT_MINMARGINUSDC_VIOLATION'],
    [c => { c.instrumentLimits.minNotionalUsd = '1001'; }, 'INSTRUMENT_MINNOTIONALUSD_VIOLATION'],
    [c => { c.instrumentLimits.maxNotionalUsd = '999'; }, 'INSTRUMENT_MAXNOTIONALUSD_VIOLATION'],
    [c => { c.instrumentLimits.updatedAtMs = NOW - 30001; }, 'BTC_INSTRUMENT_LIMITS_UNKNOWN_OR_STALE'],
    [c => { c.riskBudget.updatedAtMs = NOW + 1; }, 'RISK_BUDGET_UNKNOWN_OR_STALE'],
    [c => { delete c.contractCapability; }, 'CONTRACT_CAPABILITY_UNCONFIRMED'],
    [c => { c.safetyReady = false; }, 'LOCKED_RUNTIME_SAFETY_NOT_CONFIRMED'],
  ];
  for (const [edit, reason] of checks) {
    const h = harness(), ctx = context(); edit(ctx); const r = update(h, selected(), 0, ctx);
    assert.equal(r.ok, true, reason); assert.equal(r.snapshot.readiness.preflightReady, false);
    assert.ok(r.snapshot.readiness.blockers.includes(reason), JSON.stringify(r.snapshot.readiness.blockers)); assertOff(r.snapshot);
  }
});
test('post-save stale or exposed context cannot appear ready; snapshot values cannot mutate saved state', () => {
  const h = harness(); update(h); const ctx = context(); ctx.provider.wallet.operations.push('pending');
  const s = h.store.snapshot(ctx, NOW + 1); assert.equal(s.readiness.preflightReady, false);
  s.settings.capital.marginUsdc = '999'; s.history[0].configHash = 'bad';
  assert.equal(h.store.snapshot(context(), NOW + 2).settings.capital.marginUsdc, '100');
  assert.equal(h.store.snapshot(context(), NOW + 4000).readiness.preflightReady, false);
});
test('clock regression and malformed update revisions never create a new saved version', () => {
  const h = harness(); update(h);
  assert.equal(update(h, selected(), 1, context(), NOW - 1).ok, false);
  for (const rev of [-1, 1.5, '1', NaN, true]) assert.equal(update(h, selected(), rev).status, 400);
  assert.equal(h.calls, 1); assert.equal(h.store.snapshot(context(), NOW).revision, 1);
});
test('canonical config hash is stable under key reordering and changes with approved draft choices', () => {
  const a = selected(), b = { features: clone(a.features), schedule: clone(a.schedule), capital: clone(a.capital), symbol: a.symbol };
  assert.equal(configHash(a), configHash(b)); b.capital.marginUsdc = '101'; assert.notEqual(configHash(a), configHash(b));
});

function windowDraft(extra = {}) { return { ...makeDefaults().schedule, enabled: true, timezone: 'UTC', ...extra }; }
test('daily timezone preview is pure, uses inclusive start/exclusive stop and never registers a task', () => {
  const s = windowDraft(), before = clone(s), p = previewSchedule(s, Date.parse('2026-10-20T08:00:00Z'));
  assert.equal(p.status, 'PREVIEW_ONLY'); assert.equal(p.nextWindow.start, '2026-10-20T09:00:00.000Z');
  assert.equal(p.nextWindow.stop, '2026-10-20T18:00:00.000Z'); assert.equal(p.registered, false); assert.equal(p.effectiveScheduleEnabled, false);
  assert.ok(previewSchedule(s, Date.parse('2026-10-20T09:00:00Z')).currentWindow);
  assert.equal(previewSchedule(s, Date.parse('2026-10-20T18:00:00Z')).currentWindow, null); assert.deepEqual(s, before);
});
test('overnight weekdays refer to the start day and date bounds apply to that local start day', () => {
  const s = windowDraft({ start: '22:00', stop: '02:00', weekdays: [1], dateFrom: '2026-10-19', dateTo: '2026-10-26' });
  const p = previewSchedule(s, Date.parse('2026-10-20T00:30:00Z'));
  assert.equal(p.currentWindow.startDay, '2026-10-19'); assert.equal(p.currentWindow.startWeekday, 1);
  assert.equal(p.currentWindow.stop, '2026-10-20T02:00:00.000Z');
  assert.equal(p.nextWindow.start, '2026-10-26T22:00:00.000Z'); assert.equal(p.nextWindow.stop, '2026-10-27T02:00:00.000Z');
});
test('future/expired date bounds and disabled/incomplete windows never silently roll into unrestricted scheduling', () => {
  const future = previewSchedule(windowDraft({ dateFrom: '2026-12-01', dateTo: '2026-12-01' }), NOW);
  assert.equal(future.nextWindow.startDay, '2026-12-01');
  const expired = previewSchedule(windowDraft({ dateTo: '2026-10-19' }), NOW);
  assert.ok(expired.blockers.includes('NO_FUTURE_WINDOW_IN_DATE_BOUNDS')); assert.equal(expired.nextWindow, null);
  assert.equal(previewSchedule(windowDraft({ enabled: false }), NOW).status, 'DISABLED_DRAFT');
  assert.ok(previewSchedule(windowDraft({ start: null }), NOW).blockers.includes('WINDOW_INCOMPLETE'));
});
test('invalid/equal times, IANA timezone, date bounds and weekdays reject explicitly', () => {
  for (const extra of [{ start: '24:00' }, { start: '9:00' }, { stop: '09:00' }, { timezone: 'Not/AZone' }, { kind: 'cron' },
    { weekdays: [] }, { weekdays: [1, 1] }, { weekdays: [0] }, { weekdays: ['1'] }, { dateFrom: '2026-02-30' },
    { dateFrom: '2026-11-01', dateTo: '2026-10-01' }, { dateTo: '2200-01-01' }]) {
    assert.throws(() => validateSchedule(windowDraft(extra)), JSON.stringify(extra));
  }
});
test('IANA timezone and fractional offsets map real civil boundaries without a host-local timezone assumption', () => {
  const moscow = previewSchedule(windowDraft({ timezone: 'Europe/Moscow' }), Date.parse('2026-10-20T05:00:00Z'));
  assert.equal(moscow.nextWindow.start, '2026-10-20T06:00:00.000Z');
  const nepal = previewSchedule(windowDraft({ timezone: 'Asia/Kathmandu' }), Date.parse('2026-10-20T00:00:00Z'));
  assert.equal(nepal.nextWindow.start, '2026-10-20T03:15:00.000Z');
  const ny = previewSchedule(windowDraft({ timezone: 'America/New_York' }), Date.parse('2026-10-20T08:00:00Z'));
  assert.equal(ny.nextWindow.start, '2026-10-20T13:00:00.000Z');
});
test('nonexistent spring and ambiguous autumn DST times are explicitly unsupported, not silently shifted or folded', () => {
  for (const [day, now] of [['2026-03-08', '2026-03-08T05:00:00Z'], ['2026-11-01', '2026-11-01T04:00:00Z']]) {
    const p = previewSchedule(windowDraft({ timezone: 'America/New_York', start: day.includes('03-') ? '02:30' : '01:30', stop: '04:00', dateFrom: day, dateTo: day }), Date.parse(now));
    assert.equal(p.status, 'BLOCKED'); assert.equal(p.nextWindow, null); assert.ok(p.blockers.includes('DST_TRANSITION_WINDOW_UNSUPPORTED'));
  }
});
test('half-hour DST transition zones also fail closed instead of assuming a one-hour correction', () => {
  const p = previewSchedule(windowDraft({ timezone: 'Australia/Lord_Howe', start: '02:15', stop: '04:00', dateFrom: '2026-10-04', dateTo: '2026-10-04' }), Date.parse('2026-10-03T12:00:00Z'));
  assert.equal(p.status, 'BLOCKED'); assert.ok(p.blockers.includes('DST_TRANSITION_WINDOW_UNSUPPORTED'));
});
test('saved scheduler plan returns only current-session device data, binds exact revision/hash, and cannot authorize LIVE', () => {
  const h = harness(), d = selected(); d.schedule.enabled = true;
  const r = update(h, d); assert.equal(r.ok, true); const before = h.text, s = r.snapshot, plan = makeSchedulerPlan(s, NOW);
  assert.equal(plan.planOnly, true); assert.equal(plan.activationRequired, true); assert.equal(plan.activationAllowed, false);
  assert.equal(plan.device, 'xd://session_schedule_write'); assert.equal(plan.requests.length, 2); assert.equal(h.text, before);
  assert.equal(plan.preview.currentWindow.startDay, '2026-10-20'); assert.equal(plan.requests[0].at, '2026-10-21T06:00:00.000Z');
  for (const request of plan.requests) {
    assert.equal(request.action, 'create'); assert.ok(request.prompt.includes(s.configHash)); assert.ok(request.prompt.includes('revision=1'));
    assert.ok(request.prompt.includes('NOT monetary permission')); assert.ok(request.prompt.includes('independently stored explicit user approval'));
    for (const key of ['sessionId', 'session', 'shell', 'command', 'cron', 'every']) assert.equal(Object.hasOwn(request, key), false);
  }
  assert.match(plan.requests[0].prompt, /zero wallet positions, zero pending operations/);
  assert.match(plan.requests[1].prompt, /previously approved bot scope/); assert.match(plan.requests[1].prompt, /No unrelated wallet exposure/);
  assertOff(s);
});
test('unsaved/corrupt/hash-mismatch/disabled/DST plans yield no scheduling requests', () => {
  const h = harness(); assert.equal(makeSchedulerPlan(h.store.snapshot(context(), NOW), NOW).requests.length, 0);
  update(h); const saved = h.store.snapshot(context(), NOW); assert.equal(makeSchedulerPlan(saved, NOW).requests.length, 0);
  const fake = clone(saved); fake.configHash = '0'.repeat(64); assert.equal(makeSchedulerPlan(fake, NOW).requests.length, 0);
  fake.configHash = configHash(fake.settings); fake.storage.error = 'corrupt'; assert.equal(makeSchedulerPlan(fake, NOW).requests.length, 0);
  const d = selected(); d.schedule = windowDraft({ timezone: 'America/New_York', start: '02:30', stop: '04:00', dateFrom: '2026-03-08', dateTo: '2026-03-08' });
  const previewOnly = { revision: 1, settings: d, configHash: configHash(d), storage: { error: null } };
  assert.equal(makeSchedulerPlan(previewOnly, Date.parse('2026-03-08T05:00:00Z')).requests.length, 0);
});
test('default atomic filesystem persistence uses restrictive complete files and restart, only in the test root', () => {
  const root = process.env.LIVE_SETTINGS_TEST_ROOT || path.resolve(__dirname, '../../.live-settings-test-tmp');
  fs.mkdirSync(root, { recursive: true }); const dir = fs.mkdtempSync(path.join(root, 'atomic-')), file = path.join(dir, 'draft.json');
  try {
    const store = new LiveSettingsStore({ file });
    const r = store.update({ expectedRevision: 0, settings: selected() }, context(), NOW);
    assert.equal(r.ok, true); assert.equal(fs.statSync(file).mode & 0o777, 0o600);
    assert.deepEqual(fs.readdirSync(dir), ['draft.json']); assert.equal(JSON.parse(fs.readFileSync(file)).revision, 1);
    assert.equal(new LiveSettingsStore({ file }).snapshot(context(), NOW).revision, 1);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});
test('module has only fs/path/crypto imports and never starts timers, HTTP, scheduler devices or runtime code', () => {
  const source = fs.readFileSync(path.resolve(__dirname, '../../apps/vchera2/live-settings.cjs'), 'utf8');
  const imports = [], module = { exports: {} };
  vm.runInNewContext(source, { module, exports: module.exports, require(name) {
    imports.push(name); assert.ok(['node:fs', 'node:path', 'node:crypto'].includes(name)); return require(name);
  }, Buffer, Intl, Date, setTimeout() { throw new Error('timer'); }, setInterval() { throw new Error('interval'); } });
  assert.deepEqual(imports, ['node:fs', 'node:path', 'node:crypto']); assert.equal(typeof module.exports.LiveSettingsStore, 'function');
  assert.equal(/\b(?:setTimeout|setInterval|fetch|exec|spawn)\s*\(/.test(source), false);
  assert.equal(/require\(['"].*(?:runtime|scanner|signer|safety|launch-config)/.test(source), false);
});
test('enabled incomplete window remains a saved incomplete draft, not a ready configuration', () => {
  const h = harness(), d = selected(); d.schedule.enabled = true; d.schedule.start = null;
  const r = update(h, d); assert.equal(r.ok, true); assert.equal(r.snapshot.readiness.draftComplete, false);
  assert.equal(r.snapshot.readiness.preflightReady, false); assert.ok(r.snapshot.readiness.blockers.includes('WINDOW_INCOMPLETE'));
  assert.equal(makeSchedulerPlan(r.snapshot, NOW).requests.length, 0); assertOff(r.snapshot);
});
test('settings are a full replacement, never a partial merge that silently restores omitted defaults', () => {
  const h = harness(); update(h); const before = h.text;
  for (const key of ['capital', 'schedule', 'features', 'symbol']) {
    const d = selected(); delete d[key]; assert.equal(update(h, d, 1).status, 400);
  }
  const d = selected(); delete d.capital.budgetUsdc; assert.equal(update(h, d, 1).status, 400);
  assert.equal(h.text, before); assert.equal(h.calls, 1);
});
test('supported feature flags require actual booleans, not truthy coercions', () => {
  for (const value of ['false', 0, 1, null, {}]) {
    const h = harness(), d = selected(); d.features.noLongs = value;
    assert.equal(update(h, d).ok, false); assert.equal(h.calls, 0);
  }
});
test('reentrant injected persistence cannot acquire a second store transaction', () => {
  let text = null, store, nested;
  const read = () => { if (text === null) throw Object.assign(new Error(), { code: 'ENOENT' }); return text; };
  store = new LiveSettingsStore({ file: '/virtual/reentrant', read, save(file, value) {
    nested = store.update({ expectedRevision: 0, settings: selected() }, context(), NOW);
    text = JSON.stringify(value);
  } });
  const r = store.update({ expectedRevision: 0, settings: selected() }, context(), NOW);
  assert.equal(nested.status, 409); assert.ok(nested.reasons.includes('STORE_TRANSACTION_IN_PROGRESS'));
  assert.equal(r.ok, true); assert.equal(r.snapshot.revision, 1);
});
test('default reader refuses a symlink instead of treating another file as authoritative settings', () => {
  const root = process.env.LIVE_SETTINGS_TEST_ROOT || path.resolve(__dirname, '../../.live-settings-test-tmp');
  fs.mkdirSync(root, { recursive: true }); const dir = fs.mkdtempSync(path.join(root, 'symlink-'));
  try {
    const h = harness(); update(h); const original = path.join(dir, 'original.json'), link = path.join(dir, 'link.json');
    fs.writeFileSync(original, h.text); fs.symlinkSync(original, link);
    const store = new LiveSettingsStore({ file: link }), r = store.update({ expectedRevision: 0, settings: selected() }, context(), NOW);
    assert.equal(r.status, 503); assert.equal(r.snapshot.settings, null); assert.equal(fs.readFileSync(original, 'utf8'), h.text);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});
test('synthetic parent integration example remains a plan-only artifact, not real wallet or device evidence', () => {
  const h = harness(), d = selected(); d.schedule.enabled = true;
  const r = update(h, d), plan = makeSchedulerPlan(r.snapshot, NOW); assert.equal(plan.requests.length, 2); assertOff(r.snapshot);
  if (process.env.LIVE_SETTINGS_EXAMPLE_OUTPUT) fs.writeFileSync(process.env.LIVE_SETTINGS_EXAMPLE_OUTPUT, JSON.stringify({
    syntheticFixtureOnly: true, notActualWalletBalance: true, notASelectedMonetaryBudget: true,
    contextSchemaExample: context(), updateShapeExample: { expectedRevision: 0, settings: d },
    snapshotExample: r.snapshot, schedulerPlanExample: plan, cfgPatchDataOnlyExample: toStrategyPatch(d),
    createdSchedules: 0, orders: 0, signatures: 0,
  }, null, 2) + '\n', { mode: 0o600 });
});
