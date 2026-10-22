'use strict';
// This router chooses instrument contexts, never signs or submits exchange actions.
const fs = require('node:fs');
const { INSTRUMENTS } = require('./market-data.cjs');
const MODES = Object.freeze({ BTC: ['BTC'], ETH: ['ETH'], BOTH: ['BTC', 'ETH'] });
const HANDOFF = 'LIVE заблокирован до общей приёмки deadline-guard/shadow и отдельного разрешения на минимальный BTC. Исследования не дают торговых полномочий; ETH только наблюдение.';
function validMode(mode) { return typeof mode === 'string' && Object.hasOwn(MODES, mode); }
function selectionBlockers(current) {
  const reasons = [];
  if (current.provider?.entrySafety?.unknown?.length) reasons.push('Исход предыдущего entry intent неизвестен; сначала требуется authoritative reconciliation.');
  if (current.LIVE || current.entriesEnabled) reasons.push('Сначала остановите LIVE. Набор инструментов не меняется у работающего бота.');
  const wallet = current.provider && current.provider.wallet;
  if (!wallet || !Array.isArray(wallet.openPositions) || !Array.isArray(wallet.operations)) reasons.push('Нет достоверного состояния позиций и заявок кошелька.');
  else if (wallet.openPositions.length || wallet.operations.length) reasons.push('Набор инструментов нельзя менять при открытых позициях или pending заявках.');
  if ((current.openManaged || []).length) reasons.push('Бот ещё ведёт позицию или неизвестный исход заявки.');
  if (!current.providerFresh) reasons.push('Состояние кошелька устарело; смена инструментов запрещена.');
  return reasons;
}
class InstrumentSelection {
  constructor({ file, save, read = name => fs.readFileSync(name, 'utf8') }) {
    this.file = file; this.save = save; this.mode = 'BTC'; this.error = null; this.changedAt = null;
    try {
      const saved = JSON.parse(read(file));
      if (!validMode(saved.mode)) throw new Error('Invalid saved instrument selection');
      this.mode = saved.mode; this.changedAt = saved.changedAt || null;
    } catch (error) { if (error.code !== 'ENOENT') this.error = 'Сохранённый набор инструментов повреждён. LIVE запрещён до явного выбора.'; }
  }
  snapshot() {
    return { mode: this.mode, selectedSymbols: [...MODES[this.mode]], supportedModes: Object.keys(MODES), changedAt: this.changedAt,
      error: this.error, liveReady: liveTestApproved(), selectionIsArming: false, guardIntegrated: liveTestApproved(), shadowIntegrated: liveTestApproved(),
      reason: HANDOFF, riskScope: 'Общий кошелёк и общий лимит риска. Два тикера не удваивают разрешённый номинал.' };
  }
  startBlockers() { return [this.error, liveTestApproved() ? null : HANDOFF].filter(Boolean); }
  change(body, current) {
    if (!body || typeof body !== 'object' || Array.isArray(body) || Object.keys(body).length !== 1 || !validMode(body.mode)) return { ok: false, status: 400, reasons: ['Ожидается только mode: BTC, ETH или BOTH.'] };
    const reasons = selectionBlockers(current);
    if (reasons.length) return { ok: false, status: 409, reasons };
    const changedAt = new Date().toISOString();
    this.save(this.file, { schemaVersion: 1, mode: body.mode, changedAt, source: 'explicit-user-instrument-selection', enablesTrading: false });
    this.mode = body.mode; this.changedAt = changedAt; this.error = null;
    return { ok: true, selection: this.snapshot(), message: `Выбрано: ${MODES[this.mode].join(' + ')}. LIVE остаётся выключенным. ${HANDOFF}` };
  }
  context(symbol) {
    if (!Object.hasOwn(INSTRUMENTS, symbol)) throw new Error('Unsupported instrument');
    return { ...INSTRUMENTS[symbol], selected: MODES[this.mode].includes(symbol),
      strategyStatus: symbol === 'BTC' ? 'LEGACY_BTC_AWAITING_GUARD' : 'ETH_PROFILE_REQUIRED',
      strategyReady: false, liveReady: false, exchangeMutationsEnabled: false,
      parametersSource: symbol === 'BTC' ? 'Existing locked BTC baseline, unchanged' : 'Not configured: BTC dollar thresholds are NOT copied to ETH',
      required: symbol === 'BTC' ? ['guard integration', 'shadow validation'] : ['asset-specific strategy profile', 'guard integration', 'shadow validation'],
      reason: symbol === 'BTC' ? HANDOFF : 'ETH котировки и маршрутизация готовы. Торговый профиль ETH не подтверждён; BTC стопы и модель выплат не копируются.' };
  }
}
// Explicit user approval for a bounded BTC LIVE test lifts only the handoff blocker; all runtime guards remain.
function liveTestApproved(launch = require('./launch-config.json')) {
  const a = launch.liveTestApproval;
  return !!(a && a.approved === true && typeof a.approvedAt === 'string' && /^BTC only/.test(String(a.scope)) && typeof a.source === 'string' && a.source.length > 10);
}
module.exports = { InstrumentSelection, selectionBlockers, MODES, HANDOFF, liveTestApproved };
