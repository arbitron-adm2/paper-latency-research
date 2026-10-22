// Режим волатильности LOW / NORMAL / HIGH по размаху цены за 5 минут с гистерезисом и выдержкой.
// Чистая функция без БД и сети — покрыта тестами (test/regime.test.js).
// Границы (15.10.2026, запись Binance BTC 12–15.10, 780K точек по 250 мс): размах за 5 мин 33-й перцентиль 82$, 67-й 129$.
// На обучении (до 14.10 12:00) и проверке (после): LOW — стратегия работает (PF 2.8 / 1.9), NORMAL — около нуля (PF 1.3 / 1.0),
// HIGH — минус при любых параметрах (PF 0.9 / 0.9). Поэтому HIGH = остановка входов, а не «другие параметры».
'use strict';

const REGIMES = ['LOW', 'NORMAL', 'HIGH'];

function classify(r5m, low, high) {
  if (!Number.isFinite(r5m) || r5m < 0) return null;
  return r5m < low ? 'LOW' : r5m < high ? 'NORMAL' : 'HIGH';
}

// opts: { low, high, hystPct (0–50), holdMs (выдержка нового режима) }
function createRegime(opts) {
  const o = { low: 82, high: 129, hystPct: 10, holdMs: 120000, ...(opts || {}) };
  if (!(o.low > 0) || !(o.high > o.low)) throw new Error('regime: нужно 0 < low < high');
  if (!(o.hystPct >= 0 && o.hystPct < 50)) throw new Error('regime: hystPct от 0 до 50');
  if (!(o.holdMs >= 0)) throw new Error('regime: holdMs ≥ 0');
  const st = { regime: null, pending: null, since: 0, lastR5m: NaN, switches: 0 };
  const h = o.hystPct / 100;
  // кандидат с учётом гистерезиса: чтобы выйти из текущего режима, размах должен пройти границу с запасом
  function candidate(r5m) {
    if (st.regime === null) return classify(r5m, o.low, o.high);
    if (st.regime === 'LOW') return r5m >= o.high * (1 + h) ? 'HIGH' : r5m >= o.low * (1 + h) ? 'NORMAL' : 'LOW';
    if (st.regime === 'HIGH') return r5m < o.low * (1 - h) ? 'LOW' : r5m < o.high * (1 - h) ? 'NORMAL' : 'HIGH';
    return r5m < o.low * (1 - h) ? 'LOW' : r5m >= o.high * (1 + h) ? 'HIGH' : 'NORMAL';
  }
  function update(r5m, now) {
    now = Number.isFinite(now) ? now : Date.now();
    if (!Number.isFinite(r5m)) return { regime: st.regime, changed: false, stale: true };   // нет данных — режим не меняем
    st.lastR5m = r5m;
    const cand = candidate(r5m);
    if (cand === null) return { regime: st.regime, changed: false, stale: true };
    if (st.regime === null) { st.regime = cand; st.since = now; return { regime: cand, changed: true, stale: false }; }
    if (cand === st.regime) { st.pending = null; return { regime: st.regime, changed: false, stale: false }; }
    if (!st.pending || st.pending.regime !== cand) st.pending = { regime: cand, since: now };
    if (now - st.pending.since < o.holdMs) return { regime: st.regime, changed: false, stale: false, pending: cand };
    st.regime = cand; st.since = now; st.pending = null; st.switches++;
    return { regime: cand, changed: true, stale: false };
  }
  return { update, get regime() { return st.regime; }, get pending() { return st.pending ? st.pending.regime : null; }, get since() { return st.since; }, get switches() { return st.switches; }, opts: o, state: () => ({ ...st, pending: st.pending ? { ...st.pending } : null }) };
}

module.exports = { REGIMES, classify, createRegime };
