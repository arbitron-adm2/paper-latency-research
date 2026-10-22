// ⏰ Фандинг в начале каждого часа (Влад 16.10 ~00:35: «бот не должен открываться перед 00 — около фандинга; перед 00 закрывать
// позиции и вообще не входить за 3–4 минуты… вообще любой, все боты так должны»; ~01:00: «но выключаемую, чтобы в панели было видно»).
// Общие правила для всех ботов. Настройки — в панели основного BTC-бота (fundingGuard, fundingNoEntryMin, fundingCloseSec):
// btc-lag.js передаёт их сюда через setCfg при старте и при каждом сохранении; остальные боты берут отсюда же.
// afterMin — после XX:00 ещё столько минут без входов (Влад 16.10 ~01:05: «давай 3 минуты, основное движение там 3 минуты идёт»)
let S = { enabled: true, noEntryMin: 4, closeSec: 60, afterMin: 3 };
function setCfg(c) {
  if (!c) return;
  const n = (v, d) => (Number.isFinite(+v) ? +v : d);
  S = { enabled: c.fundingGuard !== false, noEntryMin: n(c.fundingNoEntryMin, 4), closeSec: n(c.fundingCloseSec, 60), afterMin: n(c.fundingAfterMin, 3) };
}
const msToHour = (now = Date.now()) => { const d = new Date(now); return 3600000 - (d.getMinutes() * 60000 + d.getSeconds() * 1000 + d.getMilliseconds()); };
const nextHour = (now = Date.now()) => { const d = new Date(now + msToHour(now)); return String(d.getHours()).padStart(2, '0') + ':00'; };
// причина «не входить» или null
function noEntry(now = Date.now()) {
  if (!S.enabled) return null;
  const ms = msToHour(now), since = 3600000 - ms;   // до следующего XX:00 / после прошедшего XX:00
  if (S.noEntryMin > 0 && ms <= S.noEntryMin * 60000) return `перед фандингом ${nextHour(now)}: входов нет ещё ${Math.ceil((ms + S.afterMin * 60000) / 1000)} с`;
  if (S.afterMin > 0 && since < S.afterMin * 60000) return `после фандинга: основное движение — входы через ${Math.ceil((S.afterMin * 60000 - since) / 1000)} с`;
  return null;
}
// пора закрывать открытые позиции
const mustClose = (now = Date.now()) => S.enabled && S.closeSec > 0 && msToHour(now) <= S.closeSec * 1000;
const CLOSE_REASON = 'перед фандингом (закрываю до начала часа)';
const getCfg = () => ({ ...S });
module.exports = { noEntry, mustClose, msToHour, setCfg, getCfg, CLOSE_REASON };
