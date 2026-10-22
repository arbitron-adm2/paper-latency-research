// Спред Binance ↔ цена Paper (HL mid) в bp и рекомендация guard по правилам стратегии «Вчера 2».
// Правила и пороги взяты из apps/vchera2/launch-config.json и scanner/btcy2-lag.js (decideEntry/queueEntry),
// fundingGuard — из scanner/funding-guard.js. Это СОВЕТ: ничего не отправляет и не блокирует кнопки.
// Не проверяется (нет данных в расширении): перевес стаканов HL/Binance, стенки, режим по 5-минутному размаху,
// дневные лимиты убытка, OI-потолок для размера (его показывает панель).
// Работает в фоне (importScripts) и в Node (тесты): globalThis.PTQSpread / module.exports.
(function (root) {
  'use strict';

  // Пороги в $ на 1 BTC (для ETH масштабируются отношением цен ETH/BTC).
  const RULES = {
    minDev: 3, maxDev: 60,            // вход: отклонение спреда от фона, $
    baselineSec: 120, slowBaselineMin: 30, minSamples: 30, minSlowSamples: 240,
    cushBoth: true,                   // подушка и от быстрого, и от медленного фона
    binMoveSec: 10, vetoAgainstUsd: 10, minBinMove: 2,
    maxBinRange10s: 40,
    g3MaxBinMove: 20, g3MaxCushion: 12,
    roundStepUsd: 1000, roundZoneUsd: 50, roundZoneAboveUsd: 100,   // только BTC
    funding: { noEntryMin: 4, afterMin: 3 },
    staleMs: 3000,                    // цены старше — не доверяем
  };
  const KEEP_FAST_MS = 600000, KEEP_SLOW_MS = 1800000, SAMPLE_MS = 250;

  const median = (xs) => { const a = xs.slice().sort((x, y) => x - y), n = a.length; return n ? (n % 2 ? a[(n - 1) / 2] : (a[n / 2 - 1] + a[n / 2]) / 2) : NaN; };
  const bp = (usd, px) => (px > 0 && Number.isFinite(usd) ? (usd / px) * 1e4 : NaN);

  // Причина «не входить» из-за фандинга (начало каждого часа) или null. now — мс.
  function fundingBlock(now, f = RULES.funding) {
    const d = new Date(now), since = d.getMinutes() * 60000 + d.getSeconds() * 1000 + d.getMilliseconds(), ms = 3600000 - since;
    if (f.noEntryMin > 0 && ms <= f.noEntryMin * 60000) return `перед фандингом: ещё ${Math.ceil((ms + f.afterMin * 60000) / 1000)} с без входов`;
    if (f.afterMin > 0 && since < f.afterMin * 60000) return `после фандинга: входы через ${Math.ceil((f.afterMin * 60000 - since) / 1000)} с`;
    return null;
  }
  // Круглый уровень BTC: цена ближе зоны (под — roundZoneUsd, над — roundZoneAboveUsd) → причина, иначе null.
  function roundBlock(px, r = RULES) {
    const s = r.roundStepUsd, z = r.roundZoneUsd, za = r.roundZoneAboveUsd > 0 ? r.roundZoneAboveUsd : z;
    if (!(s > 0 && px > 0)) return null;
    const lv = Math.round(px / s) * s, off = px - lv, zone = off >= 0 ? za : z;
    return Math.abs(off) < zone ? `круглый уровень ${lv}: цена ${off >= 0 ? 'над' : 'под'} ним в ${Math.abs(off).toFixed(0)}$` : null;
  }

  function createModel() {
    // по рынку: samples [[t, spread=hl−bf]], fut [[t, bf]] (тики 60 с), spot — последний тик
    const M = {};
    const get = (m) => M[m] || (M[m] = { samples: [], fut: [], spot: null, hl: null, lastSample: 0, slow: { at: 0, v: NaN, n: 0 } });
    function trim(a, from) { let i = 0; while (i < a.length && a[i][0] < from) i++; if (i) a.splice(0, i); }
    return {
      tick(v, m, t, p) {
        const s = get(m);
        if (v === 'f') { s.fut.push([t, p]); if (s.fut.length > 20000) s.fut.splice(0, s.fut.length - 20000); trim(s.fut, t - 60000); }
        else s.spot = [t, p];
      },
      // Раз в SAMPLE_MS: цена HL (mid) и последний фьючерсный тик → точка спреда.
      sample(m, t, hl, hlT = t) {
        const s = get(m);
        s.hl = [hlT, hl];
        const f = s.fut[s.fut.length - 1];
        if (!(hl > 0) || !f || t - f[0] > RULES.staleMs || t - hlT > RULES.staleMs || t - s.lastSample < SAMPLE_MS - 20) return;
        s.lastSample = t;
        s.samples.push([t, hl - f[1]]);
        trim(s.samples, t - KEEP_SLOW_MS);
      },
      // ctx: { timingAllow: bool|null, timingReasons: [], paused: bool, scale: число (для ETH), scaleOk: bool }
      evaluate(m, now, ctx = {}) {
        const s = get(m), R = RULES;
        const hlA = s.hl, fA = s.fut[s.fut.length - 1], sA = s.spot;
        const k = ctx.scale > 0 ? ctx.scale : 1;
        const out = { market: m, t: now, ready: false, rec: 'wait', side: null, title: 'нет данных', rules: [], spreadUsd: NaN, spreadBp: NaN, devUsd: NaN, devBp: NaN,
          spotBp: NaN, hl: hlA ? hlA[1] : NaN, bf: fA ? fA[1] : NaN, bs: sA ? sA[1] : NaN, note: 'Не проверено: стаканы, стенки, режим по размаху, дневные лимиты.' };
        if (!hlA || !fA || now - hlA[0] > R.staleMs || now - fA[0] > R.staleMs) { out.title = 'нет свежих цен'; out.stale = true; return out; }
        const hl = hlA[1], bf = fA[1];
        out.spreadUsd = hl - bf; out.spreadBp = bp(hl - bf, hl);
        if (sA && now - sA[0] <= R.staleMs) out.spotBp = bp(hl - sA[1], hl);
        const fast = s.samples.filter((x) => x[0] >= now - R.baselineSec * 1000).map((x) => x[1]);
        if (fast.length < R.minSamples) { out.title = `фон считаю… ${fast.length}/${R.minSamples}`; return out; }
        const baseline = median(fast), dev = (hl - bf) - baseline;
        if (now - s.slow.at > 5000) {   // медленный фон считаем не чаще раза в 5 с
          const xs = s.samples.filter((x) => x[0] >= now - R.slowBaselineMin * 60000).map((x) => x[1]);
          s.slow = { at: now, n: xs.length, v: xs.length >= R.minSlowSamples ? median(xs) : NaN };
        }
        out.ready = true; out.baselineUsd = baseline; out.devUsd = dev; out.devBp = bp(dev, hl);
        const minDev = R.minDev * k, maxDev = R.maxDev * k;
        const ad = Math.abs(dev);
        // dev > 0: HL дороже фона относительно Binance → HL должен упасть → ШОРТ; dev < 0 → ЛОНГ (как в btcy2-lag).
        const side = ad >= minDev && ad <= maxDev ? (dev > 0 ? 'S' : 'L') : null;
        out.side = side;
        const dir = side === 'L' ? 1 : side === 'S' ? -1 : 0;
        const bfAgo = (() => { const t0 = now - R.binMoveSec * 1000; let v = NaN; for (const x of s.fut) { if (x[0] <= t0) v = x[1]; else break; } return v; })();
        const binMove = dir && Number.isFinite(bfAgo) ? dir * (bf - bfAgo) : NaN;
        let lo = Infinity, hi = -Infinity;
        for (let i = s.fut.length - 1; i >= 0 && s.fut[i][0] >= now - 10000; i--) { const v = s.fut[i][1]; if (v < lo) lo = v; if (v > hi) hi = v; }
        const range10 = hi >= lo ? hi - lo : 0;
        out.binMoveUsd = binMove; out.range10Usd = range10;
        const slowCush = dir && Number.isFinite(s.slow.v) ? dir * -((hl - bf) - s.slow.v) : NaN;
        const rule = (id, ok, text) => out.rules.push({ id, ok, text });
        rule('signal', !!side, side ? `отклонение ${ad.toFixed(1)}$ (${Math.abs(out.devBp).toFixed(2)} bp) в [${minDev.toFixed(1)}; ${maxDev.toFixed(0)}]$` : `отклонение ${ad.toFixed(1)}$ вне [${minDev.toFixed(1)}; ${maxDev.toFixed(0)}]$`);
        const fb = fundingBlock(now); rule('funding', !fb, fb || 'вне окна фандинга');
        rule('speed', range10 <= R.maxBinRange10s * k, range10 <= R.maxBinRange10s * k ? `Binance за 10 с ${range10.toFixed(1)}$ (≤ ${(R.maxBinRange10s * k).toFixed(0)})` : `рынок слишком быстрый: Binance ${range10.toFixed(1)}$ за 10 с (порог ${(R.maxBinRange10s * k).toFixed(0)})`);
        if (side) {
          rule('veto', !(binMove <= -R.vetoAgainstUsd * k), binMove <= -R.vetoAgainstUsd * k ? `Binance идёт против: ${binMove.toFixed(1)}$ за ${R.binMoveSec} с` : `Binance не против (${Number.isFinite(binMove) ? binMove.toFixed(1) : '—'}$)`);
          rule('binmove', binMove >= R.minBinMove * k, `Binance в нашу сторону ${Number.isFinite(binMove) ? binMove.toFixed(1) : '—'}$ (нужно ≥ ${(R.minBinMove * k).toFixed(1)})`);
          rule('late', !(binMove >= R.g3MaxBinMove * k), binMove >= R.g3MaxBinMove * k ? `поздний вход: Binance уже прошёл ${binMove.toFixed(0)}$` : 'не поздно');
          rule('jerk', !(ad >= R.g3MaxCushion * k), ad >= R.g3MaxCushion * k ? `подушка ${ad.toFixed(1)}$ — рывок, не отставание (порог ${(R.g3MaxCushion * k).toFixed(0)})` : 'не рывок');
          if (R.cushBoth) rule('slow', slowCush >= minDev, Number.isFinite(slowCush) ? `подушка от среднего за ${R.slowBaselineMin} мин ${slowCush.toFixed(1)}$ (нужно ≥ ${minDev.toFixed(1)})` : `средний за ${R.slowBaselineMin} мин ещё не посчитан (${s.slow.n}/${R.minSlowSamples})`);
        }
        if (m === 'BTC') { const rb = roundBlock(hl); rule('round', !rb, rb || 'вдали от круглого уровня'); }
        rule('queue', ctx.timingAllow !== false, ctx.timingAllow === false ? `очередь: ${(ctx.timingReasons || []).join(', ') || 'не проходит по времени'}` : ctx.timingAllow === null || ctx.timingAllow === undefined ? 'очередь: нет оценки (не блокирую)' : 'очередь проходит по времени');
        rule('paused', !ctx.paused, ctx.paused ? 'торговля на паузе' : 'торговля идёт');
        const bad = out.rules.filter((r) => !r.ok);
        if (!side) { out.rec = 'none'; out.title = bad.length > 1 ? 'нет сигнала' : 'нет сигнала'; }
        else if (bad.length) { out.rec = 'wait'; out.title = `ЖДАТЬ · ${bad[0].text}`; }
        else { out.rec = side === 'L' ? 'long' : 'short'; out.title = side === 'L' ? 'ЛОНГ' : 'ШОРТ'; }
        if (m === 'ETH' && !ctx.scaleOk) out.note = 'ETH: пороги BTC масштабированы по отношению цен — оценочно. ' + out.note;
        return out;
      },
      reset() { for (const k of Object.keys(M)) delete M[k]; },
    };
  }

  const api = { RULES, createModel, fundingBlock, roundBlock, median, bp, SAMPLE_MS };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.PTQSpread = api;
})(typeof globalThis !== 'undefined' ? globalThis : this);
