// Ядро расчётов «Очереди papertrade»: разбор сообщений потока и вычисления без DOM.
// Работает и в расширении (фон + окно), и в Node (тесты): в браузере — globalThis.PTQ, в Node — module.exports.
(function (root) {
  'use strict';

  const E18 = 1e18;
  const MARKETS = ['BTC', 'ETH'];
  const STALE_MS = 5000;            // как у самой платформы: снимок очереди старше 5 с — устарел
  const TREND_MS = 15000;           // тренд затора — за 15 с
  const DRAIN_BLOCKS = 20;          // скорость очереди — по последним 20 блокам
  const DEFAULT_GAS_PER_INTENT = 310000; // газ на одну заявку, пока не посчитали из живой очереди

  // ---------- проверка и приведение чисел ----------
  // Пустое значение НЕ превращаем в 0: возвращаем null, и окно пишет «нет данных».
  function num(x) {
    if (x === null || x === undefined || typeof x === 'boolean' || (typeof x === 'string' && !x.trim())) return null;
    const n = typeof x === 'number' ? x : Number(x);
    return Number.isFinite(n) ? n : null;
  }
  // Большие числа в 1e18 (строкой) → обычное число; точности double хватает (дальше округляем до $).
  function fromRaw18(x) {
    if (typeof x === 'number') return Number.isFinite(x) ? x / E18 : null;
    if (typeof x !== 'string' || !/^\d+$/.test(x)) return null;
    if (x.length <= 15) return Number(x) / E18;
    const whole = x.length > 18 ? x.slice(0, x.length - 18) : '0';
    const frac = (x.length > 18 ? x.slice(-18) : x.padStart(18, '0')).slice(0, 9);
    return Number(whole) + Number(frac) / 1e9;
  }

  // ---------- канал pressure:current (очередь посредника и блоки HyperEVM) ----------
  function parsePressure(data) {
    const s = data && data.snapshot;
    if (!s || typeof s !== 'object') return { ok: false, error: 'нет snapshot' };
    const limit = num(s.blockGasLimit);
    const queuedGas = num(s.totalQueuedGas);
    const submittedGas = num(s.totalSubmittedGas);
    const queued = num(s.queuedIntents);
    const submitted = num(s.submittedIntents);
    const ts = num(s.ts);
    if (!limit || limit <= 0 || queuedGas === null || submittedGas === null || queued === null || submitted === null || ts === null) {
      return { ok: false, error: 'не хватает полей очереди' };
    }
    const blocks = [];
    const chainBlocks = s.chain && Array.isArray(s.chain.blocks) ? s.chain.blocks : [];
    for (const b of chainBlocks) {
      const n = num(b && b.number), t = num(b && b.timestampMs), gas = num(b && b.gasUsed), pt = num(b && b.papertradeGasUsed), lim = num(b && b.gasLimit);
      if (n === null || t === null || gas === null || pt === null || !lim) continue;
      blocks.push({ n, t, gas, pt, limit: lim, big: b.kind && b.kind !== 'ordinary' });
    }
    const preview = [];
    if (Array.isArray(s.queuePreview)) {
      for (const q of s.queuePreview) {
        const at = num(q && q.acceptedAtMs);
        if (at === null) continue;
        preview.push({ action: String(q.action || ''), at, gas: num(q.estimatedGas) });
      }
    }
    return {
      ok: true,
      ts, stale: !!s.stale || s.ok === false, limit,
      queuedGas, submittedGas, pendingGas: queuedGas + submittedGas,
      queued, submitted,
      blocks, preview,
      revision: typeof data.queueRevision === 'string' ? data.queueRevision : null,
    };
  }

  // Модель очереди: копит блоки и историю затора между снимками (в снимке только 5 последних блоков).
  function createQueueModel() {
    let latest = null;
    let gasPerIntent = DEFAULT_GAS_PER_INTENT;
    let gasPerIntentMeasured = false;
    const blocks = new Map();
    const hist = []; // { t, backlog }

    function add(p) {
      if (!p || !p.ok) return false;
      if (latest && p.ts < latest.ts) return false; // старое после переподключения
      latest = p;
      for (const b of p.blocks) blocks.set(b.n, b);
      if (blocks.size > 80) for (const n of [...blocks.keys()].sort((a, b) => a - b).slice(0, blocks.size - 60)) blocks.delete(n);
      if (p.queued > 0 && p.queuedGas > 0) { gasPerIntent = p.queuedGas / p.queued; gasPerIntentMeasured = true; }
      hist.push({ t: p.ts, backlog: p.pendingGas / p.limit });
      while (hist.length && hist[0].t < p.ts - TREND_MS - 5000) hist.shift();
      return true;
    }

    function summary(now) {
      if (!latest) return null;
      const p = latest;
      const age = Math.max(0, now - p.ts);
      const fresh = !p.stale && age <= STALE_MS;
      const ordered = [...blocks.values()].sort((a, b) => a.n - b.n);
      const recent = ordered.filter((b) => !b.big).slice(-DRAIN_BLOCKS);
      // Сколько газа papertrade реально уходит в блоки за секунду (блоки ~1 с, считаем по их времени).
      let drainPerSec = null;
      if (recent.length >= 3) {
        const span = (recent[recent.length - 1].t - recent[0].t) / 1000 + 1;
        const sum = recent.reduce((a, b) => a + b.pt, 0);
        drainPerSec = span > 0 ? sum / span : null;
      }
      const backlog = p.pendingGas / p.limit; // затор в блоках
      const ago = hist.find((h) => h.t >= p.ts - TREND_MS);
      const trend = ago && ago.t < p.ts ? backlog - ago.backlog : null;
      const lastFull = recent.slice(-5);
      const full5 = lastFull.length ? lastFull.reduce((a, b) => a + b.gas / b.limit, 0) / lastFull.length : null;
      // Ожидание: весь газ впереди / скорость. Меньше одного блока — следующий блок (~1 с).
      let waitSec = null, stalled = false;
      if (p.pendingGas <= 0) waitSec = 0;
      else if (backlog < 1) waitSec = 1;
      else if (drainPerSec !== null && drainPerSec > p.limit * 0.01) waitSec = p.pendingGas / drainPerSec;
      else if (drainPerSec !== null) stalled = true;
      const perSec = drainPerSec !== null ? drainPerSec / gasPerIntent : null;
      // Состояние — пороги как у paperwatch: <1 блока свободно, ≥10 блоков затор.
      let state = 'clear';
      if (backlog >= 10 || stalled) state = 'backed';
      else if (backlog >= 1 || (trend !== null && trend >= 0.5 && full5 !== null && full5 >= 0.8)) state = 'building';
      const frontWaitSec = p.preview.length ? Math.max(0, (now - Math.min(...p.preview.map((q) => q.at))) / 1000) : null;
      return {
        ts: p.ts, ageMs: age, fresh, platformStale: p.stale,
        ahead: p.queued + p.submitted, queued: p.queued, submitted: p.submitted,
        pendingGas: p.pendingGas, limit: p.limit, backlog, trend,
        drainPerSec, intentsPerSec: perSec, gasPerIntent, gasPerIntentMeasured,
        waitSec, stalled, state, frontWaitSec,
        blocks: ordered.slice(-24).map((b) => ({ n: b.n, gas: b.gas, pt: b.pt, limit: b.limit, big: !!b.big })),
        lastBlock: ordered.length ? ordered[ordered.length - 1].n : null,
      };
    }
    return { add, summary, reset() { latest = null; blocks.clear(); hist.length = 0; } };
  }

  // ---------- канал trading:current (рынки, OI, потолки) ----------
  // Поля рынка — массив. Проверено сверкой с paperwatch 17.10.2026: последние 4 значения —
  // OI лонг, OI шорт, потолок лонг, потолок шорт (в монетах, ×1e18); [1] — тикер; [14] — макс. плечо;
  // [15] — макс. размер одной позиции в $ (×1e18, сейчас 10 млн$).
  function parseTrading(data) {
    if (!data || !Array.isArray(data.instruments)) return { ok: false, error: 'нет instruments' };
    const markets = {};
    for (const ins of data.instruments) {
      if (!Array.isArray(ins) || ins.length < 8) continue;
      const sym = typeof ins[1] === 'string' ? ins[1].toUpperCase() : null;
      if (!sym) continue;
      const n = ins.length;
      const longOi = fromRaw18(ins[n - 4]), shortOi = fromRaw18(ins[n - 3]), longCap = fromRaw18(ins[n - 2]), shortCap = fromRaw18(ins[n - 1]);
      if ([longOi, shortOi, longCap, shortCap].some((v) => v === null)) continue;
      const maxLev = n > 14 ? num(ins[14]) : null;
      const maxPos = n > 15 ? fromRaw18(ins[15]) : null;
      markets[sym] = { sym, longOi, shortOi, longCap, shortCap, maxLev, maxPos };
    }
    if (!Object.keys(markets).length) return { ok: false, error: 'рынки не разобраны' };
    const minOpen = fromRaw18(data.minimumOpenNotionalRaw);
    return {
      ok: true,
      paused: data.tradingPaused === true,
      block: num(data.indexedThroughBlock),
      minOpenNotional: minOpen !== null ? minOpen : null,
      markets,
      params: tradingParams(data),
    };
  }

  // Все настройки биржи из trading:current, кроме того, что меняется само каждую секунду (OI, потолки,
  // номер блока, накопитель наград). Ключ — понятное название, значение — строка для сравнения «было → стало».
  const INS_NAMES = { 12: 'множитель среза (rateMultiplier)', 13: 'множитель позиции (positionMultiplier)', 14: 'макс. плечо', 15: 'макс. размер позиции, $', 17: 'базовая ставка среза (baseRate)', 18: 'опорный размер (referenceNotional)' };
  function show(v) {
    if (v === null || v === undefined) return '—';
    if (typeof v === 'string' && /^\d{12,}$/.test(v)) { const n = fromRaw18(v); return n === null ? v : String(+n.toPrecision(10)); }
    return typeof v === 'object' ? JSON.stringify(v) : String(v);
  }
  function tradingParams(d) {
    const p = {};
    if (!d || typeof d !== 'object') return p;
    p['торговля на паузе'] = show(d.tradingPaused);
    p['мин. позиция, $'] = show(d.minimumOpenNotionalRaw);
    p['мин. сделка'] = show(d.minimumTradeSizeRaw);
    p['комиссия с выигрыша'] = show(d.winFeeRateRaw);
    p['общее макс. плечо'] = show(d.maximumLeverage);
    p['общее мин. плечо'] = show(d.minimumLeverage);
    p['ограничения приёма заявок'] = show(d.intakeControls);
    if (Array.isArray(d.instruments)) {
      for (const ins of d.instruments) {
        if (!Array.isArray(ins) || typeof ins[1] !== 'string') continue;
        const sym = ins[1].toUpperCase();
        for (let i = 2; i < ins.length - 4; i++) p[`${sym}: ${INS_NAMES[i] || `поле [${i}]`}`] = show(ins[i]);
      }
    }
    return p;
  }
  function diffParams(base, cur) {
    const out = [];
    if (!base || !cur) return out;
    for (const k of Object.keys(cur)) if (k in base && base[k] !== cur[k]) out.push({ key: k, from: base[k], to: cur[k] });
    for (const k of Object.keys(base)) if (!(k in cur)) out.push({ key: k, from: base[k], to: 'пропало' });
    return out;
  }

  // Пополнение места OI: каждые ~15 блоков биржа поднимает потолок до «открыто + запас» (замер 17.10.2026:
  // BTC ≈ 4.5 млн$, ETH ≈ 2.8 млн$). Следим за тиками пополнения, размером запаса, урезанием потолка
  // и закрытыми сторонами (потолок ниже открытого). Возвращает события для журнала и сводку для окна.
  function createCapWatch() {
    const prev = {};        // key → { cap, oi }
    const ticks = [];       // { block, t } — когда хоть один потолок поднялся
    const head = {};        // key → [запас в $ после пополнения]
    const locked = {};      // key → bool
    let headBase = {};      // key → запас, о котором уже сообщили (для «запас изменился»)
    let lastFeed = 0;

    function feed(t, now, mids) {
      const events = [];
      if (!t || !t.ok) return events;
      // Перерыв в данных больше 20 с (обычно они идут каждую секунду) — сравнивать не с чем:
      // начинаем заново, без записей в журнал.
      if (now - lastFeed > 20000) { for (const k of Object.keys(prev)) delete prev[k]; for (const k of Object.keys(locked)) delete locked[k]; }
      lastFeed = now;
      let tick = false;
      for (const sym of Object.keys(t.markets)) {
        const m = t.markets[sym];
        const px = mids && Number.isFinite(mids[sym]) ? mids[sym] : null;
        for (const side of ['long', 'short']) {
          const key = `${sym}:${side}`;
          const cap = side === 'long' ? m.longCap : m.shortCap;
          const oi = side === 'long' ? m.longOi : m.shortOi;
          const p = prev[key];
          if (p && px) {
            if (cap > p.cap * (1 + 1e-9)) {
              tick = true;
              const h = (cap - oi) * px;
              if (h > 0) { (head[key] = head[key] || []).push(h); if (head[key].length > 7) head[key].shift(); }
              const typ = median(head[key] || []);
              if (head[key] && head[key].length >= 3) {
                if (!headBase[key]) headBase[key] = typ;
                else if (Math.abs(typ - headBase[key]) / headBase[key] > 0.35) {
                  events.push({ t: now, kind: 'headroom', key, text: `${label(key)}: пополнение стало ${money(typ)} (было ${money(headBase[key])})` });
                  headBase[key] = typ;
                }
              }
            } else if (cap < p.cap * (1 - 1e-9)) {
              // Потолок опускается и сам, когда позиции закрыли (держится «открыто + запас») — это не урезание.
              // Урезание — когда из-за потолка свободного места стало заметно меньше: больше полутора обычных
              // пополнений (или 3 млн$, пока пополнение не замерено).
              const typ = median(head[key] || []);
              const lost = ((p.cap - p.oi) - (cap - oi)) * px;
              if (lost > Math.max(1e6, typ ? 1.5 * typ : 3e6)) {
                events.push({ t: now, kind: 'cut', key, text: `${label(key)}: потолок урезан ${money(p.cap * px)} → ${money(cap * px)} (место −${money(lost)})` });
              }
            }
          }
          const isLocked = px ? (cap - oi) * px < 1000 && cap < oi : false;
          if (px && p && locked[key] !== undefined && isLocked !== locked[key]) {
            events.push({ t: now, kind: isLocked ? 'locked' : 'unlocked', key, text: isLocked
              ? `${label(key)}: закрыт — потолок ${money(cap * px)} ниже открытого ${money(oi * px)}`
              : `${label(key)}: снова открыт, свободно ${money(Math.max(0, cap - oi) * px)}` });
          }
          if (px) locked[key] = isLocked;
          prev[key] = { cap, oi };
        }
      }
      if (tick) { ticks.push({ block: t.block, t: now }); if (ticks.length > 12) ticks.shift(); }
      return events;
    }

    // Период пополнения — по последним тикам; пропущенный тик (место не выбрали) даёт двойной интервал, его отбрасываем.
    function periodMs() {
      if (ticks.length < 2) return null;
      const d = ticks.slice(1).map((x, i) => x.t - ticks[i].t).filter((x) => x > 3000);
      if (!d.length) return null;
      const min = Math.min(...d);
      return median(d.filter((x) => x <= min * 1.5));
    }
    function status(key, now) {
      const per = periodMs();
      const last = ticks.length ? ticks[ticks.length - 1].t : null;
      let nextInSec = null;
      if (per && last) { const since = now - last; nextInSec = (per - (since % per)) / 1000; }
      return { headroom: head[key] && head[key].length ? median(head[key]) : null, nextInSec, periodSec: per ? per / 1000 : null, locked: !!locked[key] };
    }
    return { feed, status, setHeadBase(b) { headBase = { ...(b || {}) }; }, headBase: () => ({ ...headBase }) };
  }
  function median(a) { if (!a || !a.length) return null; const s = [...a].sort((x, y) => x - y); return s[Math.floor(s.length / 2)]; }
  function label(key) { const [sym, side] = key.split(':'); return `${sym} ${side === 'long' ? 'лонг' : 'шорт'}`; }

  // Место по сторонам рынка в $ и сколько позиций моего размера туда влезет.
  function oiSummary(market, mid, mySize, minOpenNotional) {
    if (!market || !Number.isFinite(mid) || mid <= 0) return null;
    const minOpen = Number.isFinite(minOpenNotional) && minOpenNotional > 0 ? minOpenNotional : 1000;
    const side = (oiU, capU) => {
      const oi = oiU * mid, cap = capU * mid, room = Math.max(0, cap - oi);
      let fits = null, perPos = null;
      if (Number.isFinite(mySize) && mySize > 0) {
        perPos = Number.isFinite(market.maxPos) && market.maxPos > 0 ? Math.min(mySize, market.maxPos) : mySize;
        fits = room >= minOpen ? Math.floor(room / perPos) : 0;
      }
      return { oi, cap, room, over: Math.max(0, oi - cap), full: room < minOpen, tight: room < 1e6, usedPct: cap > 0 ? Math.min(100, (oi / cap) * 100) : null, fits, perPos };
    };
    return {
      long: side(market.longOi, market.longCap),
      short: side(market.shortOi, market.shortCap),
      maxPos: market.maxPos, maxLev: market.maxLev, minOpen,
      sizeOverMax: Number.isFinite(mySize) && Number.isFinite(market.maxPos) && mySize > market.maxPos,
    };
  }

  // Полоска места: свободное место относительно наибольшего за последние windowMs (как «бак» у paperwatch).
  // Потолок OI плавающий — место выбирают и оно снова набегает; полоска показывает, сколько осталось.
  function roomTracker(windowMs = 300000) {
    const hist = new Map(); // ключ → [{t, room}]
    return function track(key, room, now) {
      if (!Number.isFinite(room)) return null;
      const h = hist.get(key) || [];
      if (!h.length || now - h[h.length - 1].t >= 1000 || room > h[h.length - 1].room) h.push({ t: now, room });
      while (h.length && h[0].t < now - windowMs) h.shift();
      hist.set(key, h);
      const max = Math.max(room, ...h.map((x) => x.room));
      return max > 0 ? Math.min(1, room / max) : 0;
    };
  }

  // ---------- канал public:trades (все сделки платформы) ----------
  function parseTrades(data) {
    const list = Array.isArray(data) ? data : data ? [data] : [];
    const out = [];
    for (const x of list) {
      const t = num(x && x.ts), notional = num(x && x.notionalUsd);
      const sym = x && typeof x.instrument === 'string' ? x.instrument.toUpperCase() : null;
      const side = x && (x.side === 'long' || x.side === 'short') ? x.side : null;
      const kind = x && typeof x.kind === 'string' ? (x.kind === 'open' ? 'open' : /liq/i.test(x.kind) ? 'liq' : 'close') : null;
      if (t === null || notional === null || !sym || !side || !kind) continue;
      out.push({ seq: x.seq != null ? String(x.seq) : `${t}:${x.positionId}:${kind}`, t, sym, side, kind, notional, lev: num(x.leverage) });
    }
    return out;
  }

  // Активность за окно: сколько открыли/закрыли по сторонам (кто забирает место OI).
  function fillsSummary(fills, sym, now, windowMs) {
    const res = { windowMs, long: { open: 0, openUsd: 0, close: 0, closeUsd: 0 }, short: { open: 0, openUsd: 0, close: 0, closeUsd: 0 }, liq: 0, count: 0 };
    for (const f of fills) {
      if (f.sym !== sym || now - f.t > windowMs || f.t > now + 5000) continue;
      res.count++;
      const s = res[f.side];
      if (f.kind === 'open') { s.open++; s.openUsd += f.notional; } else { s.close++; s.closeUsd += f.notional; if (f.kind === 'liq') res.liq++; }
    }
    return res;
  }

  // Настройки из сводки платформы (/query/protocol/summary): порог кассы для стейкеров и начисление PAPER.
  function summaryParams(j) {
    const p = {};
    const pp = j && j.paper;
    if (!pp || typeof pp !== 'object') return p;
    p['порог кассы для стейкеров, $'] = show(pp.cap);
    p['PAPER: rate'] = show(pp.rate);
    p['PAPER: decay'] = show(pp.decay);
    p['PAPER: cliff'] = show(pp.cliff);
    return p;
  }

  // ---------- paperwatch.app (запасной источник) → тот же вид, что у papertrade ----------
  function pressureFromPaperwatch(p) {
    if (!p || num(p.t) === null || !num(p.limit)) return { ok: false, error: 'paperwatch: плохой pressure' };
    const blocks = (Array.isArray(p.blocks) ? p.blocks : []).map((b) => ({ n: num(b.n), t: num(b.t), gas: num(b.gas), pt: num(b.pt), limit: num(b.limit), big: b.kind && b.kind !== 'ordinary' }))
      .filter((b) => b.n !== null && b.t !== null && b.gas !== null && b.pt !== null && b.limit);
    const queued = num(p.queued), submitted = num(p.submitted), qg = num(p.queuedGas), sg = num(p.submittedGas);
    if ([queued, submitted, qg, sg].some((v) => v === null)) return { ok: false, error: 'paperwatch: не хватает полей' };
    return { ok: true, ts: num(p.t), stale: !!p.stale, limit: num(p.limit), queuedGas: qg, submittedGas: sg, pendingGas: qg + sg, queued, submitted, blocks, preview: [], revision: null };
  }
  function tradingFromPaperwatch(o, mids) {
    if (!o || !Array.isArray(o.markets)) return { ok: false, error: 'paperwatch: плохой oi' };
    const markets = {};
    for (const m of o.markets) {
      const sym = typeof m.symbol === 'string' ? m.symbol.toUpperCase() : null;
      const px = sym && mids ? num(mids[sym]) : null;
      const vals = [num(m.longOi), num(m.shortOi), num(m.longCap), num(m.shortCap)];
      if (!sym || !px || vals.some((v) => v === null)) continue;
      markets[sym] = { sym, longOi: vals[0] / px, shortOi: vals[1] / px, longCap: vals[2] / px, shortCap: vals[3] / px, maxLev: null, maxPos: 10e6 };
    }
    if (!Object.keys(markets).length) return { ok: false, error: 'paperwatch: рынки не разобраны' };
    return { ok: true, paused: false, block: null, minOpenNotional: 1000, markets };
  }

  // ---------- вывод ----------
  function money(v) {
    if (!Number.isFinite(v)) return '—';
    const a = Math.abs(v);
    if (a >= 1e9) return `${(v / 1e9).toFixed(2)} млрд$`;
    if (a >= 1e6) return `${(v / 1e6).toFixed(a >= 1e8 ? 0 : a >= 1e7 ? 1 : 2)} млн$`;
    if (a >= 1e3) return `${Math.round(v / 1e3)} тыс.$`;
    return `${Math.round(v)}$`;
  }
  function waitText(q) {
    if (!q) return '—';
    if (q.stalled) return 'стоит';
    if (q.waitSec === null) return '—';
    if (q.waitSec === 0) return 'нет очереди';
    if (q.waitSec < 90) return `≈ ${Math.max(1, Math.round(q.waitSec))} с`;
    return `≈ ${(q.waitSec / 60).toFixed(1)} мин`;
  }
  function plural(n, one, few, many) {
    const a = Math.abs(n) % 100, b = a % 10;
    if (a > 10 && a < 20) return many;
    if (b === 1) return one;
    if (b >= 2 && b <= 4) return few;
    return many;
  }
  // «2.2 заявки», «5 заявок», «21 заявка» (дробное — всегда «заявки»).
  function intentsText(x) {
    if (!Number.isFinite(x)) return '—';
    const v = x.toFixed(x < 10 ? 1 : 0);
    if (v.includes('.') && !v.endsWith('.0')) return `${v} заявки`;
    const n = Math.round(x);
    return `${n} ${plural(n, 'заявка', 'заявки', 'заявок')}`;
  }
  // «0.4 блока», «1.9 блока», «3 блока», «12 блоков» (дробное — всегда «блока»).
  function blocksText(x) {
    if (!Number.isFinite(x)) return '—';
    const v = x.toFixed(1);
    if (!v.endsWith('.0')) return `${v} блока`;
    const n = Math.round(x);
    return `${n} ${plural(n, 'блок', 'блока', 'блоков')}`;
  }

  const api = {
    MARKETS, STALE_MS, TREND_MS, DEFAULT_GAS_PER_INTENT,
    num, fromRaw18, parsePressure, createQueueModel, parseTrading, oiSummary, parseTrades, fillsSummary,
    tradingParams, diffParams, createCapWatch, summaryParams,
    pressureFromPaperwatch, tradingFromPaperwatch, money, waitText, plural, blocksText, intentsText, roomTracker,
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.PTQ = api;
})(typeof globalThis !== 'undefined' ? globalThis : this);
