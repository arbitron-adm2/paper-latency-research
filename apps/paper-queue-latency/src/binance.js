// Публичные тики Binance spot (@trade) и futures (@aggTrade) по BTC/ETH для наложения на график Paper.
// Только чтение публичных потоков: без ключей, подписей и заявок. Время тика — момент приёма в браузере
// (часы этой машины, как у оси графика Paper), а не биржевое время сделки.
// Работает в фоне расширения (importScripts) и в Node (тесты): globalThis.PTQBinance / module.exports.
(function (root) {
  'use strict';

  const URLS = {
    s: 'wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade',
    // Старый путь /stream у fstream молчит (проверено 21.10.2026); рабочий — /market/stream.
    f: 'wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade',
  };
  const SYMBOLS = { BTCUSDT: 'BTC', ETHUSDT: 'ETH' };
  const KEEP_MS = 180000;     // хранить историю для вновь открытых вкладок
  const KEEP_MAX = 6000;      // и не более стольки тиков на ряд
  const SILENT_MS = 8000;     // нет сообщений дольше — переподключаемся

  // Одно сообщение комбинированного потока → { v: 's'|'f', m: 'BTC'|'ETH', p, q, ev } либо null.
  function parse(venue, raw) {
    let j;
    try { j = typeof raw === 'string' ? JSON.parse(raw) : raw; } catch { return null; }
    const d = j && j.data;
    if (!d || (d.e !== 'trade' && d.e !== 'aggTrade')) return null;
    const m = SYMBOLS[d.s];
    const p = Number(d.p), q = Number(d.q);
    if (!m || !(p > 0) || !Number.isFinite(q)) return null;
    return { v: venue, m, p, q, ev: Number(d.T) || null };
  }

  // Буфер тиков по рядам "v:m": [[t, p], ...] по возрастанию t.
  function createStore() {
    const series = new Map();
    const get = (k) => { let a = series.get(k); if (!a) { a = []; series.set(k, a); } return a; };
    return {
      add(v, m, t, p) {
        const a = get(v + ':' + m);
        a.push([t, p]);
        if (a.length > KEEP_MAX + 500) { // режем пачкой, а не по одному тику
          const cut = t - KEEP_MS;
          let i = a.length - KEEP_MAX;
          while (i < a.length - 1 && a[i][0] < cut) i++;
          a.splice(0, i);
        } else if (a.length > 64 && a[0][0] < t - KEEP_MS) {
          const cut = t - KEEP_MS;
          let i = 0;
          while (i < a.length - 1 && a[i][0] < cut) i++;
          a.splice(0, i);
        }
      },
      // Снимок для новой вкладки: { 's:BTC': [[t,p],…], … }
      dump() { const o = {}; for (const [k, a] of series) o[k] = a.slice(); return o; },
      last(v, m) { const a = series.get(v + ':' + m); return a && a.length ? a[a.length - 1] : null; },
      clear() { series.clear(); },
    };
  }

  // Два сокета (spot и futures). onTick(v, m, t, p); onState(v, state) — 'connecting' | 'open' | 'error' | 'off'.
  function createFeed({ onTick, onState, WS = root.WebSocket, now = Date.now, setT = setTimeout, clearT = clearTimeout } = {}) {
    const socks = {};
    let wanted = false;
    function open(v) {
      const st = socks[v] || (socks[v] = { ws: null, retry: 0, timer: null, watch: null, lastAt: 0 });
      if (st.ws || !wanted) return;
      onState && onState(v, 'connecting');
      let ws;
      try { ws = new WS(URLS[v]); } catch { onState && onState(v, 'error'); later(v); return; }
      st.ws = ws;
      ws.onopen = () => { st.retry = 0; st.lastAt = now(); onState && onState(v, 'open'); };
      ws.onmessage = (ev) => {
        const t = now();
        st.lastAt = t;
        const r = parse(v, ev.data);
        if (r) onTick(v, r.m, t, r.p, r.q);
      };
      ws.onclose = () => { if (st.ws === ws) { st.ws = null; onState && onState(v, wanted ? 'error' : 'off'); later(v); } };
      ws.onerror = () => {};
      // Тишина (молчащий сокет без close) — пересоздаём.
      st.watch = setT(function check() {
        if (st.ws !== ws) return;
        if (now() - st.lastAt > SILENT_MS) { try { ws.close(); } catch {} st.ws = null; later(v); return; }
        st.watch = setT(check, 2000);
      }, 2000);
    }
    function later(v) {
      const st = socks[v];
      if (!st || st.timer || !wanted) return;
      clearT(st.watch);
      st.timer = setT(() => { st.timer = null; open(v); }, Math.min(15000, 500 * 2 ** st.retry++));
    }
    return {
      start() { wanted = true; open('s'); open('f'); },
      stop() {
        wanted = false;
        for (const [v, st] of Object.entries(socks)) {
          clearT(st.timer); clearT(st.watch); st.timer = null; st.watch = null;
          const w = st.ws; st.ws = null;
          if (w) { try { w.close(); } catch {} }
          onState && onState(v, 'off');
        }
      },
    };
  }

  const api = { URLS, parse, createStore, createFeed, KEEP_MS };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.PTQBinance = api;
})(typeof globalThis !== 'undefined' ? globalThis : this);
