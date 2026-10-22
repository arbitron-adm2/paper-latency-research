// Исполняется в мире страницы (MAIN) на exchange.papertrade.xyz, document_start. Только читает.
// График Paper рисуется на одном <canvas>, шкалы в DOM нет. Оборачиваем fillText и из подписей осей
// (цены справа, время снизу) восстанавливаем линейные шкалы: y = y0 + yk·цена и x = x0 + xk·время.
// Результат отдаётся окну расширения событием 'ptq-axes' (строка JSON); сам график не меняется.
(function (root) {
  'use strict';

  const RES_PX = 2.5;   // допустимое отклонение подписи от прямой, px

  // ---------- чистая математика (тестируется в Node) ----------
  const parsePrice = (s) => {
    const t = String(s).replace(/[$\s  ,]/g, '');
    return /^-?\d+(\.\d+)?$/.test(t) ? Number(t) : null;
  };
  // Время подписи оси: HH:MM[:SS] локальное → мс, ближайшие к now (±12 ч).
  function parseClock(s, now) {
    const m = /^(\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(String(s).trim());
    if (!m || +m[1] > 23 || +m[2] > 59 || (m[3] && +m[3] > 59)) return null;
    const d = new Date(now);
    let t = new Date(d.getFullYear(), d.getMonth(), d.getDate(), +m[1], +m[2], +(m[3] || 0)).getTime();
    if (t - now > 43200000) t -= 86400000; else if (now - t > 43200000) t += 86400000;
    return t;
  }
  // МНК по точкам [u, v]: v = a + b·u. null, если точек < 2 или разброс u нулевой.
  function line(pts) {
    const n = pts.length;
    if (n < 2) return null;
    let su = 0, sv = 0;
    for (const [u, v] of pts) { su += u; sv += v; }
    const mu = su / n, mv = sv / n;
    let suu = 0, suv = 0;
    for (const [u, v] of pts) { suu += (u - mu) ** 2; suv += (u - mu) * (v - mv); }
    if (!(suu > 0)) return null;
    const b = suv / suu;
    return { a: mv - b * mu, b };
  }
  // Подгонка; если не укладывается в допуск — пробуем выкинуть одну подпись (худший остаток при её отсутствии).
  const worstOf = (f, pts) => Math.max(...pts.map((p) => Math.abs(p[1] - (f.a + f.b * p[0]))));
  function fit(pts) {
    const f = line(pts);
    if (!f) return null;
    const w = worstOf(f, pts);
    if (w <= RES_PX) return f;
    if (pts.length <= 3) return null;
    let best = null;
    for (let i = 0; i < pts.length; i++) {
      const rest = pts.filter((_, j) => j !== i), g = line(rest);
      if (!g) continue;
      const gw = worstOf(g, rest);
      if (gw <= RES_PX && (!best || gw < best.w)) best = { g, w: gw };
    }
    return best ? best.g : null;
  }
  // labels: [{ s, x, y, al, bl }] в CSS px canvas. Возвращает шкалы или null.
  function axes(labels, now) {
    const ys = [], xs = [];
    let axisX = null;
    for (const l of labels) {
      if (l.al === 'left' && l.bl === 'middle') {
        const p = parsePrice(l.s);
        if (p !== null) ys.push({ p, x: l.x, y: l.y });
      } else if (l.al === 'center' && l.bl === 'alphabetic') {
        const t = parseClock(l.s, now);
        if (t !== null) xs.push([t, l.x]);
      }
    }
    // Подписи цен стоят в одной колонке справа; берём самую частую (±1 px).
    const cols = new Map();
    for (const q of ys) { const k = Math.round(q.x); cols.set(k, (cols.get(k) || 0) + 1); }
    let best = null;
    for (const [k, c] of cols) if (!best || c > best[1]) best = [k, c];
    const yp = best ? ys.filter((q) => Math.abs(q.x - best[0]) <= 1.5) : [];
    // одинаковые цены (дубли подписи) не увеличивают разброс
    const yf = fit(yp.map((q) => [q.p, q.y]));
    const xf = fit(xs);
    if (!yf || !xf || !(xf.b > 0) || !(yf.b < 0)) return null;
    // y = y0 + yk·цена (yk < 0: вверх — выше цена); x = x0 + xk·время_мс; cw — ширина области графика без шкалы
    return { y0: yf.a, yk: yf.b, x0: xf.a, xk: xf.b, cw: best[0] - 5 };
  }

  const api = { parsePrice, parseClock, fit, axes };
  if (typeof module !== 'undefined' && module.exports) { module.exports = api; return; }

  // ---------- перехват в странице ----------
  const proto = root.CanvasRenderingContext2D && root.CanvasRenderingContext2D.prototype;
  if (!proto || proto.fillText.__ptq) return;
  const orig = proto.fillText;
  const seen = new WeakMap();
  const isChart = (c) => {
    const r = seen.get(c), now = performance.now();
    if (r && (r.ok || now - r.at < 1000)) return r.ok;
    const ok = !!(c.parentElement && c.parentElement.classList.contains('trade-chart'));
    seen.set(c, { ok, at: now });
    return ok;
  };
  let buf = [], ctx = null, flushQ = false, lastEmit = 0, pendingEmit = null;

  function emit(json) {
    lastEmit = performance.now();
    try { root.dispatchEvent(new CustomEvent('ptq-axes', { detail: json })); } catch {}
  }
  function flush() {
    flushQ = false;
    const c = ctx, labels = buf;
    buf = []; ctx = null;
    if (!c || !labels.length) return;
    try {
      const cv = c.canvas, rect = cv.getBoundingClientRect();
      const k = cv.width ? rect.width / cv.width : 1;   // px холста → CSS px
      const L = labels.map((l) => ({ s: l.s, al: l.al, bl: l.bl, x: (l.m[0] * l.x + l.m[2]) * k, y: (l.m[1] * l.y + l.m[3]) * k }));
      const r = axes(L, Date.now());
      if (!r) return;
      r.w = rect.width; r.h = rect.height; r.t = Date.now();
      const json = JSON.stringify(r);
      const wait = 33 - (performance.now() - lastEmit);
      if (wait <= 0) emit(json);
      else {
        const first = pendingEmit === null;
        pendingEmit = json;               // самое свежее значение вытесняет отложенное
        if (first) setTimeout(() => { const j = pendingEmit; pendingEmit = null; if (j) emit(j); }, wait);
      }
    } catch {}
  }
  const wrapped = function (text, x, y) {
    try {
      if (isChart(this.canvas)) {
        const m = this.getTransform();
        buf.push({ s: String(text), x, y, al: this.textAlign, bl: this.textBaseline, m: [m.a, m.d, m.e, m.f] });
        ctx = this;
        if (!flushQ) { flushQ = true; queueMicrotask(flush); }
      }
    } catch {}
    return orig.apply(this, arguments);
  };
  wrapped.__ptq = true;
  Object.defineProperty(wrapped, 'name', { value: 'fillText' });
  proto.fillText = wrapped;
})(typeof globalThis !== 'undefined' ? globalThis : this);
