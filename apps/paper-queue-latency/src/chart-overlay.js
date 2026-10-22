// Наложение тиков Binance spot/futures на график Paper (вкладка биржи). Только рисует: свой прозрачный
// <canvas> поверх графика (pointer-events: none), события мыши и заявки Paper не затрагиваются.
// Шкалы берёт у chart-hook.js (событие 'ptq-axes'), тики — у панели через globalThis.PTQChart.feed.
(() => {
  'use strict';
  if (window.top !== window || globalThis.PTQChart) return;

  const COLORS = { s: '#2a7fff', f: '#b44ae0' };
  const NAMES = { s: 'Spot', f: 'Fut' };
  const ALIGN_TAU_MS = 20000;     // постоянная времени подстройки уровня (режим «≈»)
  const KEEP = 6000;
  let cfg = { chartSpot: true, chartFut: true, chartAlign: true };
  const series = { s: { BTC: [], ETH: [] }, f: { BTC: [], ETH: [] } };
  let mids = null, cal = null, host = null, cv = null, g = null, legend = null, raf = 0;
  const lvl = {};                 // сдвиг уровня Binance → цена Paper по ключу 'v:M': { off, t }

  const marketNow = () => { const m = /\/trade\/([a-z0-9]+)/i.exec(location.pathname); const c = m && m[1].toUpperCase(); return c === 'ETH' ? 'ETH' : c === 'BTC' ? 'BTC' : null; };
  const on = (v) => (v === 's' ? cfg.chartSpot : cfg.chartFut) !== false;

  // ---------- данные ----------
  function addTick(v, m, t, p) {
    const a = series[v] && series[v][m];
    if (!a) return;
    a.push([t, p]);
    if (a.length > KEEP + 500) a.splice(0, a.length - KEEP);
  }
  function feed(msg) {
    if (!msg) return;
    if (msg.full && msg.series) {
      for (const key of Object.keys(msg.series)) { const [v, m] = key.split(':'); if (series[v] && series[v][m]) series[v][m] = msg.series[key].slice(-KEEP); }
    } else if (Array.isArray(msg.ticks)) for (const [v, m, t, p] of msg.ticks) addTick(v, m, t, p);
    schedule();
  }

  // ---------- разметка ----------
  const CSS = `
:host{all:initial;position:absolute;inset:0;z-index:2;pointer-events:none;display:block;
  --bg:rgba(255,252,244,.86);--tx:#2b2b2b;--sub:#6d6a62;--line:rgba(0,0,0,.16);--warn:#c77d00}
:host([data-dark]){--bg:rgba(22,22,28,.82);--tx:#e6e6ea;--sub:#a0a0aa;--line:rgba(255,255,255,.18);--warn:#e5b53a}
canvas{position:absolute;left:0;top:0;width:100%;height:100%;pointer-events:none}
.lg{position:absolute;left:10px;bottom:30px;display:flex;gap:4px;pointer-events:auto;
  font:11px/1.2 ui-monospace,Menlo,Consolas,monospace;font-variant-numeric:tabular-nums;user-select:none}
.lg button{all:unset;cursor:pointer;display:inline-flex;align-items:center;gap:5px;padding:3px 7px;border-radius:5px;
  background:var(--bg);color:var(--tx);border:1px solid var(--line)}
.lg button[aria-pressed=false]{opacity:.45}
.lg i{width:14px;height:0;border-top:2px solid var(--c);flex:none}
.lg small{color:var(--sub);font-size:10px}
.lg small.old{color:var(--warn)}
.lg button:focus-visible{outline:2px solid var(--c)}
`;
  function ensure() {
    const chart = document.querySelector('.trade-chart');
    const base = chart && chart.querySelector(':scope > canvas');
    if (!base) { if (host) { host.remove(); host = null; } return false; }
    if (host && host.isConnected && host.parentElement === chart) return true;
    if (host) host.remove();
    if (getComputedStyle(chart).position === 'static') chart.style.position = 'relative';
    host = document.createElement('div');
    host.id = 'ptq-chart-host';
    const root = host.attachShadow({ mode: 'open' });
    root.innerHTML = `<style>${CSS}</style><canvas></canvas><div class="lg">${['s', 'f'].map((v) =>
      `<button type="button" data-v="${v}" style="--c:${COLORS[v]}" aria-pressed="true" title="Тики Binance ${v === 's' ? 'spot (trade)' : 'futures (aggTrade)'} на графике. Время — приём в браузере. Разница — к цене Hyperliquid, по которой считает Paper. Клик — показать/скрыть."><i></i><span>${NAMES[v]} —</span><small></small></button>`).join('')}<button type="button" data-a="1" aria-pressed="true" title="≈ — сдвинуть линии Binance на среднюю разницу с ценой Paper (плавно, всю линию целиком), чтобы видеть форму и опережение, а не разницу уровней. Выкл. — настоящие цены; вне шкалы Paper линия не видна, у края стрелка."><span>≈</span></button></div>`;
    cv = root.querySelector('canvas'); g = cv.getContext('2d'); legend = root.querySelector('.lg');
    legend.addEventListener('click', (e) => {
      const b = e.target.closest('button'); if (!b) return;
      const k = b.dataset.a ? 'chartAlign' : b.dataset.v === 's' ? 'chartSpot' : 'chartFut';
      cfg[k] = b.dataset.a ? cfg.chartAlign === false : !on(b.dataset.v); try { chrome.storage.local.set({ [k]: cfg[k] }); } catch {}
      schedule();
    });
    base.after(host);
    return true;
  }

  // ---------- отрисовка ----------
  const fmtP = (p) => p.toLocaleString('en-US', { minimumFractionDigits: p < 1000 ? 2 : 1, maximumFractionDigits: p < 1000 ? 2 : 1 });
  function upperBound(a, t) { let lo = 0, hi = a.length; while (lo < hi) { const m = (lo + hi) >>> 1; if (a[m][0] <= t) lo = m + 1; else hi = m; } return lo; }
  function lowerBound(a, t) { let lo = 0, hi = a.length; while (lo < hi) { const m = (lo + hi) >>> 1; if (a[m][0] < t) lo = m + 1; else hi = m; } return lo; }

  function updateLegend(market, now) {
    const dark = document.documentElement.dataset.theme === 'dark';
    host.toggleAttribute('data-dark', dark);
    const ref = mids && mids[market];
    legend.lastChild.setAttribute('aria-pressed', String(cfg.chartAlign !== false));
    for (const b of legend.children) {
      const v = b.dataset.v;
      if (!v) continue;
      const a = series[v][market] || [], last = a[a.length - 1];
      b.setAttribute('aria-pressed', String(on(v)));
      const name = b.children[1], small = b.children[2];
      const text = last ? `${NAMES[v]} ${fmtP(last[1])}` : `${NAMES[v]} —`;
      if (name.textContent !== text) name.textContent = text;
      let sm = '', old = false;
      if (last) {
        const age = now - last[0];
        if (age > 3000) { sm = `${Math.round(age / 1000)} с назад`; old = true; }
        else if (ref) { const d = last[1] - ref; sm = (d >= 0 ? '+' : '−') + Math.abs(d).toFixed(Math.abs(d) < 10 ? 2 : 1); }
      }
      if (small.textContent !== sm) small.textContent = sm;
      small.className = old ? 'old' : '';
    }
  }

  // Сдвиг уровня: плавно тянем (цена Paper − последняя цена Binance), одинаковый для всей линии.
  function offset(v, market, last, now) {
    const ref = mids && mids[market];
    if (cfg.chartAlign === false || !(ref > 0) || !last) return 0;
    const key = v + ':' + market, target = ref - last[1];
    let o = lvl[key];
    if (!o) o = lvl[key] = { off: target, t: now };
    else { o.off += (target - o.off) * Math.min(1, (now - o.t) / ALIGN_TAU_MS); o.t = now; }
    return o.off;
  }

  function draw() {
    raf = 0;
    if (!ensure()) return;
    const market = marketNow();
    const now = Date.now();
    if (!market) { host.style.display = 'none'; return; }
    host.style.display = '';
    updateLegend(market, now);
    const dpr = Math.min(2, window.devicePixelRatio || 1);
    const W = Math.round(host.clientWidth), H = Math.round(host.clientHeight);
    if (cv.width !== Math.round(W * dpr) || cv.height !== Math.round(H * dpr)) { cv.width = Math.round(W * dpr); cv.height = Math.round(H * dpr); }
    g.setTransform(dpr, 0, 0, dpr, 0, 0);
    g.clearRect(0, 0, W, H);
    // График перерисовывается только когда что-то меняется: пока он стоит, стоит и наложение — рисуем «на момент кадра».
    if (!cal || cal.mk !== market || Math.abs(cal.w - host.clientWidth) > 2 || Math.abs(cal.h - host.clientHeight) > 2) return;
    const tHead = cal.t;
    const plotH = cal.h - 26;   // снизу полоса подписи времени (plotDimensions в самом графике)
    g.save();
    g.beginPath(); g.rect(0, 0, cal.cw, plotH); g.clip();
    const X = (t) => cal.x0 + cal.xk * t;
    const tMin = (0 - cal.x0) / cal.xk;
    const edge = [];
    for (const v of ['s', 'f']) {
      const a = series[v][market];
      if (!on(v) || !a || !a.length) continue;
      const end = upperBound(a, tHead);          // тики после кадра графика пока не рисуем
      if (!end) continue;
      const off = offset(v, market, a[a.length - 1], now);
      const Y = (p) => cal.y0 + cal.yk * (p + off);
      const first = Math.max(0, lowerBound(a, tMin) - 1);
      if (first >= end) continue;
      g.beginPath();
      g.strokeStyle = COLORS[v]; g.lineWidth = 1.4; g.lineJoin = 'round'; g.globalAlpha = 0.95;
      let py = Y(a[first][1]);
      g.moveTo(Math.max(-2, X(a[first][0])), py);
      for (let i = first + 1; i < end; i++) {
        const x = X(a[i][0]);
        g.lineTo(x, py);                    // ступень: цена держится до следующего тика
        py = Y(a[i][1]);
        g.lineTo(x, py);
      }
      const xe = Math.min(cal.cw, X(tHead));
      g.lineTo(xe, py);
      g.stroke();
      if (py >= 0 && py <= plotH) {
        g.globalAlpha = 1; g.fillStyle = COLORS[v]; g.beginPath(); g.arc(xe, py, 3, 0, 6.2832); g.fill();
        g.strokeStyle = 'rgba(255,255,255,.9)'; g.lineWidth = 1; g.stroke();
      } else edge.push([v, py < 0]);
    }
    g.restore();
    // Цена Binance вне видимого диапазона шкалы Paper — стрелка у края вместо обрезанной линии.
    for (const [v, up] of edge) {
      const x = cal.cw - (v === 's' ? 18 : 36), y = up ? 3 : plotH - 3;
      g.fillStyle = COLORS[v];
      g.beginPath();
      if (up) { g.moveTo(x, y); g.lineTo(x - 5, y + 8); g.lineTo(x + 5, y + 8); } else { g.moveTo(x, y); g.lineTo(x - 5, y - 8); g.lineTo(x + 5, y - 8); }
      g.fill();
    }
  }
  function schedule() { if (!raf) raf = requestAnimationFrame(draw); }

  window.addEventListener('ptq-axes', (e) => {
    try { const c = JSON.parse(e.detail); if (c && c.xk > 0 && c.cw > 0) { c.mk = marketNow(); cal = c; schedule(); } } catch {}
  });
  setInterval(() => { if (!document.hidden) schedule(); }, 1000);   // легенда живёт и когда график стоит

  try {
    chrome.storage.local.get(['chartSpot', 'chartFut', 'chartAlign'], (s) => { cfg = { ...cfg, ...(s || {}) }; schedule(); });
    chrome.storage.onChanged.addListener((ch, area) => {
      if (area !== 'local') return;
      for (const k of ['chartSpot', 'chartFut', 'chartAlign']) if (k in ch) cfg[k] = ch[k].newValue;
      schedule();
    });
  } catch {}

  globalThis.PTQChart = { feed, setMids(m) { mids = m; }, wants: () => cfg.chartSpot !== false || cfg.chartFut !== false };
})();
