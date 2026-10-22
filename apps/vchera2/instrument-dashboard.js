'use strict';
// BTC and ETH have independent buffers, scales, positions and histories.
// There is no trading transport in this view: its sole POST saves a disabled instrument selection.
(() => {
  const SYMBOLS = ['BTC', 'ETH'], SOURCES = ['paper', 'hl', 'bf', 'bs'], WINDOW = 120000;
  const NAMES = { paper: 'Paper · сайт', hl: 'Hyperliquid', bf: 'Binance Futures', bs: 'Binance Spot' };
  const COLORS = { paper: '#d4a1ff', hl: '#78dab4', bf: '#edbd75', bs: '#91b9eb' };
  const $ = id => document.getElementById(id), finite = v => typeof v === 'number' && Number.isFinite(v);
  const fmt = (v, d = 2) => finite(v) ? v.toLocaleString('ru-RU', { minimumFractionDigits: d, maximumFractionDigits: d }) : '—';
  const sign = v => finite(v) ? `${v > 0 ? '+' : v < 0 ? '−' : ''}${fmt(Math.abs(v))}` : '—';
  function put(id, value) { const node = typeof id === 'string' ? $(id) : id; if (node && node.textContent !== value) node.textContent = value; }
  function style(id, value) { const node = typeof id === 'string' ? $(id) : id; if (node) node.className = value; }
  const view = Object.fromEntries(SYMBOLS.map(s => [s, { market: null, feeds: new Map(), historySig: '', series: Object.fromEntries(SOURCES.map(v => [v, []])) }]));
  let snapshot = null, receivedPerf = 0, dirty = false, busy = false, messagePinned = false;
  const age = (f, elapsed) => f && finite(f.ageMs) ? f.ageMs + elapsed : null;
  const fresh = (f, elapsed, limit) => f && finite(f.mid) && finite(age(f, elapsed)) && age(f, elapsed) <= limit;
  const duration = ms => finite(ms) ? ms < 1000 ? `${Math.round(ms)} мс` : `${fmt(ms / 1000, 1)} с` : 'нет данных';
  function message(text, bad = false) { put('instrument-message', text); style('instrument-message', `instrument-message${bad ? ' bad' : ''}`); }
  function controls(s) {
    const selection = s && s.instrumentSelection;
    if (!selection) return;
    if (!dirty && !busy) $('instrument-mode').value = selection.mode;
    $('save-instruments').disabled = busy || !!s.LIVE || !!s.entriesEnabled || !s.providerFresh;
    if (!dirty && !busy && !messagePinned) message(selection.error || `${selection.selectedSymbols.join(' + ')} · ${selection.reason}`, !!selection.error);
  }
  function feedTable(symbol, market) {
    const state = view[symbol];
    if (!state.feeds.size) {
      const rows = SOURCES.map(source => {
        const tr = document.createElement('tr'), cells = [];
        for (let i = 0; i < 4; i++) { const td = document.createElement('td'); tr.append(td); cells.push(td); }
        put(cells[0], NAMES[source]);
        state.feeds.set(source, { tr, cells });
        return tr;
      });
      $(`${symbol}-feeds`).replaceChildren(...rows);
    }
    for (const [source, row] of state.feeds) {
      const f = market.feeds[source] || {};
      put(row.cells[1], `${fmt(f.bid, market.priceDigits)} / ${fmt(f.ask, market.priceDigits)}`);
      row.cells[0].title = f.source || NAMES[source];
    }
  }
  function history(symbol, market) {
    const rows = Array.isArray(market.history) ? market.history.filter(r => r.symbol === symbol && r.instrumentId === market.instrumentId) : [];
    const sig = JSON.stringify(rows);
    if (sig === view[symbol].historySig) return;
    view[symbol].historySig = sig;
    if (!rows.length) {
      const tr = document.createElement('tr'), td = document.createElement('td'); td.colSpan = 4; td.className = 'muted';
      td.textContent = `Нет закрытых ${symbol}-сделок этого бота`; tr.append(td); $(`${symbol}-history`).replaceChildren(tr); return;
    }
    $(`${symbol}-history`).replaceChildren(...rows.map(row => {
      const tr = document.createElement('tr');
      const values = [`#${row.position_id}`, row.side === 'L' ? 'long' : 'short', `${fmt(row.entry, market.priceDigits)} → ${fmt(row.exit, market.priceDigits)}`, `${sign(row.real_pnl)} $`];
      values.forEach((value, i) => { const td = document.createElement('td'); td.textContent = value; if (i === 3) td.className = row.real_pnl > 0 ? 'pos' : row.real_pnl < 0 ? 'neg' : ''; tr.append(td); });
      tr.title = row.reason || ''; return tr;
    }));
  }
  function sample(symbol, market, observedAt) {
    const state = view[symbol];
    for (const source of SOURCES) {
      const f = market.feeds[source], points = state.series[source];
      const t = f && (f.receivedAtMs || f.t) || observedAt;
      if (fresh(f, 0, market.maxFeedAgeMs) && finite(t) && (!points.length || t > points[points.length - 1][0])) points.push([t, f.mid]);
      while (points.length && points[0][0] < observedAt - WINDOW) points.shift();
      if (points.length > 1000) points.splice(0, points.length - 1000);
    }
  }
  function render(s) {
    snapshot = s; receivedPerf = performance.now(); controls(s);
    for (const symbol of SYMBOLS) {
      const market = s.markets && s.markets[symbol];
      if (!market || market.symbol !== symbol || !market.feeds) continue;
      view[symbol].market = market;
      const selected = !!(market.execution && market.execution.selected);
      style(`market-${symbol}`, `panel market-card${selected ? ' selected' : ''}`);
      put(`${symbol}-selected`, selected ? 'Выбран' : 'Не выбран'); style(`${symbol}-selected`, `pill ${selected ? 'selected-pill' : 'neutral'}`);
      put(`${symbol}-execution`, market.execution ? market.execution.reason : 'Статус интеграции неизвестен; LIVE не разрешён.');
      put(`${symbol}-parameters`, symbol === 'BTC' ? `LIVE BTC · отдельные SIM-модели выше · маржа LIVE ${fmt(s.config.margin)} × ${s.config.leverage}` : 'ETH shadow использует собственные правила. Торговое исполнение LIVE ETH ещё не подключено.');
      put(`${symbol}-positions`, `Позиций кошелька: ${(market.positions || []).length}`);
      const stats = market.stats || {};
      put(`${symbol}-pnl`, finite(stats.adjustedPnlUsd) ? `Adjusted PnL бота: ${sign(stats.adjustedPnlUsd)} $` : 'PnL бота: нет истории');
      style(`${symbol}-pnl`, finite(stats.adjustedPnlUsd) ? stats.adjustedPnlUsd > 0 ? 'pos' : stats.adjustedPnlUsd < 0 ? 'neg' : '' : 'muted');
      put(`${symbol}-trades`, `Закрытий: ${stats.closedTrades || 0}`);
      feedTable(symbol, market); history(symbol, market); sample(symbol, market, Date.parse(s.observedAt));
    }
    const feed = s.researchFeed, counts = feed && feed.counts || {};
    put('research-quotes', `Quotes: ${counts.quotes || 0}`); put('research-depth', `Стаканы: ${counts.depth || 0}`);
    put('research-pressure', `Pressure: ${counts.pressure || 0}`); put('research-trades', `Публичные исполнения: ${counts.trade || 0}`);
    put('research-source', feed ? 'Прямые WS · без ордеров' : 'Research feed ещё не подключён');
    put('research-note', 'Это наблюдаемые входные события, не симулированные сделки. Полнота, точность модели очереди и guard проверяются отдельно; LIVE выключен.');
    const runs = s.researchRuns || {};
    const shadow = runs.shadow || {}, guard = runs.guard || {};
    put('shadow-run-status', shadow.available ? `${shadow.status} · ${shadow.active ? 'идёт сейчас' : shadow.completed ? 'конечный прогон завершён' : 'данные устарели'}` : 'Shadow: приёмка / запуск ещё не подтверждены');
    put('shadow-run-detail', shadow.available ? `Прогрев ${shadow.warmupComplete ? 'пройден' : 'не подтверждён'} · seq ${shadow.lastSequence ?? '—'} · Y2 BTC · дополнительные BTC/ETH SIM — ниже · исполнение INCONCLUSIVE${shadow.failure ? ' · ' + shadow.failure : ''}` : 'Нужен принятый incremental run после прогрева. Чужие/бумажные результаты не записываются в реальные сделки.');
    put('guard-run-status', guard.available ? `${guard.status} · timing ${guard.timingStatus} · ${guard.active ? 'активен' : 'не активен'}` : 'Guard: приёмка / запуск ещё не подтверждены');
    put('guard-run-detail', guard.available ? `Эвристика вход ${duration(guard.entryEstimateMs)}, выход ${duration(guard.exitEstimateMs)} · ${(guard.reasons || []).join(', ') || 'timing pass, не разрешение торговли'} · personal rank UNKNOWN` : 'Нужен принятый pressure runner. Совпадение timestamps не доказывает одинаковую queueRevision.');
    tick(Date.parse(s.observedAt));
  }
  function tick(now = Date.now()) {
    const elapsed = snapshot ? Math.max(0, performance.now() - receivedPerf) : Infinity;
    if (snapshot && elapsed > 3000) { put('shadow-run-status', 'Shadow: нет свежего снимка, текущий статус неизвестен'); put('guard-run-status', 'Guard: нет свежего снимка, текущий статус неизвестен'); }
    if ($('save-instruments')) $('save-instruments').disabled = busy || !snapshot || elapsed > 3000 || !!snapshot.LIVE || !!snapshot.entriesEnabled || !snapshot.providerFresh;
    for (const symbol of SYMBOLS) {
      const market = view[symbol].market;
      if (!market) continue;
      const feeds = market.feeds, limit = market.maxFeedAgeMs || 1500;
      const ready = fresh(feeds.hl, elapsed, limit) && fresh(feeds.bf, elapsed, limit);
      put(`${symbol}-fresh`, ready ? 'Потоки свежие' : 'Данные устарели'); style(`${symbol}-fresh`, `pill ${ready ? 'good' : 'warn'}`);
      for (const source of SOURCES) {
        const f = feeds[source] || {}, row = view[symbol].feeds.get(source), ok = fresh(f, elapsed, limit);
        put(`${symbol}-${source}`, finite(f.mid) ? `$${fmt(f.mid, market.priceDigits)}` : '—');
        style(`${symbol}-${source}`, ok ? '' : 'quote-stale');
        if (row) {
          put(row.cells[2], duration(age(f, elapsed))); style(row.tr, ok ? '' : 'stale-row');
          const bookAge = finite(f.bookAgeMs) ? f.bookAgeMs + elapsed : null;
          const bookFresh = finite(bookAge) && bookAge <= (source === 'hl' ? 10000 : 3000);
          put(row.cells[3], f.bookNA ? 'N/A · не отдельный стакан' : `${duration(bookAge)} / ${bookFresh && finite(f.imb) ? `${sign(f.imb * 100)}%` : '—'}`);
        }
      }
      const spread = (a, b) => fresh(feeds[a], elapsed, limit) && fresh(feeds[b], elapsed, limit) ? feeds[a].mid - feeds[b].mid : null;
      const paper = spread('paper', 'bf'), hl = spread('hl', 'bf');
      put(`${symbol}-paper-spread`, paper === null ? 'Paper − Futures: нет свежей пары' : `Paper − Futures: ${sign(paper)} $ · ${sign(paper / feeds.bf.mid * 10000)} bps`);
      put(`${symbol}-hl-spread`, hl === null ? 'HL − Futures: нет свежей пары' : `HL − Futures: ${sign(hl)} $`);
      draw(symbol, now);
    }
  }
  function draw(symbol, now) {
    const canvas = $(`${symbol}-chart`);
    if (!canvas || !canvas.getContext || !canvas.clientWidth) return;
    const dpr = window.devicePixelRatio || 1, w = canvas.clientWidth, h = canvas.clientHeight || 150;
    if (canvas.width !== Math.round(w * dpr) || canvas.height !== Math.round(h * dpr)) { canvas.width = Math.round(w * dpr); canvas.height = Math.round(h * dpr); }
    const ctx = canvas.getContext('2d'); if (!ctx) return;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0); ctx.clearRect(0, 0, w, h);
    const series = view[symbol].series, from = now - WINDOW, l = 64, r = 8, top = 12, bottom = 20;
    let low = Infinity, high = -Infinity;
    for (const points of Object.values(series)) for (const [t, v] of points) if (t >= from && t <= now + 1000) { low = Math.min(low, v); high = Math.max(high, v); }
    ctx.font = '10px ui-sans-serif, system-ui'; ctx.fillStyle = '#8e9eb3';
    if (!finite(low) || !finite(high)) { ctx.fillText('Ожидаем реальные котировки…', l, h / 2); return; }
    const minimum = symbol === 'BTC' ? 2 : .08, span = Math.max(minimum, high - low), center = (high + low) / 2;
    low = center - span * .62; high = center + span * .62;
    const x = t => l + (t - from) / WINDOW * (w - l - r), y = v => top + (high - v) / (high - low) * (h - top - bottom);
    ctx.strokeStyle = '#263143'; ctx.lineWidth = 1;
    for (let i = 0; i < 3; i++) { const v = low + (high - low) * i / 2, yy = y(v); ctx.beginPath(); ctx.moveTo(l, yy); ctx.lineTo(w - r, yy); ctx.stroke(); ctx.fillText(fmt(v, symbol === 'BTC' ? 0 : 2), 2, yy + 3); }
    for (let sec = 0; sec <= 120; sec += 30) ctx.fillText(sec ? `−${sec}с` : 'сейчас', x(now - sec * 1000) - (sec ? 10 : 30), h - 3);
    ctx.save(); ctx.beginPath(); ctx.rect(l, 0, w - l - r, h - bottom); ctx.clip();
    for (const source of ['bs', 'bf', 'hl', 'paper']) {
      const points = series[source].filter(([t]) => t >= from);
      if (!points.length) continue;
      ctx.strokeStyle = COLORS[source]; ctx.fillStyle = COLORS[source]; ctx.lineWidth = source === 'paper' ? 2 : 1.4; ctx.beginPath();
      points.forEach(([t, v], i) => { if (i === 0 || t - points[i - 1][0] > 3000) ctx.moveTo(x(t), y(v)); else ctx.lineTo(x(t), y(v)); }); ctx.stroke();
      const [t, v] = points[points.length - 1]; ctx.beginPath(); ctx.arc(x(t), y(v), 2, 0, Math.PI * 2); ctx.fill();
    }
    ctx.restore();
  }
  async function save() {
    if (busy || !snapshot || snapshot.LIVE || snapshot.entriesEnabled || performance.now() - receivedPerf > 3000) return;
    busy = true; messagePinned = true; $('save-instruments').disabled = true;
    const mode = $('instrument-mode').value;
    message('Сохраняем набор инструментов без включения LIVE…');
    try {
      const response = await fetch('/api/instruments/selection', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Vchera2-Control': '1' }, body: JSON.stringify({ mode }), signal: AbortSignal.timeout(5000) });
      const result = await response.json();
      if (!response.ok || result.ok !== true) throw new Error((result.reasons || []).join(' ') || result.message || 'Выбор не подтверждён');
      dirty = false; message(result.message);
    } catch (error) { message(error.message || 'Выбор не подтверждён. Проверьте подключение.', true); }
    finally { busy = false; }
  }
  $('instrument-mode').addEventListener('change', () => { dirty = true; messagePinned = false; message('Выбор ещё не сохранён. LIVE не включается.'); });
  $('save-instruments').addEventListener('click', save);
  window.InstrumentDashboard = { render, tick };
})();
