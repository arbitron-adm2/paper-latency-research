'use strict';
// Market transport only: old events do not become fresh merely by arriving now.
const transports = new Set();
function sourceTime(message) {
  const d = message && (message.data || message);
  const value = Number(d && (d.T || d.E || d.time));
  return Number.isFinite(value) && value > 0 ? value : null;
}
function stream(urls, onMessage, options = {}) {
  const { onOpen = null, ping = null, maxSourceLagMs = 2000,
    staleForMs = 1000, silenceMs = 10000, now = Date.now,
    WebSocketClass = globalThis.WebSocket, timers = globalThis } = options;
  let socket = null, generation = 0, index = 0, retry = null, heartbeat = null;
  let stopped = false, last = now(), badSince = null, failures = 0;
  const stats = { name: options.name || new URL(urls[0]).pathname, reconnects: 0,
    rejectedStale: 0, received: 0, sourceLagMs: null, reason: null, connected: false };
  const disposeSocket = () => {
    if (heartbeat !== null) timers.clearInterval(heartbeat);
    heartbeat = null;
    const old = socket; socket = null;
    if (old) { old.onopen = old.onmessage = old.onclose = old.onerror = null;
      try { if (old.terminate) old.terminate(); else old.close(); } catch {} }
    stats.connected = false;
  };
  const reconnect = reason => {
    if (stopped || retry !== null) return;
    ++generation; disposeSocket(); stats.reason = reason; stats.reconnects++; index++;
    const delay = Math.min(5000, 500 * 2 ** Math.min(failures++, 4));
    retry = timers.setTimeout(() => { retry = null; connect(); }, delay);
  };
  const connect = () => {
    if (stopped) return;
    const g = ++generation; badSince = null; last = now();
    let s;
    try { s = new WebSocketClass(urls[index % urls.length]); } catch { reconnect('connect-error'); return; }
    socket = s;
    s.onopen = () => {
      if (g !== generation || stopped) return;
      stats.connected = true; last = now();
      try { if (onOpen) onOpen(s); } catch { reconnect('subscribe-error'); return; }
      if (ping) heartbeat = timers.setInterval(() => { try { s.send(ping); } catch { reconnect('ping-error'); } }, 30000);
    };
    s.onmessage = event => {
      if (g !== generation || stopped) return;
      last = now(); stats.received++;
      let m;
      try { m = JSON.parse(typeof event.data === 'string' ? event.data : event.data.toString()); } catch { return; }
      const timestamp = sourceTime(m);
      if (timestamp !== null) {
        stats.sourceLagMs = last - timestamp;
        if (stats.sourceLagMs > maxSourceLagMs || stats.sourceLagMs < -5000) {
          stats.rejectedStale++;
          if (badSince === null) badSince = last;
          if (last - badSince >= staleForMs) reconnect('source-backlog');
          return;
        }
        badSince = null;
      }
      failures = 0;
      try { onMessage(m); } catch {}
    };
    s.onerror = () => { if (g === generation) reconnect('socket-error'); };
    s.onclose = () => { if (g === generation) reconnect('socket-close'); };
  };
  const watchdog = timers.setInterval(() => {
    if (retry !== null || stopped) return;
    if (badSince !== null && now() - badSince >= staleForMs) reconnect('source-backlog');
    else if (now() - last > silenceMs) reconnect('silent');
  }, 500);
  const handle = { status: () => ({ ...stats, receiveAgeMs: Math.max(0, now() - last) }),
    close() { stopped = true; ++generation; timers.clearInterval(watchdog);
      if (retry !== null) timers.clearTimeout(retry); retry = null; disposeSocket(); transports.delete(handle); } };
  transports.add(handle); connect(); return handle;
}
function transportStatus() { return [...transports].map(handle => handle.status()); }
module.exports = { stream, sourceTime, transportStatus };
