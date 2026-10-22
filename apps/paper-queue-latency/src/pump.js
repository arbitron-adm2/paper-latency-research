// Только на вкладке биржи (exchange.papertrade.xyz): открывает поток stream.papertrade.xyz отсюда — сервер
// пускает только с Origin биржи — и пересылает сообщения фону расширения. Работает, только когда фон
// попросил (есть открытые окна) и только в одной вкладке биржи из всех. Ничего на бирже не нажимает.
(() => {
  'use strict';
  if (window.top !== window || window.__ptqPump) return;
  window.__ptqPump = true;

  const STREAM = 'wss://stream.papertrade.xyz/connection/websocket';
  const CHANNELS = { 2: 'pressure:current', 3: 'trading:current', 4: 'public:trades' };
  let port = null, ws = null, wanted = false, retry = 0, timer = null, connected = false;

  const send = (m) => { try { port && port.postMessage(m); } catch {} };
  const alive = () => { try { return !!chrome.runtime.id; } catch { return false; } };

  function connectPort() {
    if (!alive()) return; // расширение перезагрузили — эта копия скрипта устарела
    try { port = chrome.runtime.connect({ name: 'ptq-pump' }); } catch { port = null; return; }
    port.onMessage.addListener((m) => {
      if (m && m.type === 'start') { wanted = true; open(); }
      else if (m && m.type === 'stop') { wanted = false; close(); }
    });
    port.onDisconnect.addListener(() => { port = null; wanted = false; close(); setTimeout(connectPort, 2000); });
  }

  function open() {
    if (ws || !wanted) return;
    connected = false;
    send({ type: 'status', state: 'connecting' });
    let sock;
    try { sock = new WebSocket(STREAM); } catch (e) { send({ type: 'status', state: 'error', error: e.message }); schedule(); return; }
    ws = sock;
    sock.onopen = () => sock.send(['{"connect":{"name":"js"},"id":1}', ...Object.entries(CHANNELS).map(([id, ch]) => `{"subscribe":{"channel":"${ch}"},"id":${id}}`)].join('\n'));
    sock.onmessage = (ev) => {
      for (const line of String(ev.data).split('\n')) {
        if (!line.trim()) continue;
        let m; try { m = JSON.parse(line); } catch { continue; }
        if (Object.keys(m).length === 0) { try { sock.send('{}'); } catch {} continue; } // ping → pong
        if (m.id === 1 && m.connect) { connected = true; retry = 0; send({ type: 'status', state: 'open' }); continue; }
        if (m.error) { send({ type: 'status', state: connected ? 'open' : 'error', error: JSON.stringify(m.error) }); continue; }
        if (m.subscribe && Array.isArray(m.subscribe.publications) && CHANNELS[m.id]) for (const p of m.subscribe.publications) send({ type: 'pub', ch: CHANNELS[m.id], data: p.data });
        if (m.push && m.push.pub) send({ type: 'pub', ch: m.push.channel, data: m.push.pub.data });
      }
    };
    sock.onerror = () => {};
    sock.onclose = () => {
      if (ws === sock) ws = null;
      send({ type: 'status', state: wanted ? 'reconnecting' : 'off', error: connected ? null : 'поток не открылся' });
      schedule();
    };
  }
  function schedule() {
    if (timer || !wanted) return;
    timer = setTimeout(() => { timer = null; open(); }, Math.min(15000, 1000 * Math.pow(2, retry++)));
  }
  function close() {
    if (timer) { clearTimeout(timer); timer = null; }
    if (ws) { const s = ws; ws = null; try { s.close(); } catch {} }
  }

  connectPort();
})();
