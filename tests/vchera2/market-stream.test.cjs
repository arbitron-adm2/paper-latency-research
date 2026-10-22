'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { stream } = require('../../apps/vchera2/market-stream.cjs');
function harness() {
  let clock = 100000, id = 0; const jobs = new Map(), sockets = [], received = [];
  const timers = {
    setTimeout(fn, ms) { jobs.set(++id, { fn, at: clock + ms, ms: 0 }); return id; },
    clearTimeout(id) { jobs.delete(id); },
    setInterval(fn, ms) { jobs.set(++id, { fn, at: clock + ms, ms }); return id; },
    clearInterval(id) { jobs.delete(id); },
  };
  class WS { constructor(url) { this.url = url; sockets.push(this); } close() { this.closed = true; } }
  const handle = stream(['wss://example.test/first', 'wss://example.test/second'], m => received.push(m),
    { now: () => clock, WebSocketClass: WS, timers });
  function advance(ms) {
    const end = clock + ms;
    for (;;) {
      const pending = [...jobs].filter(([, j]) => j.at <= end).sort((a, b) => a[1].at - b[1].at)[0];
      if (!pending) break;
      const [i, job] = pending; clock = job.at;
      if (job.ms) job.at += job.ms; else jobs.delete(i); job.fn();
    }
    clock = end;
  }
  const send = (socket, lag = 0) => socket.onmessage?.({ data: JSON.stringify({ data: { E: clock - lag, b: '10', a: '11' } }) });
  return { handle, sockets, received, jobs, advance, send };
}
test('busy stale stream is rejected and reconnected without waiting for socket silence', () => {
  const h = harness(), old = h.sockets[0]; old.onopen();
  const zombie = old.onmessage; h.send(old, 40000); h.advance(1000);
  assert.equal(h.received.length, 0); assert.equal(old.closed, true);
  assert.equal(h.handle.status().reason, 'source-backlog');
  h.advance(500); assert.equal(h.sockets.length, 2); assert.match(h.sockets[1].url, /second/);
  zombie({ data: JSON.stringify({ E: 101500 }) }); assert.equal(h.received.length, 0);
  h.sockets[1].onopen(); h.send(h.sockets[1], 200); assert.equal(h.received.length, 1);
  h.handle.close(); assert.equal(h.jobs.size, 0);
});
test('silence reconnect has one pending retry even when close/error race', () => {
  const h = harness(), old = h.sockets[0]; const onClose = old.onclose, onError = old.onerror;
  h.advance(10500); onClose(); onError(); h.advance(500);
  assert.equal(h.sockets.length, 2); assert.equal(h.handle.status().reconnects, 1);
  h.handle.close(); h.advance(20000); assert.equal(h.sockets.length, 2);
});
test('current event clears a transient stale streak; unknown timestamp is not invented', () => {
  const h = harness(), s = h.sockets[0]; h.send(s, 3000); h.advance(500); h.send(s, 100);
  h.advance(1000); assert.equal(h.sockets.length, 1); assert.equal(h.received.length, 1);
  s.onmessage({ data: '{"b":"10","a":"11"}' }); assert.equal(h.received.length, 2);
  h.handle.close();
});
