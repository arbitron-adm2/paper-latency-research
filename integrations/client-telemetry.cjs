'use strict';
// Optional preload for the bot's NEXT coordinated start. Never loads a signer or sends orders.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const ACTIONS = new Set(['openPosition', 'closePositions', 'cancel']);
const CODES = new Set(['intent_conflict', 'intent_not_found', 'invalid_signature', 'invalid_deadline', 'trading_paused', 'intake_paused', 'insufficient_balance', 'rate_limited']);
const ERRORS = new Set(['TypeError', 'AbortError', 'TimeoutError', 'Error']);
const LIMIT = 4 * 1024 * 1024;
function intent(value) {
  return typeof value === 'string' && /^(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|0x[0-9a-f]{64})$/i.test(value) ? value : null;
}

function install({ directory = process.env.PAPER_RESEARCH_EVENT_DIR || path.resolve(__dirname, '../../../data/unified/client-events'), fetchImpl = globalThis.fetch, replaceGlobal = true } = {}) {
  // Fail open for the original transport, not for trading decisions: telemetry
  // must never turn a successful fetch into an error or retry an uncertain POST.
  let stream, file, boot;
  const connection = `${process.pid}-${crypto.randomUUID()}`;
  try {
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    try { boot = fs.readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim(); }
    catch { boot = `process-${connection}`; }
    file = path.join(directory, `client-${connection}.jsonl`);
    stream = fs.createWriteStream(file, { flags: 'ax', mode: 0o600 });
  } catch {
    return { fetch: fetchImpl, file: null, flush() {}, close: async () => {} };
  }
  let buffer = '', seq = 0, disabled = false, blocked = false, closed = false, closing;
  const pending = new Set(), cancelReaders = new Set();
  stream.on('error', () => { disabled = true; buffer = ''; });
  stream.on('drain', () => { blocked = false; flush(); });
  function emit(kind, payload, requestId = null) {
    try {
      if (disabled || closed) return;
      const now = Date.now(), mono = process.hrtime.bigint().toString();
      const data = { origin: 'client_transport', kind, event_ms: now, precision: 'local_wall_and_monotonic',
        payload: { pid: process.pid, request_id: requestId, attempt_id: requestId, ...payload } };
      const line = JSON.stringify({ recv_ms: now, recv_mono_ns: mono, _capture: { boot_id: boot, connection, seq: ++seq }, data }) + '\n';
      if (Buffer.byteLength(buffer) + stream.writableLength + Buffer.byteLength(line) > LIMIT) { disabled = true; buffer = ''; return; }
      buffer += line;
    } catch { disabled = true; buffer = ''; }
  }
  function flush() {
    if (!buffer || disabled || blocked || closed) return;
    try { const data = buffer; buffer = ''; blocked = !stream.write(data); }
    catch { disabled = true; buffer = ''; }
  }
  const timer = setInterval(flush, 100); timer.unref();
  const heartbeat = setInterval(() => emit('client_hook_heartbeat', {}), 10000); heartbeat.unref();
  emit('client_hook_started', {}); flush();

  async function observe(response, common, requestId) {
    let reader, timeout, finishCancel;
    let cancelled = false;
    const cancel = () => { cancelled = true; finishCancel?.(null); try { Promise.resolve(reader?.cancel()).catch(() => {}); } catch {} };
    cancelReaders.add(cancel);
    try {
      const clone = response.clone();
      // Do not start an unbounded .json() consumer of a streaming response.
      if (!clone.body || typeof clone.body.getReader !== 'function') return;
      reader = clone.body.getReader();
      const chunks = []; let size = 0;
      const expired = new Promise(resolve => { finishCancel = resolve; timeout = setTimeout(cancel, 5000); timeout.unref(); });
      const consume = async () => {
        while (!cancelled) {
          const { done, value } = await reader.read();
          if (done) break;
          size += value.byteLength;
          if (size > 65536) { cancel(); return null; }
          chunks.push(Buffer.from(value));
        }
        if (cancelled) return null;
        return JSON.parse(Buffer.concat(chunks).toString('utf8'));
      };
      const j = await Promise.race([consume(), expired]);
      if (!j || typeof j !== 'object' || Array.isArray(j) || cancelled || closed) return;
      const accepted = typeof j.acceptedAtMs === 'number' && Number.isFinite(j.acceptedAtMs) && j.acceptedAtMs > 0 ? j.acceptedAtMs : null;
      emit('http_response_body', { ...common, status: common.status,
        intent_id: common.intent_id || intent(j.intentId), accepted_at_ms: accepted,
        code: typeof j.code === 'string' ? (CODES.has(j.code) ? j.code : 'unknown_code') : null }, requestId);
    } catch { /* Optional evidence only; preserve the actual Response. */ }
    finally { clearTimeout(timeout); cancelReaders.delete(cancel); try { reader?.releaseLock(); } catch {} }
  }

  async function wrapped(input, init) {
    let common;
    try {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
      const parsed = new URL(url);
      const method = String(init?.method ?? input?.method ?? 'GET').toUpperCase();
      if (parsed.origin !== 'https://exchange.papertrade.xyz' || !/^\/intents(?:\/[^/]+\/cancel)?$/.test(parsed.pathname) || method !== 'POST') return fetchImpl(input, init);
      let body = {};
      // Never consume or clone the original Request body before its fetch.
      if (typeof init?.body === 'string' && init.body.length <= 65536) { try { body = JSON.parse(init.body); } catch {} }
      const action = parsed.pathname.endsWith('/cancel') ? 'cancel' : ACTIONS.has(body?.action) ? body.action : 'unknown';
      const positionIds = Array.isArray(body?.signed?.positionIds) ? body.signed.positionIds.slice(0, 100).filter(x => (typeof x === 'string' && /^\d{1,30}$/.test(x)) || (Number.isSafeInteger(x) && x >= 0)).map(String) : [];
      common = { action, position_ids: positionIds, intent_id: action === 'cancel' ? intent(parsed.pathname.split('/')[2]) : null };
    } catch { return fetchImpl(input, init); }
    const requestId = crypto.randomUUID();
    emit('http_request_sent', common, requestId);
    let response;
    try { response = await fetchImpl(input, init); }
    catch (e) {
      let type = 'unknown_error';
      try { if (ERRORS.has(e?.name)) type = e.name; } catch {}
      emit('http_transport_unknown', { ...common, error_type: type, uncertain: true }, requestId);
      throw e;
    }
    try {
      const status = Number.isInteger(response.status) && response.status >= 100 && response.status <= 599 ? response.status : null;
      emit('http_response_headers', { ...common, status, ok: response.ok === true }, requestId);
      const task = observe(response, { ...common, status }, requestId);
      pending.add(task); task.finally(() => pending.delete(task));
    } catch { /* Response evidence must not change a completed fetch. */ }
    return response;
  }
  function close() {
    if (closing) return closing;
    closing = (async () => {
      clearInterval(timer); clearInterval(heartbeat);
      let deadline;
      await Promise.race([Promise.allSettled([...pending]), new Promise(resolve => { deadline = setTimeout(resolve, 100); })]);
      clearTimeout(deadline);
      for (const cancel of cancelReaders) cancel();
      // end() may accept the remaining bounded buffer even while backpressured.
      try {
        if (!disabled && buffer) { stream.write(buffer); buffer = ''; }
      } catch { disabled = true; }
      closed = true;
      await new Promise(resolve => { if (stream.destroyed) resolve(); else { stream.once('error', resolve); stream.end(resolve); } });
    })();
    return closing;
  }
  if (replaceGlobal) globalThis.fetch = wrapped;
  return { fetch: wrapped, file, flush, close };
}
if (process.env.PAPER_RESEARCH_PRELOAD === '1') install();
module.exports = { install };
