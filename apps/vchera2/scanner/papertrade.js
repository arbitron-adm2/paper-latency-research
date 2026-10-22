// ============================================================
// Клиент papertrade.xyz: ключ бота, заявки, очередь, потоки событий.
//
// Схема восстановлена из кода сайта и проверена на настоящих заявках (2026-10-12):
// все 11 подписей из HAR восстанавливаются к одному ключу сессии, а идентификатор
// заявки, посчитанный здесь, совпал с тем, что вернул сервер.
//
// Как устроен доступ:
//   • основной кошелёк один раз подписывает RegisterSessionKey для ключа бота;
//   • дальше ключ бота подписывает только OpenPosition / Close / CancelIntent.
//     Политика сайта прямо запрещает ключу сессии вывод, стейкинг, клейм и прочее —
//     даже утёкший ключ бота не может вывести деньги.
// ============================================================

const fs = require('fs');
const path = require('path');
const { ethers } = require('ethers');
const WS = globalThis.WebSocket || require('ws');
const { WalletView } = require('../execution-state.cjs');
const { createCloseSender } = require('../close-request.cjs');
const { createEntrySender, EntryIntentLedger } = require('../entry-deadline.cjs');
const { privateRuntime, atomicPrivateJSON } = require('../runtime.cjs');

const BASE = 'https://exchange.papertrade.xyz';
const STREAM = 'wss://stream.papertrade.xyz/connection/websocket';
const CHAIN_ID = 999;                                            // HyperEVM
const EXCHANGE = '0x6cd5661646289fb6e65ea5c032310fded797d0a2';  // контракт биржи
const ZERO = '0x0000000000000000000000000000000000000000';
const BUILD = '2ef06a20538e';                                     // заголовок x-papertrade-build с сайта

// Домены и типы подписи — дословно из кода сайта (typed-data / signer)
const DOMAIN = { name: 'String', version: '1', chainId: CHAIN_ID, verifyingContract: EXCHANGE };
const CANCEL_DOMAIN = { name: 'PaperTrade Intent Cancel', version: '2', chainId: CHAIN_ID, verifyingContract: EXCHANGE };
const TYPES = {
  RegisterSessionKey: [{ name: 'user', type: 'address' }, { name: 'key', type: 'address' }, { name: 'expiry', type: 'uint64' },
    { name: 'feeAddress', type: 'address' }, { name: 'nonce', type: 'uint256' }, { name: 'deadline', type: 'uint64' }],
  OpenPosition: [{ name: 'user', type: 'address' }, { name: 'instrumentId', type: 'uint32' }, { name: 'isLong', type: 'bool' },
    { name: 'size', type: 'uint128' }, { name: 'leverage', type: 'uint32' }, { name: 'useDebt', type: 'bool' },
    { name: 'nonce', type: 'uint256' }, { name: 'deadline', type: 'uint64' }],
  Close: [{ name: 'user', type: 'address' }, { name: 'positionIds', type: 'uint256[]' }, { name: 'nonce', type: 'uint256' },
    { name: 'deadline', type: 'uint64' }],
  CancelIntent: [{ name: 'user', type: 'address' }, { name: 'intentId', type: 'bytes32' }, { name: 'deadline', type: 'uint256' }],
};
const INTENT_PREFIX = ethers.keccak256(ethers.toUtf8Bytes('papertrade:intent-id:v2'));
const KEY_TTL_SEC = 27 * 86400;   // как у сайта: ключи сессии живут ~4 недели
const BTC = 0;
const PRICE_SCALE = { 0: 10, 1: 100 };   // instrumentId → масштаб цен (BTC ×10, ETH ×100), из /state/trading [3]

// ---------- общее для всех аккаунтов: публичный поток сделок и замеры очереди ----------
const listeners = new Set();
const onTrade = (fn) => listeners.add(fn);
const publicEvidenceListeners = new Set();
const onPublicEvidence = fn => { publicEvidenceListeners.add(fn); return () => publicEvidenceListeners.delete(fn); };
let tradesConnected = false, tradesStarted = false;
const qSamples = [];           // { t, kind: 'entry' | 'exit' | 'cancel', ms, intent, pid } — со всех наших кошельков
const qListeners = new Set();
const onQueueSample = (fn) => qListeners.add(fn);
function qPush(s) { qSamples.push(s); if (qSamples.length > 500) qSamples.shift(); for (const fn of qListeners) { try { fn(s); } catch {} } }
const queueSamples = (kind, n = 30) => qSamples.filter((s) => !kind || s.kind === kind).slice(-n);

// 15.10: аккаунт = свой ключ бота + свой основной кошелёк + свой поток состояния. Основной аккаунт (файл papertrade-bot-key.json)
// остаётся тем же для всех модулей; «бот вчера» берёт отдельный (papertrade-bot-key-2.json), чтобы торговать со своего кошелька
// и не трогать позиции основного (Влад: «два разных коша, чтобы показать статистику на продажу»).
function createAccount(keyFileName) {
// ---------- ключ бота ----------
// Лежит в data/ (в git и в архив для сервера не попадает). Это НЕ основной кошелёк:
// ключ годится только для торговли — и только после разрешения основным кошельком.
// Isolated app: only the dedicated session key in the private runtime; never create/overwrite here.
const KEY_FILE = path.join(__dirname, '..', '.runtime', 'session-key.json');
const LOCKED_MASTER = require('../launch-config.json').wallet.toLowerCase();
const entryLedgerFile = path.join(privateRuntime(), 'entry-intents.json');
const entryLedger = new EntryIntentLedger({ wallet: LOCKED_MASTER,
  read: () => JSON.parse(fs.readFileSync(entryLedgerFile, 'utf8')), save: data => atomicPrivateJSON(entryLedgerFile, data) });
let key = null;      // { privateKey, address, master, createdAt }
let signer = null;
function loadKey() {
  try {
    const raw = JSON.parse(fs.readFileSync(KEY_FILE, 'utf8'));
    const w = new ethers.Wallet(raw.private_key.startsWith('0x') ? raw.private_key : '0x' + raw.private_key);
    if (w.address.toLowerCase() !== String(raw.address).toLowerCase() || String(raw.master).toLowerCase() !== LOCKED_MASTER) throw new Error('key mismatch');
    key = { privateKey: w.privateKey, address: w.address.toLowerCase(), master: LOCKED_MASTER, createdAt: raw.created_at_utc };
    signer = w;
  } catch { key = null; signer = null; }
}
function ensureKey(master) {
  if (String(master).toLowerCase() !== LOCKED_MASTER) throw new Error('Wallet is locked to ' + LOCKED_MASTER);
  loadKey();
  if (!key) throw new Error('Session key missing: run session_cli.py prepare/register');
  return key.address;
}
loadKey();

// nonce как у сайта: случайная база, дальше +1 на каждую заявку
let nonceNext = BigInt(ethers.hexlify(ethers.randomBytes(16)));
const takeNonce = () => (nonceNext++).toString();
const deadline = () => String(Math.floor(Date.now() / 1000) + 3600);   // сайт даёт заявке час

// Приведение полей к виду, в котором их подписывает и шлёт сайт
function norm(type, v) {
  if (type.endsWith('[]')) return v.map((x) => norm(type.slice(0, -2), x));
  if (type.startsWith('uint') || type.startsWith('int')) return BigInt(v).toString();
  if (type === 'address' || type.startsWith('bytes')) return String(v).toLowerCase();
  return v;
}
function normMsg(kind, msg) {
  const o = {};
  for (const { name, type } of TYPES[kind]) o[name] = norm(type, msg[name]);
  return o;
}
async function sign(kind, domain, msg) {
  if (!signer) throw new Error('нет ключа бота');
  const m = normMsg(kind, msg);
  const sig = ethers.Signature.from(await signer.signTypedData(domain, { [kind]: TYPES[kind] }, m));
  if (ethers.verifyTypedData(domain, { [kind]: TYPES[kind] }, m, sig.serialized).toLowerCase() !== key.address) throw new Error('Session signer recovery mismatch');
  const digest = ethers.TypedDataEncoder.hash(domain, { [kind]: TYPES[kind] }, m);
  const intentId = ethers.keccak256(ethers.concat([INTENT_PREFIX, digest, key.address]));
  return { signed: { ...m, v: String(sig.v), r: sig.r.toLowerCase(), s: sig.s.toLowerCase() }, intentId };
}

// ---------- HTTP ----------
const HDR = { 'Content-Type': 'application/json', Accept: 'application/json', 'x-papertrade-build': BUILD,
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/130 Safari/537.36' };
// 17.10 ~03:20: запрос на закрытие пачки основного завис без ответа ~55 с — sendClose считал его «отправляется» и не повторял,
// а liveTick ждал его (await) и стоял целиком; Влад закрыл пачку руками. Теперь у запросов к платформе есть потолок времени:
// не ответила за ms — { ok: false, reason: 'timeout' }, и вызывающий повторит (закрытие) или снимет заявку (вход).
async function post(p, body, ms = 0) {
  const t0 = Date.now();
  try {
    const r = await fetch(BASE + p, { method: 'POST', headers: HDR, body: JSON.stringify(body), ...(ms > 0 ? { signal: AbortSignal.timeout(ms) } : {}) });
    const j = await r.json().catch(() => ({}));
    return { ok: r.ok, status: r.status, data: j, ms: Date.now() - t0, uncertain: r.status >= 500 };
  } catch (e) {
    return { ok: false, status: 0, data: { code: 'transport_unknown', reason: 'transport_unknown' }, ms: Date.now() - t0, timeout: e.name === 'TimeoutError', uncertain: true };
  }
}

// ---------- разрешение ключа бота (подписывает основной кошелёк в браузере) ----------
let pendingReg = null;
function registrationTypedData(master) {
  const user = String(master).toLowerCase();
  if (!/^0x[0-9a-f]{40}$/.test(user)) throw new Error('адрес кошелька papertrade вида 0x…');
  const botAddr = ensureKey(user);
  const message = normMsg('RegisterSessionKey', {
    user, key: botAddr, expiry: Math.floor(Date.now() / 1000) + KEY_TTL_SEC, feeAddress: ZERO,
    nonce: takeNonce(), deadline: deadline(),
  });
  pendingReg = message;
  return {
    domain: DOMAIN,
    types: { EIP712Domain: [{ name: 'name', type: 'string' }, { name: 'version', type: 'string' },
      { name: 'chainId', type: 'uint256' }, { name: 'verifyingContract', type: 'address' }], RegisterSessionKey: TYPES.RegisterSessionKey },
    primaryType: 'RegisterSessionKey',
    message,
  };
}
async function submitRegistration(signature) {
  if (!pendingReg) throw new Error('сначала запроси данные для подписи');
  const who = ethers.verifyTypedData(DOMAIN, { RegisterSessionKey: TYPES.RegisterSessionKey }, pendingReg, signature).toLowerCase();
  if (who !== pendingReg.user) throw new Error(`подписал не тот кошелёк: ${who}`);
  const sig = ethers.Signature.from(signature);
  const res = await post('/intents', { action: 'registerSessionKey',
    signed: { ...pendingReg, v: String(sig.v), r: sig.r.toLowerCase(), s: sig.s.toLowerCase() } });
  if (res.ok) pendingReg = null;
  return res;
}

// ---------- заявки ----------
// margin в $; на платформе маржа в единицах 1e18
const open = createEntrySender({ master: LOCKED_MASTER, takeNonce, maxValidityMs: require('../entry-deadline.cjs').configuredValidityMs(),
  sign: message => sign('OpenPosition', DOMAIN, message),
  post: body => post('/intents', body, 15000),
  record: metadata => entryLedger.record(metadata), unresolved: () => entryLedger.unresolved() });
const close = createCloseSender({ master: LOCKED_MASTER, takeNonce, deadline,
  sign: message => sign('Close', DOMAIN, message),
  post: body => post('/intents', body, 6000) });
async function cancel(intentId) {
  const { signed } = await sign('CancelIntent', CANCEL_DOMAIN, { user: key.master, intentId, deadline: deadline() });
  return post(`/intents/${intentId}/cancel`, { user: signed.user, deadline: signed.deadline, v: signed.v, r: signed.r, s: signed.s }, 6000);
}
async function queueRank() {
  const r = await fetch(`${BASE}/queue-rank/accounts/${key.master}`, { headers: HDR, signal: AbortSignal.timeout(5000) });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(`queue HTTP ${r.status}`);
  const map = {};
  for (const e of j.entries || []) map[e.intentId] = Number(e.position);
  return map;
}

// ---------- замер очереди по своему кошельку: любые заявки, бота и ручные (Влад 14.10: «пусть трекает мои
// сделки и подстраивается под меня») ----------
// Платформа пушит в том же потоке дельты: addedOperations (нажатая заявка, createdAtMs), addedPositions
// (исполнение входа, ts + intentId), closedPositions (исполнение закрытия). Разница = реальная очередь.
const pressed = new Map();     // intentId → { t, kind, pids, seen }
function noteOps(ops) {
  for (const op of ops || []) { const id = String(op[0]).toLowerCase(); if (!pressed.has(id)) pressed.set(id, { t: Number(op[7]) || Date.now(), kind: op[1], pids: (op[9] || []).map(String), seen: Date.now() }); }
  if (pressed.size > 300) for (const [k, v] of pressed) if (Date.now() - v.seen > 3600e3) pressed.delete(k);
}
function noteFills(added, closed, openNow) {
  for (const p of added || []) { const id = String(p[9] || '').toLowerCase(); const pr = pressed.get(id); if (!pr) continue; const ms = (Number(p[8]) || Date.now()) - pr.t; if (ms > 0 && ms < 120000) qPush({ t: Date.now(), kind: 'entry', ms, intent: id, pid: String(p[0]) }); pressed.delete(id); }
  for (const c of closed || []) { const pid = String(c[0]); for (const [id, pr] of pressed) { if (pr.kind !== 'closePositions' || !pr.pids.includes(pid)) continue; const ms = Date.now() - pr.t; if (ms > 0 && ms < 120000) qPush({ t: Date.now(), kind: 'exit', ms, intent: id, pid }); pr.pids = pr.pids.filter((x) => x !== pid); if (!pr.pids.length) pressed.delete(id); break; } }
  // полный снимок после переподключения: исполнения, случившиеся в разрыве
  if (openNow) {
    for (const p of openNow) { const id = String(p[9] || '').toLowerCase(); const pr = pressed.get(id); if (!pr || pr.kind !== 'openPosition') continue; const ms = (Number(p[8]) || Date.now()) - pr.t; if (ms > 0 && ms < 120000) qPush({ t: Date.now(), kind: 'entry', ms, intent: id, pid: String(p[0]) }); pressed.delete(id); }
    const ids = new Set(openNow.map((p) => String(p[0])));
    for (const [id, pr] of pressed) { if (pr.kind !== 'closePositions') continue; const gone = pr.pids.filter((pid) => !ids.has(pid)); for (const pid of gone) { const ms = Date.now() - pr.t; if (ms > 0 && ms < 120000) qPush({ t: Date.now(), kind: 'exit', ms, intent: id, pid }); } pr.pids = pr.pids.filter((pid) => ids.has(pid)); if (!pr.pids.length) pressed.delete(id); }
  }
}
// отмена: заявка пропала из списка (removedOperationIds) без исполнения — ждём 3 с, вдруг исполнение придёт следом
const removedOps = new Map();   // intentId → removedAt
function noteRemoved(ids) { for (const id of ids || []) { const k = String(id).toLowerCase(); if (pressed.has(k)) removedOps.set(k, Date.now()); } }
setInterval(() => {
  const now = Date.now();
  for (const [id, at] of removedOps) {
    if (now - at < 3000) continue;
    removedOps.delete(id);
    const pr = pressed.get(id); if (!pr) continue;   // исполнилась — учтена как вход/выход
    pressed.delete(id);
    qPush({ t: now, kind: 'cancel', ms: at - pr.t, intent: id, pid: pr.pids[0] || null, op: pr.kind });
  }
}, 1000);

// ---------- поток состояния кошелька (SSE) ----------
// Баланс, разрешённые ключи, открытые позиции и заявки — обновляются сами.
const view = new WalletView(LOCKED_MASTER);
const wallet = view.wallet;
onTrade(t => {
  if (String(t.wallet || '').toLowerCase() === LOCKED_MASTER && t.kind === 'close' && t.positionId != null && Number.isFinite(t.exit)) {
    view.confirmClosed(t.positionId, { px: t.exit, ts: Number(t.ts), source: 'public-trades' });
  }
});
let sseAbort = null;
let walletMaster = null;
async function walletStream(master) {
  if (walletMaster === master && sseAbort) return;   // уже слушаем этот кошелёк (второй бот — тот же кошелёк)
  walletMaster = master;
  if (sseAbort) { try { sseAbort.abort(); } catch {} }
  const ctrl = new AbortController(); sseAbort = ctrl;
  while (!ctrl.signal.aborted) {
    try {
      // Каждое подключение живёт 10 с: при подключении платформа присылает полный
      // снимок кошелька, а промежуточные изменения позиций доходят не всегда.
      const conn = new AbortController();
      const onAbort = () => conn.abort();
      ctrl.signal.addEventListener('abort', onAbort, { once: true });
      setTimeout(() => conn.abort(), 10000);
      const r = await fetch(`${BASE}/state/user/live?wallet=${master}`, { headers: { Accept: 'text/event-stream', 'User-Agent': HDR['User-Agent'] }, signal: conn.signal });
      if (!r.ok) throw new Error(`wallet HTTP ${r.status}`);
      let initial = true;
      wallet.connected = r.ok;
      const reader = r.body.getReader(); const dec = new TextDecoder(); let buf = '';
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        let i;
        while ((i = buf.indexOf('\n\n')) >= 0) {
          const ev = buf.slice(0, i); buf = buf.slice(i + 2);
          const name = ((ev.split('\n').find((l) => l.startsWith('event:')) || 'event:').slice(6)).trim();
          if (name !== 'wallet') continue;
          try {
            const j = JSON.parse(ev.split('\n').filter((l) => l.startsWith('data:')).map((l) => l.slice(5)).join(''));
            view.apply(j, { initial }); initial = false;
            entryLedger.observePositions(wallet.openPositions);
            entryLedger.resolveExpired({ walletT: wallet.t, positions: wallet.openPositions, operationIds: wallet.operations.map(op => op[0]) });
            noteOps(j.operations);
            noteOps(j.addedOperations);
            noteFills(j.addedPositions, j.closedPositions, Array.isArray(j.openPositions) ? j.openPositions : null);
            noteRemoved(j.removedOperationIds);
          } catch (e) { wallet.err = e.message; }
        }
      }
    } catch (e) { if (!ctrl.signal.aborted && e.name !== 'AbortError') wallet.err = e.message; }
    wallet.connected = false;
    if (!ctrl.signal.aborted) await new Promise((res) => setTimeout(res, 300));
  }
}
function keyStatus() {
  const k = key && wallet.sessionKeys.find((s) => s.address === key.address);
  return { address: key ? key.address : null, master: key ? key.master : null, active: !!(k && k.expiresAt > Date.now() + 3600e3), expiresAt: k ? k.expiresAt : null };
}
function status() {
  return { key: keyStatus(), wallet: { ...wallet }, tradesConnected, chainId: CHAIN_ID, exchange: EXCHANGE,
    entrySafety: { signedDeadlineEnabled: true, maximumSignalTtlMs: require('../entry-deadline.cjs').configuredValidityMs(), reserveSeconds: 1, requiresCapabilityRecheckBeforeLive: true, unknown: entryLedger.unresolved(), error: entryLedger.error } };
}
return {
  ensureKey, registrationTypedData, submitRegistration, open, close, cancel, queueRank,
  walletStream, tradesStream, onTrade, onPublicEvidence, status, keyStatus, wallet,
  confirmClosed: (positionId, evidence) => view.confirmClosed(positionId, evidence),
  onQueueSample, queueSamples, keyFile: keyFileName,
};
}

// ---------- публичный поток сделок (все кошельки) ----------
function tradesStream() {
  if (tradesStarted) return;   // один поток на процесс: BTC- и ETH-боты слушают через listeners
  tradesStarted = true;
  let ws;
  const connect = () => {
    try { ws = new WS(STREAM); } catch { setTimeout(connect, 3000); return; }
    ws.onopen = () => { tradesConnected = true; ws.send('{"connect":{"name":"js"},"id":1}\n{"subscribe":{"channel":"public:trades","flag":1},"id":2}\n{"subscribe":{"channel":"pressure:current","flag":1},"id":3}'); };
    ws.onmessage = (e) => {
      for (const line of String(typeof e.data === 'string' ? e.data : e.data.toString()).split('\n')) {
        if (!line.trim()) continue;
        if (line === '{}') { try { ws.send('{}'); } catch {} continue; }   // пинг сервера — отвечаем
        let m; try { m = JSON.parse(line); } catch { continue; }
        if (m.push && ['public:trades', 'pressure:current'].includes(m.push.channel) && m.push.pub) {
          const event = { channel: m.push.channel, recv_ms: Date.now(), payload: m.push.pub.data };
          for (const fn of publicEvidenceListeners) { try { fn(event); } catch {} }
        }
        if (m.push && m.push.channel === 'public:trades') {
          for (const t of (m.push.pub && m.push.pub.data) || []) for (const fn of listeners) { try { fn(t); } catch {} }
        }
      }
    };
    ws.onerror = () => {};
    ws.onclose = () => { tradesConnected = false; setTimeout(connect, 2000); };
  };
  connect();
}

// основной аккаунт — как раньше, для всех модулей; дополнительные — по имени, один объект на имя
// Isolated app: a single account bound to the locked wallet; original journal keys are never touched.
let single = null;
function getAccount(name) {
  if (!['main', 'btcy2', undefined, null, ''].includes(name)) throw new Error('Other bots forbidden');
  if (!single) single = createAccount('isolated');
  return single;
}
module.exports = { getAccount };
