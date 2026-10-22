'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { install } = require('../../integrations/client-telemetry.cjs');

test('transport evidence preserves response, uncertainty and excludes signed material', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'paper-telemetry-'));
  let calls = 0;
  const response = new Response(JSON.stringify({ intentId: '0xknown', signed: { privateKey: 'never' } }), { status: 202 });
  const source = async () => { calls++; if (calls === 2) throw new TypeError('network failure'); return response; };
  const journal = install({ directory: dir, fetchImpl: source, replaceGlobal: false });
  try {
    const body = JSON.stringify({ action: 'openPosition', signed: { r: 'SECRET', s: 'SECRET', nonce: 'SECRET' } });
    assert.equal(await journal.fetch('https://exchange.papertrade.xyz/intents', { method: 'POST', body }), response);
    await assert.rejects(journal.fetch('https://exchange.papertrade.xyz/intents', { method: 'POST', body }), TypeError);
    await new Promise(resolve => setImmediate(resolve));
    await journal.close();
    const raw = fs.readFileSync(journal.file, 'utf8');
    assert.equal(raw.includes('SECRET'), false);assert.equal(raw.includes('never'), false);
    const rows = raw.trim().split('\n').map(JSON.parse);
    assert.equal(rows.filter(x => x.data.kind === 'http_request_sent').length, 2);
    assert.equal(rows.filter(x => x.data.kind === 'http_transport_unknown').length, 1);
    assert.ok(rows.every(x => /^\d+$/.test(x.recv_mono_ns)));
  } finally { fs.rmSync(dir, { recursive: true }); }
});

const IID = 'c001cafe-1234-4567-890a-123456789abc';
function temp() { return fs.mkdtempSync(path.join(os.tmpdir(), 'paper-telemetry-')); }
function records(journal) { return fs.readFileSync(journal.file, 'utf8').trim().split('\n').map(JSON.parse); }

test('Request POST and URL inputs preserve identity and do not consume request body', async () => {
  const dir = temp(); const calls = [];
  const response = new Response(JSON.stringify({ intentId: IID, acceptedAtMs: 1700000000000 }), { status: 202 });
  const source = async (input, init) => { calls.push([input, init]); assert.equal(input.bodyUsed, false); return response; };
  const journal = install({ directory: dir, fetchImpl: source, replaceGlobal: false });
  const req = new Request('https://exchange.papertrade.xyz/intents', { method: 'POST', body: '{"secret":"NEVER_CAPTURE"}' });
  try {
    assert.equal(await journal.fetch(req), response); assert.equal(req.bodyUsed, false);
    assert.equal(calls[0][0], req); assert.equal(calls[0][1], undefined);
    await journal.close();
    const rows = records(journal).filter(x => x.data.kind.startsWith('http_'));
    assert.equal(rows.length, 3); assert.equal(rows[0].data.payload.action, 'unknown');
    assert.equal(rows[2].data.payload.intent_id, IID);
    assert.equal(rows[2].data.payload.accepted_at_ms, 1700000000000);
    assert.ok(rows.every(x => x.data.payload.attempt_id === x.data.payload.request_id));
    assert.equal(JSON.stringify(rows).includes('NEVER_CAPTURE'), false);
  } finally { await journal.close(); await req.body.cancel(); fs.rmSync(dir, { recursive: true }); }
});

test('init method override is respected; a URL object POST is captured', async () => {
  const dir = temp(); let calls = 0;
  const journal = install({ directory: dir, replaceGlobal: false, fetchImpl: async () => { calls++; return new Response('{}'); } });
  const req = new Request('https://exchange.papertrade.xyz/intents', { method: 'POST', body: '{}' });
  try {
    await journal.fetch(req, { method: 'GET' });
    await journal.fetch(new URL('https://exchange.papertrade.xyz/intents'), { method: 'POST', body: '{}' });
    await journal.close();
    assert.equal(calls, 2);
    assert.equal(records(journal).filter(x => x.data.kind === 'http_request_sent').length, 1);
  } finally { await journal.close(); await req.body.cancel(); fs.rmSync(dir, { recursive: true }); }
});

test('cancel target survives missing response intentId and conflict is not terminal cancellation', async () => {
  const dir = temp(); const response = new Response('{"code":"intent_conflict"}', { status: 409 });
  const journal = install({ directory: dir, replaceGlobal: false, fetchImpl: async () => response });
  try {
    assert.equal(await journal.fetch(`https://exchange.papertrade.xyz/intents/${IID}/cancel`, { method: 'POST', body: '{"signature":"NEVER"}' }), response);
    await journal.close();
    const rows = records(journal).filter(x => x.data.kind.startsWith('http_'));
    assert.equal(rows.length, 3);
    assert.ok(rows.every(x => x.data.payload.intent_id === IID && x.data.payload.action === 'cancel'));
    assert.equal(rows.at(-1).data.payload.code, 'intent_conflict');
    assert.equal(rows.some(x => x.data.kind.includes('cancelled')), false);
    assert.equal(JSON.stringify(rows).includes('NEVER'), false);
  } finally { await journal.close(); fs.rmSync(dir, { recursive: true }); }
});

test('free-form metadata and credentials are not serialized; original rejection is preserved', async () => {
  const dir = temp(); let calls = 0;
  const failure = Object.assign(new Error('NEVER_CAPTURE'), { name: 'NEVER_CAPTURE' });
  const response = new Response('{"intentId":"NEVER_CAPTURE","code":"NEVER_CAPTURE","signed":{"r":"NEVER_CAPTURE"}}');
  const source = async () => { if (++calls === 2) throw failure; return response; };
  const journal = install({ directory: dir, replaceGlobal: false, fetchImpl: source });
  const options = { method: 'POST', headers: { Authorization: 'NEVER_CAPTURE' }, body: JSON.stringify({ action: 'NEVER_CAPTURE', signed: { nonce: 'NEVER_CAPTURE', positionIds: ['NEVER_CAPTURE', '17', {}, 18] } }) };
  try {
    assert.equal(await journal.fetch('https://exchange.papertrade.xyz/intents', options), response);
    await assert.rejects(journal.fetch('https://exchange.papertrade.xyz/intents', options), e => e === failure);
    await journal.close();
    const raw = fs.readFileSync(journal.file, 'utf8'); assert.equal(raw.includes('NEVER_CAPTURE'), false);
    const rows = records(journal);
    assert.deepEqual(rows.find(x => x.data.kind === 'http_request_sent').data.payload.position_ids, ['17', '18']);
    assert.equal(rows.find(x => x.data.kind === 'http_response_body').data.payload.code, 'unknown_code');
    assert.equal(rows.find(x => x.data.kind === 'http_transport_unknown').data.payload.error_type, 'unknown_error');
    assert.equal(calls, 2);
  } finally { await journal.close(); fs.rmSync(dir, { recursive: true }); }
});

test('unwritable telemetry path does not change fetch or install a failing wrapper', async () => {
  const dir = temp(); const file = path.join(dir, 'not-a-directory'); fs.writeFileSync(file, 'x');
  const response = new Response('{}'); let calls = 0;
  const source = async () => { calls++; return response; };
  const journal = install({ directory: file, replaceGlobal: false, fetchImpl: source });
  try {
    assert.equal(await journal.fetch('https://exchange.papertrade.xyz/intents', { method: 'POST' }), response);
    assert.equal(journal.file, null); assert.equal(calls, 1); await journal.close();
  } finally { fs.rmSync(dir, { recursive: true }); }
});

test('clone/parser failures and unavailable response metadata cannot reject a completed fetch', async () => {
  const dir = temp(); const response = { get status() { throw new Error('metadata'); }, clone() { throw new Error('clone'); } };
  const journal = install({ directory: dir, replaceGlobal: false, fetchImpl: async () => response });
  try {
    assert.equal(await journal.fetch('https://exchange.papertrade.xyz/intents', { method: 'POST' }), response);
    await journal.close();
    assert.equal(records(journal).filter(x => x.data.kind === 'http_request_sent').length, 1);
  } finally { await journal.close(); fs.rmSync(dir, { recursive: true }); }
});

test('close is bounded when cloned response never completes', async () => {
  const dir = temp(); let cancelled = false;
  const response = { status: 202, ok: true, clone: () => ({ body: { getReader: () => ({ read: () => new Promise(() => {}), cancel: () => { cancelled = true; return Promise.resolve(); }, releaseLock() {} }) } }) };
  const journal = install({ directory: dir, replaceGlobal: false, fetchImpl: async () => response });
  try {
    await journal.fetch('https://exchange.papertrade.xyz/intents', { method: 'POST' });
    const start = Date.now(); await journal.close(); assert.ok(Date.now() - start < 1500); assert.equal(cancelled, true);
    assert.equal(records(journal).some(x => x.data.kind === 'http_response_body'), false);
  } finally { await journal.close(); fs.rmSync(dir, { recursive: true }); }
});
