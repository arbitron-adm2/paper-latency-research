#!/usr/bin/env node
'use strict';
// Finite read-only ingestion. It consumes the parent's feed, not upstream WS.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { FeedGuard, fetchBatch, atomicWrite, ENDPOINT, MODE, NEW_POSITIONLESS_OBSERVATION } = require('../src/feed-guard.cjs');
function parseArgs(argv) {
  const known = new Set(['--run-id', '--status', '--predictions', '--cohorts', '--duration-s', '--smoke-s', '--poll-ms', '--asset', '--notional-usd', '--cohort-id', '--start-at-current']);
  const args = {};
  for (let i = 0; i < argv.length; i += 2) {
    const key = argv[i];
    if (!known.has(key) || args[key] !== undefined || argv[i + 1] === undefined) throw new Error('INVALID_ARGUMENT');
    args[key] = argv[i + 1];
  }
  if (!args['--status'] || !args['--run-id']) throw new Error('STATUS_AND_RUN_ID_REQUIRED');
  if ((args['--duration-s'] === undefined) === (args['--smoke-s'] === undefined)) throw new Error('EXACTLY_ONE_FINITE_DURATION_REQUIRED');
  const smoke = args['--smoke-s'] !== undefined, seconds = Number(args[smoke ? '--smoke-s' : '--duration-s']);
  if (!Number.isFinite(seconds) || (smoke ? seconds <= 0 || seconds > 10 : seconds < 300 || seconds > 900)) throw new Error('DURATION_OUT_OF_RANGE');
  const pollMs = Number(args['--poll-ms'] || 500);
  if (!Number.isInteger(pollMs) || pollMs < 100 || pollMs > 5000) throw new Error('POLL_OUT_OF_RANGE');
  const size = args['--notional-usd'] === undefined ? null : Number(args['--notional-usd']);
  if (size !== null && (!Number.isFinite(size) || size <= 0)) throw new Error('INVALID_NOTIONAL');
  if (args['--asset'] && !['BTC', 'ETH'].includes(args['--asset'])) throw new Error('UNSUPPORTED_ASSET');
  if (args['--start-at-current'] !== undefined && args['--start-at-current'] !== 'new-positionless-observational-run') throw new Error('INVALID_START_AT_CURRENT_POLICY');
  return { runId: args['--run-id'], status: path.resolve(args['--status']), predictions: args['--predictions'] ? path.resolve(args['--predictions']) : null,
    cohorts: args['--cohorts'] ? path.resolve(args['--cohorts']) : null, smoke, seconds, pollMs, startAtCurrent: args['--start-at-current'] !== undefined,
    selector: { asset: args['--asset'] || null, notional_usd: size, cohort_id: args['--cohort-id'] || null } };
}
const FETCH_TIMEOUT_MS = 1500, FETCH_START_RESERVE_MS = 100;
function requestWindow(remainingMs) {
  if (!Number.isFinite(remainingMs)) throw new Error('INVALID_REMAINING_WINDOW');
  return remainingMs >= FETCH_TIMEOUT_MS + FETCH_START_RESERVE_MS
    ? { start: true, timeout_ms: FETCH_TIMEOUT_MS, wait_ms: 0 }
    : { start: false, timeout_ms: null, wait_ms: Math.max(0, remainingMs), reason: 'FINITE_DEADLINE_NO_FULL_REQUEST_BUDGET' };
}
function observationStatus(value, { final = false, stopped = false, now = Date.now() } = {}) {
  return { ...value, timing_status: value.status, status: final ? (stopped ? 'STOPPED' : 'COMPLETED') : value.status,
    run_state: final ? (stopped ? 'STOPPED' : 'COMPLETED') : 'RUNNING', active: !final,
    new_simulated_decisions_permitted: !final && value.status === 'ALLOW_SHADOW',
    completed_at_ms: final ? now : null, status_scope: final ? 'COMPLETED_OBSERVATION_NOT_A_LIVE_FEED' : 'ACTIVE_TIMING_FILTER_ONLY' };
}
async function run(args) {
  let empirical = null;
  if (args.cohorts) {
    const stat = fs.statSync(args.cohorts); if (stat.size > 8 * 1024 * 1024) throw new Error('COHORT_FILE_LIMIT');
    empirical = JSON.parse(fs.readFileSync(args.cohorts, 'utf8'));
    if (empirical.schemaVersion !== 1 || empirical.kind !== 'guard_empirical_summary') throw new Error('COHORT_SCHEMA');
  }
  let predictionsFd = null, stored = 0;
  if (args.predictions) { fs.mkdirSync(path.dirname(args.predictions), { recursive: true, mode: 0o700 }); predictionsFd = fs.openSync(args.predictions, 'wx', 0o600); }
  const guard = new FeedGuard({ runId: args.runId, empirical, selector: args.selector, onPrediction: prediction => {
    if (predictionsFd !== null) {
      if (++stored > 20000) throw new Error('PREDICTION_CAP');
      fs.writeSync(predictionsFd, JSON.stringify(prediction) + '\n');
    }
  } });
  const start = Date.now(), end = start + args.seconds * 1000;
  const elapsedStart = process.hrtime.bigint(), durationNs = BigInt(Math.ceil(args.seconds * 1e9));
  let stopped = false, requests = 0, deadlineRequestsSkipped = 0;
  const stop = () => { stopped = true; };
  process.once('SIGTERM', stop); process.once('SIGINT', stop);
  const moduleHashes = Object.fromEntries(['../src/feed-guard.cjs', '../src/latency.js', './headless-guard.cjs'].map(file => [file, crypto.createHash('sha256').update(fs.readFileSync(path.resolve(__dirname, file))).digest('hex')]));
  function publish(final = false) {
    const now = Date.now(), value = observationStatus(guard.status(now), { final, stopped, now });
    value.runner = { finite: true, smoke: args.smoke, started_at_ms: start, planned_end_ms: end,
      elapsed_ms_monotonic: Number(process.hrtime.bigint() - elapsedStart) / 1e6,
      duration_s: args.seconds, network_requests: requests, final, active: !final, stopped_by_signal: stopped, module_sha256: moduleHashes,
      start_mode: args.startAtCurrent ? 'EXPLICIT_NEW_POSITIONLESS_START_AT_CURRENT' : 'STRICT_CURSOR_ZERO',
      fetch_timeout_ms: FETCH_TIMEOUT_MS, fetch_start_reserve_ms: FETCH_START_RESERVE_MS, deadline_requests_skipped: deadlineRequestsSkipped,
      prediction_records_written: stored, wire_capture: false, full_collector: false };
    atomicWrite(args.status, value); return value;
  }
  let result;
  try {
    publish();
    while (!stopped && process.hrtime.bigint() - elapsedStart < durationNs) {
      const left = Number(durationNs - (process.hrtime.bigint() - elapsedStart)) / 1e6;
      const window = requestWindow(left);
      if (!window.start) {
        deadlineRequestsSkipped++;
        if (window.wait_ms > 0 && !stopped) await new Promise(resolve => setTimeout(resolve, window.wait_ms));
        break;
      }
      const beforeCursor = guard.cursor;
      try {
        requests++;
        if (args.startAtCurrent && !guard.bootstrapRecord) {
          const metadata = await fetchBatch(0, 1, undefined, window.timeout_ms);
          guard.bootstrapAtCurrent(metadata, Date.now(), NEW_POSITIONLESS_OBSERVATION);
        } else {
          const batch = await fetchBatch(guard.cursor, 256, undefined, window.timeout_ms);
          guard.ingest(batch, Date.now());
        }
      }
      catch (error) { guard.fetchError(typeof error.message === 'string' && /^[A-Z_]+$/.test(error.message) ? error.message : 'FEED_FETCH_FAILED', Date.now()); }
      publish();
      const remaining = Number(durationNs - (process.hrtime.bigint() - elapsedStart)) / 1e6;
      const catchingUp = guard.cursor > beforeCursor && guard.last !== null && guard.cursor < guard.last;
      if (remaining > 0 && !stopped) await new Promise(resolve => setTimeout(resolve, Math.min(catchingUp ? 1 : args.pollMs, remaining)));
    }
    result = publish(true);
  } finally {
    if (predictionsFd !== null) { fs.fsyncSync(predictionsFd); fs.closeSync(predictionsFd); }
    process.removeListener('SIGTERM', stop); process.removeListener('SIGINT', stop);
  }
  console.log(JSON.stringify({ runId: args.runId, status: result.status, timing_status: result.timing_status, run_state: result.run_state, active: result.active, endpoint: ENDPOINT, mode: MODE, financial_mutations: 0,
    counts: result.counts, predictions: stored, finite_duration_s: args.seconds, output: args.status }));
  return result;
}
if (require.main === module) {
  Promise.resolve().then(() => run(parseArgs(process.argv.slice(2)))).catch(error => { console.error(JSON.stringify({ status: 'FAILED', code: /^[A-Z_]+$/.test(error.message) ? error.message : 'RUNNER_ERROR', financial_mutations: 0 })); process.exitCode = 1; });
}
module.exports = { parseArgs, run, requestWindow, observationStatus, FETCH_TIMEOUT_MS, FETCH_START_RESERVE_MS };
