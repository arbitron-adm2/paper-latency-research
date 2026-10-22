'use strict';
// Offline check only: compare every own-data second against Python's frozen rule.
const fs = require('node:fs'), readline = require('node:readline');
const { Candidate, exitReason, PROFILE_HASH } = require('../apps/vchera2/btc-long-candidate.cjs');
async function run() {
  const [file, output, cashFile] = process.argv.slice(2), c = new Candidate();
  const mismatches = [], signals = [], rows = [];
  for await (const line of readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity })) {
    const r = JSON.parse(line), actual = c.step(r); rows.push(r);
    if (actual.signal) signals.push(r.t);
    if (actual.signal !== r.expectedSignal && mismatches.length < 20) mismatches.push({ t: r.t, expected: r.expectedSignal, actual: actual.signal, actualFeatures: actual });
  }
  const exitChecks = [];
  if (cashFile) for (const trade of JSON.parse(fs.readFileSync(cashFile, 'utf8'))) {
    const p = { entry: trade.entry_raw / 10, opened_at: trade.entry_ms };
    const from = rows.findIndex(r => r.t === trade.entry_ms);
    let hit = null;
    for (let i = from + 1; i < rows.length && rows[i].t <= trade.entry_ms + 120000; i++) {
      const reason = exitReason(p, rows[i]);
      if (reason) { hit = { t: rows[i].t, reason }; break; }
    }
    const actualReason = hit?.reason.includes('стоп') ? 'sl' : hit?.reason.includes('тейк') ? 'tp' : hit?.reason.includes('120') ? 'timeout' : 'unknown';
    const expected = trade.exit_ms - 9000;
    const ok = hit?.t === expected && actualReason === trade.reason;
    exitChecks.push({ signal: trade.signal_ms, expectedDecisionMs: expected, actualDecisionMs: hit?.t, expectedReason: trade.reason, actualReason, ok });
    if (!ok) mismatches.push(exitChecks.at(-1));
  }
  const result = { status: mismatches.length ? 'FAIL' : 'PASS', profileHash: PROFILE_HASH, seconds: rows.length,
    expectedSignalSeconds: rows.filter(r => r.expectedSignal).length, actualSignalSeconds: signals.length,
    signalTimes: signals, mismatches, exitChecks, orders: 0 };
  fs.writeFileSync(output, JSON.stringify(result, null, 2)+'\n');
  if (mismatches.length) process.exitCode = 1;
  console.log(JSON.stringify({ status: result.status, seconds: result.seconds, expected: result.expectedSignalSeconds, actual: signals.length }));
}
run().catch(e => { console.error(e.message); process.exitCode = 1; });
