'use strict';
const { config } = require('./safety.cjs');
const command = process.argv[2] || 'status';
if (!['status', 'health', 'config', 'kill', 'start', 'stop'].includes(command) || process.argv.length > 3) {
  console.error('Usage: node cli.cjs status|health|config|start|stop|kill (stop/kill keep dashboard alive and flatten own positions)');
  process.exit(2);
}
const base = `http://${config.host}:${config.port}`;
const req = ['kill','start','stop'].includes(command)
  ? fetch(base + (command === 'kill' ? '/api/kill' : `/api/trading/${command}`), { method: 'POST', headers: command === 'kill' ? { 'X-Vchera2-Kill': '1' } : { 'X-Vchera2-Control': '1' }, signal: AbortSignal.timeout(30000) })
  : fetch(base + (command === 'health' ? '/health' : command === 'config' ? '/api/btcy2lag/config' : '/state'), { signal: AbortSignal.timeout(5000) });
req.then(async r => { const j = await r.json(); console.log(JSON.stringify(j, null, 2)); if (!r.ok) process.exitCode = 1; })
  .catch(() => { console.error('Process not reachable'); process.exitCode = 1; });
