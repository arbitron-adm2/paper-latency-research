'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
test('public shadow CLI can delegate to exported realtime main without weakening source-lock',()=>{
 const realtime=require('../../apps/papertrade-shadow/realtime.cjs'),cli=require('../../apps/papertrade-shadow/cli.cjs');
 assert.equal(typeof realtime.main,'function');assert.equal(typeof cli.main,'function');assert.equal(typeof cli.checkLockRecords,'function');
});
test('canonical installed shadow pins explicit frozen public source, never silently reuses changed LIVE',()=>{
 const fs=require('node:fs'),path=require('node:path'),source=require('../../apps/papertrade-shadow/strategy.cjs'),cli=require('../../apps/papertrade-shadow/cli.cjs');
 assert.notEqual(source.SOURCE_ROOT,source.ROOT);assert.equal(path.basename(source.SOURCE_ROOT),'reviewed-baseline');
 const locks=JSON.parse(fs.readFileSync(path.join(source.SOURCE_ROOT,'source-lock-reviewed.json'),'utf8'));assert.doesNotThrow(()=>cli.checkLockRecords(locks));
 const altered=structuredClone(locks);altered[0].sha256='0'.repeat(64);assert.throws(()=>cli.checkLockRecords(altered),/Locked strategy changed \(frozen profile\)/);
 const unsafe=structuredClone(locks);unsafe[0].path='apps/vchera2/.runtime/session-key.json';assert.throws(()=>cli.checkLockRecords(unsafe),/Invalid\/duplicate/);
});
