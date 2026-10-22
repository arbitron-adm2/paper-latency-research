'use strict';
// One-shot local journal repair, never signs or submits a trade. Run only with bot stopped.
const fs=require('node:fs'), path=require('node:path');
const {execFileSync}=require('node:child_process');
const {DatabaseSync}=require('node:sqlite');
const {ExecutionJournal}=require('./execution-journal.cjs');
const {parseCloseTrade,E18}=require('./execution-state.cjs');
const {privateRuntime}=require('./runtime.cjs');
const config=require('./launch-config.json');
async function snapshot() {
  const r=await fetch(`https://exchange.papertrade.xyz/state/user/live?wallet=${config.wallet}`,{headers:{Accept:'text/event-stream'},signal:AbortSignal.timeout(15000)});
  if(!r.ok)throw new Error('Wallet read failed');
  const reader=r.body.getReader(),dec=new TextDecoder();let buf='';
  try { for(;;){const {value,done}=await reader.read();if(done)break;buf+=dec.decode(value,{stream:true});buf=buf.replace(/\r\n/g,'\n');let i;
    while((i=buf.indexOf('\n\n'))>=0){const frame=buf.slice(0,i);buf=buf.slice(i+2);if(!/^event:\s*wallet$/m.test(frame))continue;
      const j=JSON.parse(frame.split('\n').filter(l=>l.startsWith('data:')).map(l=>l.slice(5)).join('\n'));
      if(j.address.toLowerCase()!==config.wallet || !Array.isArray(j.openPositions)||!Array.isArray(j.operations))throw new Error('Wrong or incomplete wallet');return j;
    }
  }}finally{await reader.cancel().catch(()=>{});}throw new Error('No wallet snapshot');
}
async function main(){
  const apply=process.argv[2]==='--apply';
  if(process.argv.length>3 || (process.argv[2] && !apply))throw new Error('Usage: node repair-journal.cjs [--apply]');
  const active=execFileSync('systemctl',['--user','show','paper-vchera2-live-20261020.service','--property=ActiveState','--value'],{encoding:'utf8'}).trim();
  if(active!=='inactive' && active!=='failed')throw new Error('Stop bot before journal repair');
  const w=await snapshot();if(w.openPositions.length || w.operations.length)throw new Error('Wallet is not flat; manual reconciliation required');
  const r=await fetch(`https://exchange.papertrade.xyz/query/accounts/${config.wallet}/trades`,{signal:AbortSignal.timeout(10000)});
  if(!r.ok)throw new Error('History read failed');const history=await r.json();
  const confirmed=new Map();for(const x of history.items){const ev=parseCloseTrade(x);if(ev)confirmed.set(String(x.positionId),ev);}
  const root=privateRuntime(),db=new DatabaseSync(path.join(root,'vchera2.sqlite'),{readOnly:!apply});
  const groups=db.prepare("SELECT position_id,MIN(id) canonicalId,COUNT(*) n FROM btcy2_lag_trades WHERE live=1 AND position_id IS NOT NULL AND status!='duplicate' GROUP BY position_id HAVING COUNT(*)>1").all();
  const logs=execFileSync('journalctl',['--user','-u','paper-vchera2-live-20261020.service','--no-pager','-o','cat','--grep=ВЫХОД #'],{encoding:'utf8'});
  const originals=new Map();for(const line of logs.split('\n')){const m=line.match(/ВЫХОД #(\d+) @[^ ]+ \((.*)\) ход /);if(m&&!originals.has(Number(m[1])))originals.set(Number(m[1]),m[2]);}
  const reasons={};for(const g of groups){if(!confirmed.has(g.position_id))throw new Error('No close evidence for '+g.position_id);if(originals.has(g.canonicalId))reasons[g.position_id]=originals.get(g.canonicalId);}
  let repaired=[],backup=null;
  if(apply){
    backup='vchera2-before-repair-'+Date.now()+'.sqlite';db.prepare('VACUUM INTO ?').run(path.join(root,backup));
    const journal=new ExecutionJournal(db);repaired=journal.repairDuplicates(confirmed,reasons);
  }
  const rows=db.prepare("SELECT id,position_id,status,reason,entry_info,exit_info,closed_at,real_pnl FROM btcy2_lag_trades WHERE live=1 ORDER BY id").all();
  const output={checked_at_utc:new Date().toISOString(),apply,available_usdc:E18(w.funds.availableBalance),available_raw:w.funds.availableBalance,open_positions:w.openPositions.length,operations:w.operations.length,duplicate_groups:groups,repaired,reason_source:'original canonical row exit in journald (not duplicate exit)',private_database_backup:backup,rows,financial_requests_sent:0};
  const reports=path.resolve(__dirname,'../../reports/vchera2-repair-20261020');fs.mkdirSync(reports,{recursive:true});fs.writeFileSync(path.join(reports,apply?'repair.json':'preflight.json'),JSON.stringify(output,null,2));
  db.close();console.log(JSON.stringify(output,null,2));
}
main().catch(e=>{console.error(e.message);process.exitCode=1;});
