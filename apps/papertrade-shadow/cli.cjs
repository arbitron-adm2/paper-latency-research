'use strict';
// No production imports. Only the VM bridge may read the explicit locked source allowlist.
process.env.TZ='Europe/Moscow';
const fs=require('node:fs'),path=require('node:path');
const {spawnSync}=require('node:child_process');
const {simulate,resume,json}=require('./engine.cjs');
const {forensic}=require('./forensic.cjs');
const {ROOT,SOURCE_ROOT,sha,sourceManifest}=require('./strategy.cjs');
const APP=__dirname;
function safeRead(p){
  const r=fs.realpathSync(path.resolve(p));
  if(!['.json','.jsonl'].includes(path.extname(r))||r.includes('/.runtime/')||!['reports','apps/papertrade-shadow','tests/papertrade-shadow'].some(d=>r.startsWith(path.join(ROOT,d)+'/')))throw new Error('Input outside research JSON allowlist');
  return fs.readFileSync(r,'utf8');
}
function options(args){
  const o={};
  for(let i=0;i<args.length;i+=2){if(!args[i]?.startsWith('--')||!args[i+1]||args[i+1].startsWith('--'))throw new Error('Expected --name value');
    const key=args[i].slice(2);if(!['out','config','tape','rows','provider','seconds','dataset','resume','additional','source-lock'].includes(key)||key in o)throw new Error('Unknown/duplicate option '+key);o[key]=args[i+1];}
  return o;
}
function validateConfig(c){
  if(!Number.isInteger(c.seed)||!Array.isArray(c.scenarios)||!Array.isArray(c.experiments)||!c.scenarios.length||!c.experiments.length||c.scenarios.length*c.experiments.length>64)throw new Error('Invalid/bounded research config required');
  for(const group of [c.scenarios,c.experiments]){
    const ids=new Set();
    for(const x of group){if(!/^[a-z][a-z0-9-]{0,70}$/.test(x.id)||ids.has(x.id))throw new Error('Invalid/duplicate research id');ids.add(x.id);}
  }
  for(const x of c.experiments){if(!(Number.isFinite(x.margin)&&x.margin>=10&&x.margin<=10000000&&Number.isInteger(x.leverage)&&x.leverage>=1&&x.leverage<=1000))throw new Error('Invalid research size');}
  for(const s of c.scenarios){
    if(!(s.cancelConflictProbability>=0&&s.cancelConflictProbability<=1))throw new Error('Explicit cancel probability hypothesis required');
    if(!s.ideal)for(const kind of ['entry','exit','cancel'])if(!s[kind+'Ms']?.length||s[kind+'Ms'].some(x=>!Number.isInteger(x)||x<0||x>3600000))throw new Error('Invalid bounded latency pool');
  }
}
function checkLockRecords(locks){
  const expected=new Set(['apps/vchera2/launch-config.json',...sourceManifest().map(x=>x.path)]);
  if(!Array.isArray(locks)||locks.length!==expected.size)throw new Error('Source lock must cover every reviewed dependency');
  for(const x of locks){
    if(!expected.delete(x.path)||!/^[0-9a-f]{64}$/.test(x.sha256))throw new Error('Invalid/duplicate source lock record');
    if(sha(fs.readFileSync(path.join(SOURCE_ROOT,x.path)))!==x.sha256)throw new Error('Locked strategy changed (frozen profile): '+x.path);
  }
}
function lock(p=path.join(APP,'source-lock.json')){
  const locks=JSON.parse(safeRead(p));checkLockRecords(locks);return locks;
}
function output(p){
  if(!p)throw new Error('--out required');
  const out=path.resolve(p),parent=fs.realpathSync(path.dirname(out));
  if(!parent.startsWith(path.join(ROOT,'reports')+path.sep)&&parent!==path.join(ROOT,'reports'))throw new Error('Output must be a new directory in reports');
  fs.mkdirSync(out);return out;
}
function save(out,name,x){fs.writeFileSync(path.join(out,name),typeof x==='string'?x:JSON.stringify(x,null,2)+'\n',{flag:'wx'});}
function saveRun(out,result){
  save(out,'summary.json',result.summary);save(out,'ledger.jsonl',result.ledger.map(x=>json(x)).join('\n')+'\n');
  save(out,'positions.json',result.positions);
  if(result.strategy_rows)save(out,'strategy-rows.json',result.strategy_rows);
  if(result.checkpoint)save(out,'checkpoint.json',result.checkpoint);
  if(result.excluded)save(out,'excluded.json',result.excluded);
}
async function main(args=process.argv.slice(2)){
  const [command,...rest]=args;
  if(command==='shadow'||command==='realtime')return require('./realtime.cjs').main(['run',...rest]);
  if(!['replay','simulate','buffered-smoke'].includes(command))throw new Error('Commands: replay | simulate | shadow/realtime | buffered-smoke (all virtual/read-only)');
  const o=options(rest),locks=lock(o['source-lock']);const out=output(o.out);
  const launch=JSON.parse(safeRead(path.join(APP,'baseline.json')));
  const config=JSON.parse(safeRead(o.config||path.join(APP,'scenarios.json')));
  validateConfig(config);
  save(out,'baseline.json',launch);save(out,'config.json',config);
  save(out,'source-lock.json',locks);
  if(command==='replay'){
    if(!o.rows||!o.provider)throw new Error('replay requires --rows and --provider');
    const result=forensic(JSON.parse(safeRead(o.rows)),JSON.parse(safeRead(o.provider)),config.scenarios[1]);saveRun(out,result);
    console.log(JSON.stringify(result.summary,null,2));return;
  }
  if(o.resume){
    const cp=JSON.parse(safeRead(o.resume)),extra=o.additional?safeRead(o.additional).trim().split('\n').filter(Boolean).map(JSON.parse):[];
    const result=await resume(cp,extra);saveRun(out,result);console.log(JSON.stringify(result.summary,null,2));return;
  }
  let tape=o.tape;
  if(command==='buffered-smoke'){
    if(tape)throw new Error('buffered-smoke reads existing collector, not an arbitrary tape');
    const seconds=Number(o.seconds);
    if(!Number.isInteger(seconds)||seconds<1||seconds>30)throw new Error('buffered-smoke requires --seconds 1..30');
    const dataset=path.resolve(o.dataset||path.join(ROOT,'data/live'));
    if(dataset!==path.join(ROOT,'data/live'))throw new Error('shadow dataset must be the existing data/live collector');
    const result=spawnSync('python',[path.join(APP,'data.py'),'shadow','--out',path.join(out,'capture'),'--dataset',dataset,'--seconds',String(seconds)],{encoding:'utf8',timeout:(seconds+60)*1000,maxBuffer:1024*1024});
    if(result.status!==0)throw new Error('Bounded local capture failed: '+result.stderr);
    tape=path.join(out,'capture/events.jsonl');
  }
  if(!tape)throw new Error('simulate requires --tape');
  const text=safeRead(tape),events=text.trim().split('\n').filter(Boolean).map(JSON.parse);
  save(out,'input-manifest.json',{path:path.relative(ROOT,path.resolve(tape)),sha256:sha(text),events:events.length,seed:config.seed,mode:command});
  const comparison=[];
  for(const scenario of config.scenarios)for(const experiment of config.experiments){
    checkLockRecords(locks);
    const result=await simulate(events,config,launch,experiment,scenario);
    checkLockRecords(locks);
    result.summary.mode=command;
    if(experiment.id!=='baseline'||scenario.id!==config.scenarios[1].id)delete result.checkpoint;
    const dir=path.join(out,scenario.id+'--'+experiment.id);fs.mkdirSync(dir);saveRun(dir,result);comparison.push(result.summary);
  }
  save(out,'comparison.json',comparison);
  const summary={mode:command,status:'INCONCLUSIVE',runs:comparison.length,input_events:events.length,
    objective_model:config.objective_model,financial_mutations_sent:0,secrets_read:0,network_requests:0,
    shadow_ingestion:command==='buffered-smoke'?'bounded buffered diagnostic; NOT realtime strategy acceptance':null,
    baseline:comparison.find(x=>x.experiment.id==='baseline'&&!x.scenario.includes('diagnostic')),
    conclusion:'Scenarios are sensitivity exercises, not inferred queue ranks or validated returns. Missing data never implies profitability.'};
  save(out,'summary.json',summary);console.log(JSON.stringify(summary,null,2));
}
module.exports={main,safeRead,options,checkLockRecords,validateConfig};
if(require.main===module)main().catch(e=>{console.error('SHADOW ERROR:',e.message);process.exitCode=1;});
