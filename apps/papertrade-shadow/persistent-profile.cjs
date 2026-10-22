'use strict';
const fs=require('node:fs'),path=require('node:path');
const {ROOT,SOURCE_ROOT,sourceManifest,sha}=require('./strategy.cjs');
const {checkLockRecords}=require('./cli.cjs');
const C=require('./state-codec.cjs');
function freeze(x){if(x&&typeof x==='object'){Object.freeze(x);for(const v of Object.values(x))freeze(v);}return x;}
function buildProfile(){
  const locks=JSON.parse(fs.readFileSync(path.join(SOURCE_ROOT,'source-lock-reviewed.json'),'utf8'));checkLockRecords(locks);
  const launch=JSON.parse(fs.readFileSync(path.join(__dirname,'baseline.json'),'utf8'));
  const reviewedLaunch=JSON.parse(fs.readFileSync(path.join(SOURCE_ROOT,'apps/vchera2/launch-config.json'),'utf8'));
  if(C.stringify(launch)!==C.stringify(reviewedLaunch))throw new Error('BASELINE_CONFIG_NOT_REVIEWED');
  const config=JSON.parse(fs.readFileSync(path.join(__dirname,'scenarios.json'),'utf8'));
  const experiment=config.experiments.find(x=>x.id==='baseline'),scenario=config.scenarios.find(x=>x.id==='observed-sized-latency-hypothesis');
  if(!experiment||experiment.margin!==2100||experiment.leverage!==600||String(experiment.balance)!=='2300'||!scenario)throw new Error('FIXED_PROFILE_DRIFT');
  config.experiments=[experiment];config.scenarios=[scenario]; // no shadow UI size/leverage alternatives
  config.persistentService={enabled:true,acknowledgeSimulationOnly:true,cutoffPolicy:'continuous-observer-only-no-live'};
  config.realtime={pollMs:50,publishMs:1000,limit:256,timeoutMs:2000,maxResponseBytes:2097152,maxReceiveAgeMs:2500,maxServerClockSkewMs:5000};
  config.entryDeadline={signalTtlMs:5000,safetySeconds:1};
  config.executionPricePolicy={id:'causal-hl-bbo-v1',maxAgeMs:1500,staleFallback:false,missingPrice:'outcome_unknown_keep_capital'};
  const runtime=['strategy.cjs','engine.cjs','incremental.cjs','live-feed.cjs','deadline.cjs','money.cjs','telemetry.cjs','cli.cjs','state-codec.cjs','engine-state.cjs','history.cjs','history-schema.sql','raw-retention.cjs','r3-observer.cjs','persistent-profile.cjs','persistent-session.cjs','service.cjs','suite.cjs','strategy-registry.cjs','native-bridge.cjs','native-parameters.json','reviewed-native/manifest.json','reviewed-native/btc-lag.js','reviewed-native/eth-lag.js','reviewed-native/funding-guard.js','reviewed-native/regime.js'];
  runtime.push('decision-details.cjs');
  runtime.push('execution-prices.cjs');
  runtime.push('queue-pools/vlad-preset.json');
  runtime.push('queue-pools/live-preset-20261020.json');
  const implementation=runtime.map(file=>({file,sha256:sha(fs.readFileSync(path.join(__dirname,file)))}));
  for(const file of ['feed-guard.cjs','latency.js'])implementation.push({file:'apps/paper-queue-latency/src/'+file,sha256:sha(fs.readFileSync(path.join(ROOT,'apps/paper-queue-latency/src',file)))});
  implementation.push({file:'apps/strategy-lab/profiles.cjs',sha256:sha(fs.readFileSync(path.join(ROOT,'apps/strategy-lab/profiles.cjs')))});
  const configHash=C.hash(C.stringify({config,experiment,scenario,launch}));
  const profile={schemaVersion:1,id:'persistent-fixed-1260000-ttl5-sim-v3-registry',notionalUsd:1260000,strictlyGreaterThanUsd:1000000,marginUsdc:2100,leverage:600,
    initialVirtualCapitalUsdc:'2300',topUpsAllowed:false,resetPnlAllowed:false,assets:{BTC:'SIM_REVIEWED_STRATEGIES',ETH:'SIM_NATIVE_RULES_LATENCY_UNVALIDATED'},
    sourceRoot:'apps/papertrade-shadow/reviewed-baseline',sourceManifest:sourceManifest(),locks,implementation,configHash,config,experiment,scenario,launch,
    clockPolicy:'Continuous observer-only simulation; explicit replacement of historical absolute cutoff in private VM config. No LIVE permission or schedule.',
    recoveryPolicy:'Lossless checkpoint only when compatible; uncertain downtime outcomes censored, no fresh-price reconstruction.',
    rawPolicy:{samplePerChannelPerSecond:true,contextBeforeMs:2000,contextAfterMs:5000,maxMemoryBytes:2097152,maxChunkBytes:262144,quotaBytes:134217728,retentionMs:1800000},
    storagePolicy:{dbQuotaBytes:4294967296,marketSecondsRetentionDays:7,checkpointEveryMs:5000,fullCheckpointSlots:2,note:'SIM ledgers/unknown evidence never auto-deleted; market 1s rolled up, not tick replay.'},
    modeledAssumptions:{scenario:scenario.label??scenario.id,entryValidityMs:5000,currentLiveSenderParity:false,queueRank:'INCONCLUSIVE',volumePriority:'INCONCLUSIVE',cashPayout:'INCONCLUSIVE',edge:'INCONCLUSIVE'}};
  profile.modelHash=C.hash(C.stringify({id:profile.id,configHash,implementation,sourceManifest:profile.sourceManifest}));return freeze(profile);
}
module.exports={buildProfile};
