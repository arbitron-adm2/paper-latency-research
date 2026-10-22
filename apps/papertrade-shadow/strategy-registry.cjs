'use strict';
const crypto=require('node:crypto');
const {parameters}=require('../strategy-lab/profiles.cjs');
const TEMPLATES=Object.freeze([
 {id:'btc-main',asset:'BTC',family:'main',label:'Основной BTC'},
 {id:'eth-native',asset:'ETH',family:'native',label:'Исходный ETH'},
 {id:'btc-y2',asset:'BTC',family:'y2',label:'Вчера 2'},
]);
const MAX_MODELS=16;
function validateDefinition(input){
 if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).some(k=>!['requestId','template','label','timing','signals','parameters','execution'].includes(k)))throw new Error('INVALID_STRATEGY_FIELDS');
 const template=TEMPLATES.find(t=>t.id===input.template);
 if(!template||!['observe','strict'].includes(input.timing)||!['both','spread','book'].includes(input.signals))throw new Error('INVALID_STRATEGY_TEMPLATE');
 if(typeof input.label!=='string'||!input.label.trim()||input.label.trim().length>64||/[\x00-\x1f\x7f]/.test(input.label))throw new Error('INVALID_STRATEGY_LABEL');
 if(typeof input.requestId!=='string'||!/^[-a-zA-Z0-9_]{16,80}$/.test(input.requestId))throw new Error('INVALID_STRATEGY_REQUEST_ID');
 const id='custom_'+crypto.createHash('sha256').update(input.requestId).digest('hex').slice(0,16);
 if(input.execution!==undefined&&(!input.execution||typeof input.execution!=='object'||Array.isArray(input.execution)||Object.keys(input.execution).length!==1||!['vlad','live'].includes(input.execution.preset)))throw new Error('INVALID_EXECUTION_PRESET');
 return{id,asset:template.asset,family:template.family,template:template.id,label:input.label.trim(),timing:input.timing,signals:input.signals,custom:true,...(input.parameters?{parameters:parameters(input.parameters)}:{}),...(input.execution?{execution:{preset:input.execution.preset}}:{})};
}
function catalog(){return{mode:'SIM',templates:TEMPLATES,maxModels:MAX_MODELS,initialCapitalUsdc:'2300',marginUsdc:'2100',leverage:600,entryTtlMs:5000,sourceRules:'Reviewed template defaults; only selected signal families and timing policy change',externalSubscriptions:0};}
module.exports={TEMPLATES,MAX_MODELS,validateDefinition,catalog};
