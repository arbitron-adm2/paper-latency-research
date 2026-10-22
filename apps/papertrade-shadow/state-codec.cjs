'use strict';
// Lossless research state codec: no eval, functions, prototypes, signatures or secrets.
const crypto=require('node:crypto');
const tag=x=>Object.prototype.toString.call(x);
function encode(value,stack=new Set(),depth=0){
  if(depth>128)throw new Error('CHECKPOINT_DEPTH');
  if(value===null||typeof value==='string'||typeof value==='boolean')return value;
  if(typeof value==='number')return Number.isFinite(value)?value:{t:'number',v:String(value)};
  if(typeof value==='bigint')return {t:'bigint',v:String(value)};
  if(value===undefined)return {t:'undefined'};
  if(typeof value!=='object'||stack.has(value))throw new Error('CHECKPOINT_UNSUPPORTED_OR_CYCLE');
  stack.add(value);let out;
  const e=x=>encode(x,stack,depth+1);
  if(Array.isArray(value))out={t:'array',v:value.map(e)};
  else if(tag(value)==='[object Map]')out={t:'map',v:[...value].map(([k,v])=>[e(k),e(v)])};
  else if(tag(value)==='[object Set]')out={t:'set',v:[...value].map(e)};
  else if(tag(value)==='[object Date]')out={t:'date',v:value.getTime()};
  else out={t:'object',v:Object.keys(value).sort().map(k=>[k,e(value[k])])};
  stack.delete(value);return out;
}
function decode(x,depth=0){
  if(depth>128)throw new Error('CHECKPOINT_DEPTH');
  if(x===null||typeof x==='string'||typeof x==='boolean'||typeof x==='number')return x;
  if(!x||typeof x!=='object'||Array.isArray(x))throw new Error('CHECKPOINT_TAG');
  const d=v=>decode(v,depth+1);
  switch(x.t){
    case 'undefined':return undefined;
    case 'number':if(!['NaN','Infinity','-Infinity'].includes(x.v))throw new Error('CHECKPOINT_NUMBER');return Number(x.v);
    case 'bigint':if(!/^-?\d{1,100}$/.test(x.v))throw new Error('CHECKPOINT_BIGINT');return BigInt(x.v);
    case 'array':return x.v.map(d);
    case 'map':return new Map(x.v.map(([k,v])=>[d(k),d(v)]));
    case 'set':return new Set(x.v.map(d));
    case 'date':return new Date(x.v);
    case 'object':{const o={};for(const [k,v] of x.v){if(['__proto__','prototype','constructor'].includes(k))throw new Error('CHECKPOINT_KEY');Object.defineProperty(o,k,{value:d(v),enumerable:true,writable:true,configurable:true});}return o;}
    default:throw new Error('CHECKPOINT_TAG');
  }
}
function stringify(x){const s=JSON.stringify(encode(x));if(Buffer.byteLength(s)>33554432)throw new Error('CHECKPOINT_BYTE_BOUND');return s;}
function parse(s){if(typeof s!=='string'||Buffer.byteLength(s)>33554432)throw new Error('CHECKPOINT_BYTE_BOUND');return decode(JSON.parse(s));}
const hash=s=>crypto.createHash('sha256').update(s).digest('hex');
const clone=x=>parse(stringify(x));
module.exports={encode,decode,stringify,parse,hash,clone};
