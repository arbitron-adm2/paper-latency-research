'use strict';
// Offline regression preloader: any accidental network library import fails immediately.
const Module=require('node:module'),load=Module._load;
const denied=new Set(['net','http','https','http2','tls','dns','dgram','undici','ws','ethers']);
Module._load=function(id,...args){if(denied.has(id.replace(/^node:/,'')))throw new Error('OFFLINE_NETWORK_DENIED: '+id);return load.call(this,id,...args);};
globalThis.fetch=()=>{throw new Error('OFFLINE_NETWORK_DENIED');};
globalThis.WebSocket=class{constructor(){throw new Error('OFFLINE_NETWORK_DENIED');}};
