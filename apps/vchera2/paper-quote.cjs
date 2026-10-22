'use strict';
// Same BBO -> quote conversion used by the production PaperTrade frontend:
// wire-DbTMi4Qf.js network.wsApiUrl + focus-CXaE1eE0.js export u.
// It is the site's live quote, NOT a promised future execution price.
function scaled(raw, scale) {
  if (typeof raw !== 'string' || !/^\d+(?:\.\d+)?$/.test(raw)) throw new Error('Invalid quote price');
  const [whole,decimal='']=raw.split('.');
  const n=BigInt(whole+decimal)*BigInt(scale),d=10n**BigInt(decimal.length);
  if(n<=0n || n%d!==0n)throw new Error('Quote price does not match instrument scale');
  return n/d;
}
function paperQuote(data, now=Date.now(), scale=10, symbol='BTC') {
  if (!['BTC','ETH'].includes(symbol) || scale !== (symbol === 'BTC' ? 10 : 100) || data.coin!==symbol || !Array.isArray(data.bbo) || data.bbo.length!==2 || !(Number(data.time)>0))throw new Error('Invalid Paper BBO/instrument scale');
  const [b,a]=data.bbo,bidRaw=scaled(b.px,scale),askRaw=scaled(a.px,scale);
  if(askRaw<bidRaw)throw new Error('Crossed Paper BBO');
  const bid=Number(bidRaw)/scale,ask=Number(askRaw)/scale,mid=Number((bidRaw+askRaw)/2n)/scale;
  const bs=Number(b.sz),as=Number(a.sz),total=bs+as;
  if(!Number.isFinite(total)||bs<0||as<0)throw new Error('Invalid BBO size');
  return {bid,ask,mid,weightedPrice:total>0?(bid*as+ask*bs)/total:mid,t:now,sourceTimeMs:Number(data.time),
    bookAgeMs:null,imb:null,bookNA:true,symbol,source:`PaperTrade website quote: Hyperliquid BBO, ${symbol} scale ${scale}; no HL smoothing`,
    sourceUrl:'https://exchange.papertrade.xyz/assets/focus-CXaE1eE0.js'};
}
module.exports={paperQuote};
