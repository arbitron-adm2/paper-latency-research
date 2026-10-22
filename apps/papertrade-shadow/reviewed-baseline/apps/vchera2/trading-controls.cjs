'use strict';
const {marginCapacity}=require('./execution-state.cjs');
function startBlockers(s) {
  const reasons=[];
  if (s.cutoffReached) reasons.push('Окно торговли закончилось: после 15:30 МСК старт запрещён.');
  if (s.highWaterTripped) reasons.push('Сработал дневной high-water: старт до следующего дня запрещён.');
  if (s.pausedUntil>s.now) reasons.push('Ещё действует защитная пауза после убытка.');
  if (!s.keyActive) reasons.push('Торговый ключ не активен.');
  if (!s.walletFresh) reasons.push('Нет свежего состояния кошелька.');
  if (!s.marketFresh) reasons.push('Нет свежих цен HL/Binance.');
  if (!s.executionFresh) reasons.push('Сверка исполнений ещё не завершена.');
  if (s.positionCount || s.pendingCount || s.walletPositions || s.walletOperations) reasons.push('Есть позиция или заявка: дождитесь подтверждённого закрытия/отмены.');
  const capacity=marginCapacity(s.available,s.margin);
  if(!capacity.ready) reasons.push(`Для полной маржи с резервом нужно ${capacity.required.toFixed(2)} USDC; доступно ${Number.isFinite(s.available)?s.available.toFixed(6):'неизвестно'}.`);
  return reasons;
}
function authorizedControl(req,legacyKill=false) {
  const header=legacyKill?req.headers['x-vchera2-kill']:req.headers['x-vchera2-control'];
  if(header!=='1')return false;
  let host;try{host=new URL('http://'+req.headers.host);}catch{return false;}
  if(!['127.0.0.1','localhost','[::1]'].includes(host.hostname)||host.username||host.password)return false;
  if(req.headers.origin && req.headers.origin!=='http://'+req.headers.host)return false;
  return true;
}
module.exports={startBlockers,authorizedControl};
