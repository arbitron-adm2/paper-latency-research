'use strict';
function metrics(trades,{initialCapital=2300,balance=2300,locked=0,reserved=0,unknown=0,drawdown=0,fees=0,exposureMs=0}={}){
 let wins=0,losses=0,zeros=0,gains=0,loss=0,net=0,equity=initialCapital,high=initialCapital,closedDd=0;const values=[];
 for(const t of trades){const x=Number(t.cash??t.cashRaw)/1e18;values.push(x);net+=x;equity+=x;high=Math.max(high,equity);closedDd=Math.max(closedDd,high-equity);if(x>1e-8){wins++;gains+=x;}else if(x< -1e-8){losses++;loss-=x;}else zeros++;}
 const n=trades.length,p=n?wins/n:null,z=1.96,den=n?1+z*z/n:1,center=n?(p+z*z/(2*n))/den:null,half=n?z*Math.sqrt(p*(1-p)/n+z*z/(4*n*n))/den:null;
 return{closed:n,wins,losses,zeros,wr:p,wrExcludingZeros:wins+losses?wins/(wins+losses):null,wr95:n?[Math.max(0,center-half),Math.min(1,center+half)]:null,netUsdc:net,profitFactor:loss?gains/loss:gains?null:0,profitFactorInfinite:!loss&&gains>0,expectancyUsdc:n?net/n:null,feesUsdc:fees,maxClosedDrawdownUsdc:closedDd,maxGrossMarkedDrawdownUsdc:drawdown,netWithoutBestTradeUsdc:n?net-Math.max(...values):null,endingCashUsdc:balance,lockedUsdc:locked,reservedUsdc:reserved,unresolved:unknown,exposureMs,openRiskExcludedFromClosedPnl:locked>0||reserved>0||unknown>0,netAfterFullCollateralLossSensitivityUsdc:net-locked-reserved,collateralSensitivityIsActualLoss:false,performance:'CONDITIONAL_SIM_NOT_VERIFIED_CASH'};
}
module.exports={metrics};
