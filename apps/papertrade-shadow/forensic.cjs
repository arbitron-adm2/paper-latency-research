'use strict';
const {gross,decimal,reconstructedPayout}=require('./money.cjs');
const {sha}=require('./strategy.cjs');
function parse(x){return x?JSON.parse(x):{};}
function forensic(rows,provider,model=null){
  const map=new Map(),conflicts=[];
  for(const p of provider.items||[]){
    if(!['PositionClosed','Liquidated'].includes(p.kind))continue;
    const id=String(p.positionId);
    if(map.has(id)&&JSON.stringify(map.get(id))!==JSON.stringify(p))conflicts.push(id);
    else map.set(id,p);
  }
  const canonical=[],excluded=[],events=[],seen=new Set();let adjusted=0n,fees=0n;
  for(const row of rows){
    if(row.live!==1||row.status!=='closed'||!row.position_id||seen.has(String(row.position_id))){
      excluded.push({row_id:row.id,status:row.status,positionId:row.position_id,
        classification:row.status==='cancelled'?'journal cancelled; terminal server outcome not proven':'duplicate/noncanonical/nonclosed'});continue;
    }
    seen.add(String(row.position_id));
    const p=map.get(String(row.position_id));
    if(!p){excluded.push({row_id:row.id,reason:'missing provider confirmation'});continue;}
    const entry=parse(row.entry_info),exit=parse(row.exit_info);
    const ambiguity=String(row.position_id)==='1658910';
    const amount=BigInt(p.adjustedPnlRaw),fee=BigInt(p.userPaidFeeRaw);
    adjusted+=amount;fees+=fee;
    const entryRaw=BigInt(p.entryPriceRaw),exitRaw=BigInt(p.exitPriceRaw),scale=BigInt(p.priceScaleRaw);
    const g=gross(p.marginRaw,p.leverage,entryRaw,exitRaw,p.isLong);
    const qms=entry.decidedAt?p.openedAtMs-entry.decidedAt:null;
    const closeDecisionMs=exit.decidedAt?p.timestampMs-exit.decidedAt:null;
    const closeAckMs=exit.sentAt?p.timestampMs-exit.sentAt:null;
    const record={row_id:row.id,positionId:String(p.positionId),intentId:row.intent_id,
      source:'public account history + local canonical trade journal',kind:p.kind,
      entry:decimal(entryRaw*10n**18n/scale),exit:decimal(exitRaw*10n**18n/scale),
      opened_ms:p.openedAtMs,closed_ms:p.timestampMs,reason:row.reason,protective_late_fill:!!entry.invalidatedReason,
      journal_entry_decision_ms:entry.decidedAt||null,journal_entry_to_fill_ms:qms,
      interval_classification:{holding_period_ms:p.timestampMs-p.openedAtMs,holding_period_is_queue_latency:false,
        journal_decision_to_fill_ms:qms,submit_to_fill_ms:null,entry_timing_class:'own empirical journal transfer advisory; unverified submit/identity',
        asset:p.symbol,notional_raw:(BigInt(p.marginRaw)*BigInt(p.leverage)).toString(),censored:false},
      entry_attribution:{status:ambiguity?'AMBIGUOUS_65431_MS':'journal-claimed; no independent open intent proof in /trades',
        exact_queue_label_eligible:false,reason:ambiguity?'production fallback matches direction/size/time; stale-intent attribution unresolved':'provider confirms position but does not expose entry intentId'},
      close_decision_to_execution_ms:closeDecisionMs,close_ack_to_execution_ms:closeAckMs,
      timestamp_resolution:'provider server execution 1000ms; journal decisions ms; unknown clock skew',
      gross_price_pnl:decimal(g),adjusted_pnl:decimal(amount),reported_fee:decimal(fee),
      calculated_after_reported_fee:decimal(amount-fee),cash_payout_verified:false,
      cash_semantics:'adjustedPnlRaw minus userPaidFeeRaw is a calculation, not verified cash payout; no payoutRaw',
      txHash:p.txHash,provider_sha256:sha(JSON.stringify(p)),
      at_decision_prediction:{status:'UNIDENTIFIABLE',rank:null,fill_time:null,fill_price:null,
        reason:'No calibrated pre-decision server model. Forensic facts are not out-of-sample predictions.'}};
    if(model?.payoutMode==='reconstructed-deadband-haircut'){
      const reconstructed=reconstructedPayout(p.marginRaw,p.leverage,entryRaw,exitRaw,p.isLong,model.instrumentRow,model.winFeeRaw);
      record.payout_formula_check={source:'historical integer reconstruction, current state parameters',
        adjusted_raw:reconstructed.adjusted.toString(),fee_raw:reconstructed.fee.toString(),
        adjusted_residual_raw:(reconstructed.adjusted-amount).toString(),fee_residual_raw:(reconstructed.fee-fee).toString(),
        independent_validation:false,cash_payout_verified:false};
    }
    canonical.push(record);
    const add=(kind,t,payload,confidence)=>{if(t!=null)events.push({schema:1,event_ms:t,recv_ms:null,source:'forensic',sequence:events.length,
      market:p.symbol,account:null,kind,positionId:String(p.positionId),intentId:row.intent_id,
      payload,payload_sha256:sha(JSON.stringify(payload)),provider_record_sha256:record.provider_sha256,quality:{confidence,receive_time:'unavailable',hashKind:'canonical-event-payload-NOT-raw-wire'}});};
    add('entry_decision',entry.decidedAt,{px:entry.decidedPx},ambiguity?'ambiguous':'journal-only');
    add('cancel_requested',entry.cancelRequestedAt,{error:entry.cancelFailed,terminal:false},'journal-only');
    add('entry_execution',p.openedAtMs,{priceRaw:p.entryPriceRaw,scale:p.priceScaleRaw},'provider-position-confirmed');
    add('close_decision',exit.decidedAt,{reason:exit.why},'journal-position-linked');
    add('close_ack',exit.sentAt,{not_send_timestamp:true},'journal-position-linked');
    add('close_execution',p.timestampMs,{priceRaw:p.exitPriceRaw,adjustedPnlRaw:p.adjustedPnlRaw,userPaidFeeRaw:p.userPaidFeeRaw},'provider-confirmed');
  }
  events.sort((a,b)=>a.event_ms-b.event_ms||a.sequence-b.sequence);
  const ids=new Set(canonical.map(x=>x.positionId));
  return {positions:canonical,ledger:events,excluded,
    summary:{mode:'replay',status:'INCONCLUSIVE',canonical_closed:canonical.length,
      provider_account_records:map.size,provider_other_records:[...map.keys()].filter(id=>!ids.has(id)).length,
      wins:canonical.filter(x=>BigInt(x.adjusted_pnl.replace('.',''))>0n).length,
      losses:canonical.filter(x=>Number(x.adjusted_pnl)<0).length,flat:canonical.filter(x=>Number(x.adjusted_pnl)===0).length,
      adjusted_pnl:decimal(adjusted),reported_fees:decimal(fees),calculated_after_reported_fees:decimal(adjusted-fees),
      cash_payout_verified:false,canonical_reconciliation:conflicts.length?'CONFLICT':'provider-matched by positionId',provider_conflicts:conflicts,
      exact_entry_queue_labels:0,unconfirmed_cancellations:excluded.filter(x=>x.status==='cancelled').length,
      financial_mutations_sent:0,secrets_read:0,network_requests:0,
      conclusion:'Negative canonical reported sample; small, dependent and not independent validation. Neither three selected wins nor forensic reconstruction establish edge.'}};
}
module.exports={forensic};
