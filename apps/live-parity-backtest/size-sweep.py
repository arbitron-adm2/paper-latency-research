"""First reconcile $2k evidence, then freeze and inspect $1.2m/$1.26m counterfactuals."""
import argparse
import hashlib
import json
import os
import re
import statistics
import subprocess
from decimal import Decimal
from pathlib import Path
from model import cash, obj, quantile, successful_cancel
from prepare import ROOT, dump, readonly
from report import pair_attempts


def link(src, dest):
    dest.parent.mkdir(parents=True, exist_ok=True)
    os.link(src, dest)  # Immutable research inputs only; never an active database.


def prepare_run(reference, out):
    out.mkdir(parents=True, exist_ok=False)
    load = lambda name: json.loads((reference / name).read_text())
    plan = load('plan.json'); labels = load('labels.json'); previous = load('comparison.json')
    params = json.loads((reference / 'source/apps/papertrade-shadow/native-parameters.json').read_text())
    checks = []
    for r in labels:
        if r['status'] != 'closed':
            continue
        expected = r['close_evidence']['realPnl'] - r['close_evidence']['fee']
        got = cash(r['margin'], r['leverage'], r['entry'], r['exit'], r['side'] == 'L', params)['net']
        checks.append({'id': r['id'], 'net': expected, 'recalculated': got, 'error': got - expected})
    small = next(r for r in previous['freeModels'] if r['id'] == 'train_queue_and_observation')
    dump(out / 'stage1-2000.json', {
        'nominal': 2000, 'margin': 20, 'leverage': 100, 'closedLabels': len(checks),
        'actualNet': sum(r['net'] for r in checks), 'formulaMaxAbsoluteError': max(abs(r['error']) for r in checks),
        'formulaMatchesWithinOneMicroUsdc': all(abs(r['error']) < 1e-6 for r in checks),
        'freeReplayExactLiveReplica': False, 'previousHoldout': small,
        'decision': 'Full $2k strategy does not exactly match live. Large runs are exploratory SIM only, not validated profit forecasts.',
        'rows': checks})
    for name in ('labels.json', 'calibration.json', 'quotes.sqlite', 'input-summary.json',
                 'source-manifest.json', 'strategy-profiles.json', 'label-provenance.json', 'profile-provenance.json'):
        link(reference / name, out / name)
    for src in (reference / 'source').rglob('*'):
        if src.is_file():
            link(src, out / 'source' / src.relative_to(reference / 'source'))
    c = readonly(ROOT / 'data/unified/market.sqlite3')
    large = [dict(r) for r in c.execute("SELECT id,position_id,opened_at,closed_at,side,entry,exit,margin,leverage,status,reason,entry_info,exit_info,close_evidence FROM bot__btcy2_lag_trades WHERE live=1 AND status='closed' AND margin=2100 AND leverage=600 AND closed_at<? ORDER BY opened_at", (plan['start'],))]
    cancelled = [dict(r) for r in c.execute("SELECT id,opened_at,closed_at,status,reason FROM bot__btcy2_lag_trades WHERE live=1 AND status='cancelled' AND margin=2100 AND leverage=600 AND opened_at>=? AND closed_at<=?", (min(r['opened_at'] for r in large) - 60000, max(r['closed_at'] for r in large) + 60000))]
    c.close()
    if len(large) < 8:
        raise ValueError('INSUFFICIENT_BIG_EXECUTION_LABELS')
    for r in large:
        for k in ('entry_info', 'exit_info', 'close_evidence'):
            r[k] = obj(r[k])
    # Whitelisted journal observations, exact position ID on entry, canonical row ID + price on close.
    observations = []; first = min(r['opened_at'] for r in large); last = max(r['closed_at'] for r in large)
    child = subprocess.Popen(['journalctl', '--user', '-u', 'paper-vchera2-live-20261020.service',
                              '--since', '@' + str(first // 1000 - 60), '--until', '@' + str(last // 1000 + 60),
                              '--output=json', '--no-pager'], stdout=subprocess.PIPE, text=True)
    for line in child.stdout:
        row = json.loads(line); msg = row.get('MESSAGE', ''); t = int(row['__REALTIME_TIMESTAMP']) / 1000
        match = re.search(r'^\[btc-вчера2\] ВХОД LIVE .*?позиция (\d+)', msg)
        if match:
            observations.append({'kind': 'entry', 'pid': match[1], 'at': t})
        match = re.search(r'^\[btc-вчера2\] ВЫХОД #(\d+) @([\d.]+)', msg)
        if match:
            observations.append({'kind': 'exit', 'id': int(match[1]), 'px': float(match[2]), 'at': t})
    if child.wait() != 0:
        raise ValueError('JOURNAL_READ_FAILED')
    entry_obs = []; exit_obs = []; entry_blocks = []; exit_blocks = []; formulas = []
    for r in large:
        ei, xi = r['entry_info'], r['exit_info']
        entry_blocks.append(round((r['opened_at'] - ei['decidedAt'] // 1000 * 1000) / 1000))
        if xi.get('decidedAt') is not None and xi['decidedAt'] < r['closed_at']:
            exit_blocks.append(round((r['closed_at'] - xi['decidedAt'] // 1000 * 1000) / 1000))
        r['observed'] = {}
        for kind, pool, physical in [('entry', entry_obs, r['opened_at']), ('exit', exit_obs, r['closed_at'])]:
            hits = [o['at'] for o in observations if o['kind'] == kind and 0 <= o['at'] - physical < 30000
                    and ((kind == 'entry' and o.get('pid') == r['position_id'])
                         or (kind == 'exit' and o.get('id') == r['id'] and o.get('px') == r['exit']))]
            if hits:
                r['observed'][kind] = min(hits); pool.append(round(min(hits) - physical))
        ev = r['close_evidence']; expected = ev['realPnl'] - ev['fee']
        predicted = cash(2100, 600, r['entry'], r['exit'], r['side'] == 'L', params)['net']
        formulas.append({'id': r['id'], 'net': expected, 'recalculated': predicted, 'error': predicted - expected})
    small_cal = load('calibration.json')
    failed = sum(bool(r['entry_info'].get('cancelRequestedAt')) for r in large)
    success = sum(successful_cancel(r) for r in cancelled)
    cal = {**small_cal, 'trainIds': [r['id'] for r in large], 'maxLabelTime': last,
           'entryBlocks': entry_blocks, 'exitBlocks': exit_blocks,
           'entryObservationMs': entry_obs if len(entry_obs) >= 8 else small_cal['entryObservationMs'],
           'exitObservationMs': exit_obs if len(exit_obs) >= 8 else small_cal['exitObservationMs'],
           'observationCoverage': {'entry': len(entry_obs), 'exit': len(exit_obs), 'total': len(large)},
           'observationTransferFromSmall': {'entry': len(entry_obs) < 8, 'exit': len(exit_obs) < 8},
           'cancelSuccesses': success, 'cancelFailures': failed,
           'cancelSuccessProbability': (success + 1) / (success + failed + 2),
           'scope': '13 earlier BTC executions 2100x600; transfer to 2000x600 unvalidated',
           'queueAssociationWarning': 'Entry delay is bot journal association, not independently verified server receipt. One recorded delay is 65 seconds.',
           'quoteLagMs': 0}
    dump(out / 'large-labels.json', large); dump(out / 'large-calibration.json', cal)
    dump(out / 'large-actual-accounting.json', {'closed': len(large), 'nominal': 1260000,
         'net': sum(r['net'] for r in formulas), 'formulaMaxAbsoluteError': max(abs(r['error']) for r in formulas),
         'formulaMeanAbsoluteError': statistics.mean(abs(r['error']) for r in formulas), 'rows': formulas})
    launch = json.loads((ROOT / 'apps/vchera2/launch-config.json').read_text())
    ttl = launch.get('independentGuards', {}).get('entrySignedValidityMs', 5000)
    plan['variantSettings'] = {
        'train_queue_and_observation': {'sizing': {'margin': 20, 'leverage': 100}, 'entryTtlMs': ttl},
        'large_1200000': {'sizing': {'margin': 2000, 'leverage': 600}, 'calibration': 'large-calibration.json', 'entryTtlMs': ttl},
        'large_1260000': {'sizing': {'margin': 2100, 'leverage': 600}, 'calibration': 'large-calibration.json', 'entryTtlMs': ttl}}
    plan.update(variants=list(plan['variantSettings']), reconcileSignedExpiry=True,
                revision='nominal-sweep-1', referenceRun=str(reference),
                minimumCapitalRule='Available USDC >= margin; no extra 5 percent reserve, no virtual top-ups',
                largeOutcomeMeaning='Counterfactual. Actual admission/OI and current large queue are not established.',
                signedExpiry={'ttlMs': ttl, 'reconciliationGraceMs': 30000, 'source': 'current entry-deadline.cjs'})
    dump(out / 'plan.json', plan)
    manifest = []
    for src in (ROOT / 'apps/live-parity-backtest').glob('*'):
        if src.is_file():
            b = src.read_bytes(); dest = out / 'own-source' / src.name; dest.parent.mkdir(exist_ok=True)
            dest.write_bytes(b); manifest.append({'file': src.name, 'sha256': hashlib.sha256(b).hexdigest()})
    dump(out / 'own-source-manifest.json', manifest)
    print(json.dumps({'stage1FullParity': False, 'smallFormulaMatches': True, 'largeLabels': len(large),
                      'largeActualNet': sum(r['net'] for r in formulas), 'largeEntryBlocks': entry_blocks,
                      'largeExitBlocks': exit_blocks, 'observations': cal['observationCoverage'], 'ttlMs': ttl}))


def finish(out):
    load = lambda n: json.loads((out / n).read_text())
    plan = load('plan.json'); result = load('strategy-result-0.json')
    previous = json.loads((Path(plan['referenceRun']) / 'strategy-result-0.json').read_text())
    labels = load('labels.json'); models = []
    for variant, row in zip(result['variants'], result['results']):
        entries = variant['entries']; validation_ids = {e['id'] for e in entries if e['createdAt'] >= plan['split']}
        trades = [t for t in row['trades'] if t['intentId'] in validation_ids]
        sizing = variant['sizing']; margin_raw = int(Decimal(str(sizing['margin'])) * 10**18)
        assert all(int(t['margin_raw']) == margin_raw and t['leverage'] == sizing['leverage'] for t in row['trades'])
        net = sum(int(t['cash']) for t in row['trades'])
        assert Decimal(variant['summary']['cashUsdc']) == Decimal(2300) + Decimal(net) / Decimal(10**18)
        assert variant['summary']['virtualTopUps'] == 0
        models.append({'id': variant['id'], 'sizing': sizing, 'allClosed': len(row['trades']),
                       'allNet': float(Decimal(net) / Decimal(10**18)), 'holdoutClosed': len(trades),
                       'holdoutNet': sum(int(t['cash']) / 1e18 for t in trades),
                       'holdoutLate': sum(t['late'] for t in trades), 'counts': row['counts'],
                       'finalCash': variant['summary']['cashUsdc'], 'locked': variant['summary']['lockedUsdc'],
                       'reserved': variant['summary']['reservedUsdc'], 'errors': variant['summary']['errors'],
                       'lastEntryPolicy': row['lastEntryPolicyReason'],
                       'sourcePositions': variant['nativePositions'], 'sourcePending': variant['nativePending'],
                       'pendingObservations': variant['observationPending']})
    before = next(r for r in previous['results'] if r['profile']['id'] == 'train_queue_and_observation')
    after = next(r for r in result['results'] if r['profile']['id'] == 'train_queue_and_observation')
    fields = ['openedAt', 'closedAt', 'entry_raw', 'exit_raw', 'margin_raw', 'leverage', 'long', 'cash', 'reason', 'late']
    fingerprint = lambda rows: [{k: r[k] for k in fields} for r in rows]
    same = fingerprint(before['trades']) == fingerprint(after['trades'])
    targets = [{'id': r['id'], 'at': r['entry_info'].get('decidedAt', r['opened_at']), 'side': r['side'], 'strategy': r['strategy']}
               for r in labels if 'stale' not in (r['reason'] or '') and r['entry_info'].get('decidedAt', r['opened_at']) >= plan['split']]
    small_v = next(v for v in result['variants'] if v['id'] == 'train_queue_and_observation')
    matched = pair_attempts(targets, [e for e in small_v['entries'] if e['createdAt'] >= plan['split']])
    accounting = load('large-actual-accounting.json')
    output = {'mode': 'OFFLINE_NOMINAL_SWEEP_NO_ORDERS', 'stage1': load('stage1-2000.json'),
              'smallPriorRunReproducedExactlyIgnoringSyntheticIds': same,
              'smallLiveAttemptMatching': matched, 'models': models, 'largeHistoricalAccounting': accounting,
              'financialMutations': 0, 'actualOrders': 0,
              'largePerformanceStatus': 'UNVALIDATED_SIM_COUNTERFACTUAL; NO EXACT_1_2M_LIVE_LABELS_IN_THIS_WINDOW',
              'inputEvents': result['inputEvents'], 'inputSha256': result['inputSha256']}
    dump(out / 'size-comparison.json', output)
    text = ['# Проверка $2000, затем $1.2 млн — 21.10.2026', '',
            'Расчёт офлайн. Окно вечерней канонической ленты: 20.10, 22:20–23:25 МСК; '
            'проверочная часть с 22:50. Одинаковый начальный виртуальный капитал 2300 USDC, '
            'одна позиция, без пополнений. Для каждого размера заново рассчитываются заявки, '
            'ликвидация, комиссии, доступная маржа и отказ от следующего входа.', '',
            '## Сначала $2000', '',
            f'Повтор предыдущего полного SIM-прогона: **{same}**. Сравнены цены, времена, '
            'сторона, маржа, плечо, выплаты и причины всех закрытий; технические ID не сравнивались. '
            'Это проверка воспроизводимости симулятора, а не совпадения с живой торговлей.', '',
            'Формула выплаты совпадает со всеми 46 живыми закрытиями до 0.000001 USDC. '
            'Живой итог целого окна +0.224114 USDC, проверочных 29 сделок +0.243497 USDC. '
            f'На проверке совпало {matched["matched"]}/{matched["actualAttempts"]} живых попыток. '
            'Полного совпадения стратегии нет, поэтому большие результаты ниже не являются '
            'подтверждённым прогнозом прибыли.', '',
            '## Затем большие размеры', '',
            '| Номинал | Маржа × плечо | Закрытий всего | Net всего, USDC | Закрытий после 22:50 | Net после 22:50 | Остаток USDC |',
            '|---|---|---:|---:|---:|---:|---:|']
    for m in models:
        text.append(f'| ${m["sizing"]["notional"]:,} | {m["sizing"]["margin"]} × {m["sizing"]["leverage"]} | '
                    f'{m["allClosed"]} | {m["allNet"]:+.6f} | {m["holdoutClosed"]} | {m["holdoutNet"]:+.6f} | {float(m["finalCash"]):.6f} |')
    cal = load('large-calibration.json')
    text += ['', f'Большая модель использует {accounting["closed"]} более ранних закрытий нашего бота '
             'на 2100×600 ($1.26 млн), а не умножение задержек или PnL маленьких сделок. '
             f'У входа {len(cal["entryBlocks"])} образцов, выхода {len(cal["exitBlocks"])}. '
             f'Покрытие задержки уведомлений: {cal["observationCoverage"]}. '
             f'Текущий срок подписанного входа {plan["signedExpiry"]["ttlMs"]/1000:.0f} с; '
             'истечение сверяется после дополнительных 30 с по политике клиента.', '',
             f'Фактический исторический итог этих 13 больших сделок: **{accounting["net"]:+.6f} USDC**. '
             f'Формула на их фактических ценах отличается максимум на {accounting["formulaMaxAbsoluteError"]:.9f} USDC. '
             'Это другое время и другой режим, не живой результат вечернего прогона.', '',
             'Ограничения:', '',
             '- На этом вечернем окне нет живых исполнений ровно $1.2 млн для поштучной проверки. '
             'Перенос 13 более ранних наблюдений на новый рынок и соседний номинал не подтверждён.',
             '- Серверная очередь, наличие OI и приём большой заявки не восстановлены. '
             'Известный поздний отказ большой LONG-заявки по OI показывает, что модель может предполагать вход там, где его не дадут.',
             '- Один связанный журналом вход ожидал около 65 с. С текущим сроком 20 с он моделируется как истёкший; '
             'историческая связь решения и исполнения не подтверждена отдельной серверной квитанцией.',
             '- Вероятность отмены по малой исторической выборке не является установленным законом. '
             'Один фиксированный seed не оценивает ожидаемую прибыль и риск хвостов.',
             '- Начальный капитал — условные 2300 USDC, не утверждение о текущем балансе кошелька. '
             'Виртуальные пополнения запрещены; после нехватки полной маржи новые входы пропускаются.', '',
             'Файлы: size-comparison.json, stage1-2000.json, large-calibration.json, '
             'large-actual-accounting.json, strategy-result-0.json, strategy-0.sqlite.']
    (out / 'report.md').write_text('\n'.join(text) + '\n')
    print(json.dumps({'smallReproducible': same, 'models': models, 'largeHistoricalNet': accounting['net']}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    p = argparse.ArgumentParser(); p.add_argument('--reference', type=Path); p.add_argument('--out', type=Path, required=True)
    p.add_argument('--report', action='store_true'); a = p.parse_args()
    if a.report:
        finish(a.out)
    else:
        if not a.reference:
            p.error('--reference required for preparation')
        prepare_run(a.reference, a.out)
