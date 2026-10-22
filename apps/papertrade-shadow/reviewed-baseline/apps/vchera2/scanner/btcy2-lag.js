// СГЕНЕРИРОВАНО make-yesterday2-bot.cjs из scanner/btcy-lag.js («вчера») + три правила основного. Правки вносить в генератор.
// СГЕНЕРИРОВАНО make-yesterday-bot.cjs из backup/bot-20261012-1200/btc-lag.js — код BTC-бота на 12.10.2026 12:00.
// ВНИМАНИЕ: 14.10 правлено руками поверх генератора (ищи «14.10» и «📝 Бумага»): бумажный режим (paperDelay/getPaperStats,
// вход/выход через задержки очереди) и сторож потоков stream() с поколениями. Перезапуск генератора всё это сотрёт.
// ============================================================
// BTC-бот для papertrade: торгует BTC по цене Hyperliquid,
// сигналы берёт из стаканов Hyperliquid и Binance.
//
// На чём построено (замеры 2026-10-12, синхронная запись трёх рынков + сделки эталона):
//   • papertrade исполняет СТРОГО по mid Hyperliquid — все цены входа и выхода
//     эталонного бота совпали с mid HL до цента, без задержки;
//   • резкий ход Binance Futures HL повторяет с опозданием: 11% сразу, 53% за 0.5 с,
//     83% за 1 с. Отклонение спреда от фона ≥ $10 — ~2 раза в минуту, и через 5 с
//     HL сдвигался в сторону Binance в 88% случаев;
//   • крупнейшие прибыли эталона (+$3 398, +$3 110) пришли БЕЗ спреда: на всех
//     стаканах у цены стоял перевес продавцов 77–99%, на HL заявка 0.9–1.4M на продажу.
//
// Отсюда две стратегии, работают параллельно и независимо:
//   «Спред»   — HL отстал от Binance сверх фона, торгуем HL вдогонку;
//   «Лимитки» — перевес крупных заявок у цены, торгуем по направлению перевеса.
//
// Спред сравниваем не с нулём, а с ФОНОМ: HL котируется в USDC, Binance в USDT,
// плюс разный фандинг — между ними всегда есть медленная разница в несколько долларов.
//
// Только WebSocket: REST Binance Futures банит IP за частые запросы,
// а с этого же IP работают сканер и парсеры журнала.
// ============================================================

const { db } = require('./../db');
const fs = require('fs');
const path = require('path');
const WS = globalThis.WebSocket || require('ws');
// Isolated readiness integration; original source snapshot remains untouched.
const SAFETY = require('../safety.cjs');
const { ExecutionJournal } = require('../execution-journal.cjs');
const { parseCloseTrade, buildLiveFill, trailingHit } = require('../execution-state.cjs');
const { startBlockers } = require('../trading-controls.cjs');
const { paperQuote } = require('../paper-quote.cjs');
const { marketData } = require('../market-data.cjs');
// 15.10: свой аккаунт (свой ключ бота и свой кошелёк), чтобы торговать параллельно с основным и не трогать его позиции
// 15.10 вечер, Влад: «бота вчера не могу включить вживую… дай возможность, чтобы я сам мог включать одного или другого».
// Свой ключ «вчера» так и не разрешён в MetaMask, поэтому настройка useMainKey: торговать ключом и кошельком основного.
// LIVE тогда может быть только у одного из двух ботов (проверка в server.js по одинаковому кошельку).
// Регистрация ключа и поток кошелька всегда идут через свой аккаунт, чтобы не сбить ключ основного.
const ptMod = require('./papertrade');
const FG = require('./funding-guard');   // фандинг в начале часа: за 4 мин без входов, за 60 с закрыть позиции
const ptOwn = ptMod.getAccount('btcy2'), ptMain = ptMod.getAccount('main');
const PT_OWN_ONLY = new Set(['walletStream', 'registrationTypedData', 'submitRegistration', 'ensureKey', 'keyFile']);
const ptUsingMain = () => { try { return !!(cfg && cfg.useMainKey); } catch { return false; } };
const pt = new Proxy({}, { get(_, k) { const a = ptUsingMain() && !PT_OWN_ONLY.has(k) ? ptMain : ptOwn; const v = a[k]; return typeof v === 'function' ? v.bind(a) : v; } });

db.exec(`
CREATE TABLE IF NOT EXISTS btcy2_lag_config (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS btcy2_lag_trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  opened_at INTEGER NOT NULL,
  closed_at INTEGER,
  side TEXT NOT NULL,              -- L | S
  entry REAL NOT NULL,
  exit REAL,
  qty REAL NOT NULL,               -- BTC
  margin REAL NOT NULL,
  leverage REAL NOT NULL,
  dev REAL,                        -- отклонение спреда от фона при входе, $ на 1 BTC
  spread REAL,                     -- сырой спред HL − Binance Fut при входе
  baseline REAL,                   -- фон спреда при входе
  bin_move REAL,                   -- ход Binance Fut в сторону сделки перед входом
  imb TEXT,                        -- перевес у цены HL/BinF/BinS при входе
  reason TEXT,                     -- причина выхода
  gross_pnl REAL,                  -- полный результат по цене
  est_pnl REAL,                    -- оценка выплаты платформы (с её урезанием прибыли)
  max_fav REAL,                    -- лучший ход за сделку, $ на 1 BTC
  max_adv REAL,                    -- худший ход за сделку
  status TEXT NOT NULL DEFAULT 'open'
);
CREATE INDEX IF NOT EXISTS idx_btcy2_lag_trades_opened ON btcy2_lag_trades(opened_at);
`);
try { db.exec("ALTER TABLE btcy2_lag_trades ADD COLUMN strategy TEXT DEFAULT 'spread'"); } catch {}
try { db.exec('ALTER TABLE btcy2_lag_trades ADD COLUMN l1 TEXT'); } catch {}
// Живые сделки: номер позиции и заявки на платформе, настоящий итог из её истории
try { db.exec('ALTER TABLE btcy2_lag_trades ADD COLUMN live INTEGER DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE btcy2_lag_trades ADD COLUMN position_id TEXT'); } catch {}
try { db.exec('ALTER TABLE btcy2_lag_trades ADD COLUMN intent_id TEXT'); } catch {}
try { db.exec('ALTER TABLE btcy2_lag_trades ADD COLUMN real_pnl REAL'); } catch {}
// Сила сигнала при входе: для «Спреда» — отставание HL в $, для «Лимиток» — перевес 0…1.
// По ней бот потом считает, сколько в среднем приносит сигнал такой силы.
try { db.exec('ALTER TABLE btcy2_lag_trades ADD COLUMN sig REAL'); } catch {}
const executionJournal = new ExecutionJournal(db);
executionJournal.ensureUnique();

// ---------- настройки ----------
const DEFAULTS = {
  enabled: true,          // общий выключатель
  margin: 5714.28,        // как у эталона: $5 714 × 700 ≈ $4M позиция
  leverage: 700,
  cooldownSec: 3,
  // Сколько позиций держать, бот решает сам по своей статистике (см. decideEntry).
  // maxPositions — потолок на весь бот (спред + лимитки вместе). Эталон докупает пачками до 3–5.
  maxPositions: 2,
  addMargin: 200,         // маржа дополнительных позиций, USDC; 0 = такая же, как первая (как у эталона)
  sizeToOi: false,        // резать размер входа под запас OI (входить частями, как эталон) — пока выключено
  // Защита без присмотра
  maxDayLossUsd: 0,       // дневной убыток (закрытые + открытые) достиг этого → входы выкл, позиции закрыть; 0 = выкл
  lossWindow: 10,         // плохая полоса: смотрим последние столько сделок
  maxLossStreak: 4,       // …убытков среди них ≥ этого; 0 = выкл
  lossUsd: 100,           // …и минус по окну ≥ этого → пауза входов
  lossHardUsd: 300,       // минус по окну ≥ этого → пауза в любом случае; 0 = выкл
  streakPauseMin: 10,     // …на столько минут
  // Telegram в личку (ADMIN_TELEGRAM_ID): защита пишет всегда; плюс по желанию каждая сделка и сводка
  tgEnabled: false,       // общий выключатель Telegram-уведомлений бота (выкл — не пишет ничего и никуда)
  tgTrades: false,        // писать каждую закрытую сделку
  tgSummaryMin: 60,       // сводка «мы против эталона» раз в столько минут; 0 = выкл
  // Доля Binance в решениях по стаканам (перевес, разворот, фильтр входа): Binance ведёт цену,
  // HL только подтверждает — 0.75 = Binance 75%, HL 25%. Крупный уровень по-прежнему ищем на HL.
  binWeight: 0.75,
  bookFlipProfitImb: 0.85, // позиция в плюсе: закрывать по развороту стакана только при перевесе ≥ этого (0.85 = 85%)
  bigProfitUsd: 200,       // прибыль позиции в деньгах, с которой откат Binance терпим шире
  binExitBigUsd: 10,       // …и вот настолько ($ хода Binance от лучшего уровня)
  minSamples: 15,         // столько закрытых сделок нужно по сигналу такой силы, чтобы ему доверять
  evMinUsd: 0,            // первая позиция: средний итог сигнала, $ на 1 BTC, не ниже
  evAddUsd: 2,            // добавить позицию: сигнал должен приносить не меньше
  noAddToLoser: true,     // не добавлять, если уже открытые позиции в минусе
  maxFeedAgeMs: 1500,     // данные старше — не входим

  // --- очередь papertrade ---
  // Заявки на платформе исполняются не сразу, а по очереди. По видео эталона:
  // исполнения ждали 3–5.5 с, а 17 из 23 заявок он отменил, не дождавшись.
  // Без этой задержки бумажный тест показывал бы прибыль, которой не будет.
  // 15.10 Влад: «в HIGH пусть вообще не торгует». Режим по размаху Binance за 5 мин (scanner/regime.js), границы как у основного:
  // HIGH от 129$ (с гистерезисом 10% и выдержкой 2 мин). За 14–15.10 «вчера» в HIGH −32.5K, в LOW+NORMAL +11.7K.
  regHaltHigh: true, regLowUsd: 82, regHighUsd: 129, regHystPct: 10, regHoldSec: 120,
  // 15.10 ~22:50 Влад: «очень сильно вредит всё-таки BTC-боту вчера лонги» (бумага за всё время: лонги −16.3K из −19.3K)
  // 🧪 «вчера 2»: три правила основного перед входом (0 = правило выключено)
  g3Enabled: true, g3MaxBinMove: 20, g3MaxCushion: 12, g3MinBfBook: 60,
  noLongs: true,          // только шорты: лонг не открывается никаким путём (спред, лимитки); открытые доводятся по правилам
  // 16.10 ~00:55 Влад: «ограничения по стенке на ботов, которых тестируем» — стена как у основного: заявка от 5 млн$ в 30$ от цены
  // (и 5 с после того, как исчезла) — не входить. У «вчера» выкл (остаётся как есть для сравнения), у «вчера 2» вкл (генератор).
  wallGuard: true,
  entryDelayMs: 3000,     // сколько ждём исполнения входа
  exitDelayMs: 3000,      // и выхода — цена берётся на момент исполнения, а не решения
  // Живой вход: у эталона исполнение 3–5 с, и 17 из 23 заявок он отменяет. Поздний вход = вход после хода:
  // 12.10 06:51–07:00 наши входы исполнялись через 2–7.6 с на 9–18$ хуже решения и дали −$189.
  entryMaxWaitMs: 3000,   // не исполнился за это время — отменяем
  cancelMissedUsd: 8,     // HL уже прошёл столько $ в нашу сторону, пока заявка стояла — ход упущен, отменяем
  maxBinRange10s: 40,     // Binance за 10 с прошёл больше (max−min) — рынок слишком быстрый для очереди 8 с, не входим
  // Спред входит только вместе со стаканами: средний перевес включённых стаканов у цены в сторону сделки
  // должен быть ≥ этого (0.2 = 20%). У эталона стаканы были «за» в 85% входов, «ровные/против» — убыточны.
  bookFilterImb: 0.2,
  cushionMaxHoldSec: 60,  // пока подушка спреда цела, позицию «не пошла» держим до стольких секунд
  bookHoldImb: 0.3,       // …или пока крупные заявки у цены за нас с перевесом ≥ этого (0.3 = 65% объёма на нашей стороне)
  cancelIfSignalGone: true, // пока стоим в очереди на вход, сигнал пропал — отменяем
  maxQueue: 5,            // для работы через API: впереди больше N человек — отмена
  splitParts: 3,           // 17.10: при отказе OI добирать до N частей (1 = одна позиция, как было)
  splitMinUsd: 250,        // 17.10 Влад: самый мелкий кусок лесенки, $ маржи (0 = 1/splitParts)
  splitStartUsd: 0,        // 17.10 Влад: с какой маржи начинать лесенку (0 = со всей маржи) — первая заявка меньше, реже отказ OI, раньше в очереди

  // --- выход, общий для обеих стратегий ---
  // Главный выход — по Binance: он ведёт HL, поэтому его разворот виден раньше,
  // и HL закрываем, пока он ещё на пике. На 99 входах эталона (запись 2026-10-12)
  // с 3 с очереди: +5.28 / +1.56 $ на BTC (старые/свежие сделки) против +2.99 / +0.54
  // у самого эталона и +0.99 / −1.62 у выхода по цене HL.
  binExitUsd: 8,          // Binance откатился от своего лучшего уровня на столько $ — выходим
  stopUsd: 8,             // стоп, $ на 1 BTC против позиции по HL (у эталона медиана −8)
  // Правила по цене HL — по умолчанию выключены (0): в проверенной схеме их нет,
  // они закрывают прибыльные сделки раньше, чем разворачивается Binance.
  beStartUsd: 0,          // после такого плюса стоп переносится в безубыток
  trailStartUsd: 0,       // после такого плюса включается трейлинг по HL…
  trailUsd: 8,            // …и выходим, если отдали столько от максимума
  stallSec: 0,            // в плюсе: нет нового максимума HL столько секунд — выходим
  takeProfitUsd: 0,       // фиксированный тейк, $ на 1 BTC (0 = выкл)
  // Платформа режет прибыль тем меньше, чем она больше (срез ≈ 2.5·√хода: при ходе
  // $10 срезает 74%, при $96 — 26%). Поэтому позицию в плюсе таймер НЕ закрывает:
  // держим, пока идёт движение. Таймер ниже — только для сделок, которые не пошли.

  // --- «Спред» ---
  spreadEnabled: true,
  minDev: 10,             // вход: HL отстал от Binance сверх фона на столько $ (на 1 BTC)
  // 18.10 ~03:25 Влад: «вчера 2 — чтобы не шортил, когда HL ниже своего среднего за полчаса» (как у основного): подушка на вход
  // от minDev и по быстрому фону (baselineSec), и по медленному — медиана спреда за slowBaselineMin минут. По умолчанию выкл.
  cushBoth: false,
  slowBaselineMin: 30,
  // 19.10 ~22:45 Влад: выход основного «подушка против» — HL ушёл на «не ту» сторону от Binance на cushionExitUsd $
  // (худшее из двух: от фона за 2 мин и от фона за 20 мин, как у основного) и держится cushionExitMs — выходим. 0 = выкл
  cushionExitUsd: 0,
  cushionExitMs: 1000,
  maxDev: 60,             // выше — почти наверняка сбой данных, не входим
  exitDev: 2,             // выход: отставание сократилось до этого — HL догнал
  spreadMaxHoldSec: 8,    // не пошла за столько секунд — выходим в ноль (у эталона ~5–8 с)
  baselineSec: 120,       // окно, по которому считаем фон спреда
  minBinMove: 0,          // Binance должен пройти столько $ в сторону сделки (0 = не требовать)
  binMoveSec: 10,
  // Запрет для обеих стратегий: Binance за binMoveSec ушёл ПРОТИВ сделки на столько $.
  // По 159 сделкам эталона: в таких входах у него 0 плюсов, 4 минуса, итог −$1 289.
  vetoAgainstUsd: 10,

  // --- самообучение выхода ---
  // Раз в 10 минут бот переигрывает свои сделки и сделки эталона по сохранённому
  // пути цены с разными параметрами выхода и берёт лучшие — но только если они
  // лучше и на старых 70% сделок, и на свежих 30% (защита от подгонки под прошлое).
  autoTune: true,
  tuneMinSamples: 60,

  // --- эталон: бот сам забирает его сделки и прикладывает к ним состояние рынка ---
  refEnabled: true,

  // --- живая торговля на papertrade ---
  // Выключено: бот торгует на бумаге. Включено (и ключ бота разрешён кошельком):
  // заявки уходят на платформу, позиции открываются по реальной цене исполнения.
  live: false,
  // 18.10 Влад: «ровные числа должны быть обезопасены» — не входить у круглого уровня BTC ближе roundZoneUsd в любую сторону (0 = выкл.)
  roundStepUsd: 1000, roundZoneUsd: 50,
  // 20.10 Влад: «от 81 000 до 81 100 ничего не открываем, под уровнем 50» — зона НАД уровнем отдельно (0 = как под уровнем, roundZoneUsd)
  roundZoneAboveUsd: 0,
  useMainKey: true,        // торговать ключом и кошельком основного бота (свой ключ не разрешён); выкл — свой ключ и свой кошелёк
  ptWallet: '',
  refWallet: '0x2222222222222222222222222222222222222222',

  // --- «Лимитки» ---
  bookEnabled: true,
  bookUseHl: true,        // какие стаканы учитывать
  bookUseBf: true,
  bookUseBs: false,       // спот по умолчанию выключен: не факт, что эталон на него смотрит
  bookMinImb: 0.6,        // перевес у цены ±0.02%: (bid−ask)/(bid+ask) по модулю ≥
  bookMinSizeUsd: 800000, // на HL лучший уровень со стороны перевеса не меньше (у эталона 0.9–1.4M)
  bookConfirmMs: 500,     // перевес должен простоять — крупные заявки часто снимают за секунды
  bookDirection: 'follow', // follow — за перевесом (продавцы -> шорт), fade — против
  bookExitOnFlip: true,   // выйти, если перевес развернулся
  bookMaxHoldSec: 8,      // не пошла за столько секунд — выходим; пошедшую ведёт выход по Binance

  // Урезание прибыли платформой: [ход в плюс, $ на 1 BTC] -> [выплачено, $ на 1 BTC].
  // Снято из API истории сделок (adjustedPnl после комиссии) + сделки эталона на 89/96.
  // Срез зависит только от хода цены, не от маржи и плеча; комиссия — 2% от прибыли;
  // убыток считается полностью. За пределами таблицы — «ход − 2.4·√ход».
  haircut: [[0, 0], [1, 0], [2, 0.009], [3, 0.102], [4, 0.284], [5, 0.54], [6, 0.86], [7, 1.234], [8, 1.657],
    [9, 2.12], [12, 3.704], [14, 4.895], [15, 5.524], [19, 8.194], [20, 8.894], [23, 11.061], [25, 12.546],
    [29, 15.611], [31, 17.194], [44.5, 28.182], [52, 34.509], [57, 38.741], [89, 65.23], [96, 71.26], [103, 78.915]],
};
// Запас, который платформа держит до bust: замерено на 700× (bust через 0.0952%
// вместо 1/700 = 0.1429%) и 1000× (0.053% вместо 0.1%) — около 0.047% в обоих случаях.
const BUST_RESERVE = 0.00047;

const NUM = {
  roundStepUsd: [0, 100000], roundZoneUsd: [0, 1000], roundZoneAboveUsd: [0, 1000],
  margin: [1, 1e7], addMargin: [0, 1e7], leverage: [1, 1000], cooldownSec: [0, 3600], maxPositions: [1, 5], maxFeedAgeMs: [100, 10000],
  minSamples: [3, 1000], evMinUsd: [-100, 1000], evAddUsd: [-100, 1000], vetoAgainstUsd: [0, 1000],
  tuneMinSamples: [20, 10000],
  stopUsd: [0.5, 5000], beStartUsd: [0, 5000], trailStartUsd: [0, 5000], trailUsd: [0.5, 5000], takeProfitUsd: [0, 5000],
  stallSec: [0, 600], entryDelayMs: [0, 30000], exitDelayMs: [0, 30000], maxQueue: [1, 100], splitParts: [1, 20], splitMinUsd: [0, 100000], splitStartUsd: [0, 100000], binExitUsd: [0, 500],
  entryMaxWaitMs: [500, 60000], cancelMissedUsd: [0, 500], cushionExitUsd: [0, 500], cushionExitMs: [0, 60000], maxBinRange10s: [0, 5000], bookFilterImb: [0, 1], cushionMaxHoldSec: [5, 3600], binWeight: [0, 1], bookFlipProfitImb: [0, 1], bigProfitUsd: [0, 1e7], binExitBigUsd: [0, 500], bookHoldImb: [0, 1], maxDayLossUsd: [0, 1e7], maxLossStreak: [0, 50], streakPauseMin: [1, 1440], lossWindow: [0, 200], lossUsd: [0, 1e7], lossHardUsd: [0, 1e7], tgSummaryMin: [0, 1440],
  g3MaxBinMove: [0, 1000], g3MaxCushion: [0, 1000], g3MinBfBook: [0, 100],
  minDev: [0.5, 500], maxDev: [1, 5000], exitDev: [0, 500], spreadMaxHoldSec: [1, 3600], baselineSec: [10, 3600], slowBaselineMin: [5, 30],
  minBinMove: [0, 1000], binMoveSec: [1, 60],
  bookMinImb: [0.05, 1], bookMinSizeUsd: [0, 1e9], bookConfirmMs: [0, 30000], bookMaxHoldSec: [1, 3600],
  regLowUsd: [1, 5000], regHighUsd: [2, 10000], regHystPct: [0, 49], regHoldSec: [0, 3600],
};
const BOOL = ['enabled', 'spreadEnabled', 'bookEnabled', 'bookUseHl', 'bookUseBf', 'bookUseBs', 'bookExitOnFlip', 'cancelIfSignalGone', 'noAddToLoser', 'refEnabled', 'autoTune', 'live', 'sizeToOi', 'tgTrades', 'tgEnabled', 'regHaltHigh', 'cushBoth', 'useMainKey', 'noLongs', 'wallGuard', 'g3Enabled'];

function loadConfig() {
  const row = db.prepare('SELECT json FROM btcy2_lag_config WHERE id = 1').get();
  let saved = {};
  try { saved = row ? JSON.parse(row.json) : {}; } catch { saved = {}; }
  // Таблица среза — факт платформы, а не настройка: всегда берём из кода
  { const d = new Date(); d.setHours(0, 0, 0, 0); return SAFETY.lockedStrategyConfig(DEFAULTS, d.getTime()); }
}
let cfg = loadConfig();

// Лента действий бота для панели: что сделал и почему (вход, выход, отмена, отказ платформы, пропуск сигнала,
// самообучение). Последние 150 событий в памяти.
const events = [];
const logEvent = (kind, text) => { events.push({ t: Date.now(), kind, text }); if (events.length > 150) events.shift(); };

function setConfig(patch) {
  SAFETY.rejectUnsafePatch(patch);
  const next = { ...cfg };
  for (const [k, [lo, hi]] of Object.entries(NUM)) {
    if (patch[k] === undefined || patch[k] === '') continue;
    const v = Number(patch[k]);
    if (!Number.isFinite(v)) throw new Error(`${k}: не число`);
    if (v < lo || v > hi) throw new Error(`${k}: допустимо от ${lo} до ${hi}`);
    next[k] = v;
  }
  for (const k of BOOL) if (patch[k] !== undefined) next[k] = !!patch[k];
  if (next.useMainKey !== cfg.useMainKey && [...positions.values(), ...pending].some((p) => p.live)) throw new Error('есть живые позиции или заявки — сначала закрой их, потом меняй ключ');
  if (next.useMainKey) {   // на ключе основного торгуем только с его кошелька
    const mw = String(ptMain.keyStatus().master || '').toLowerCase();
    if (/^0x[0-9a-f]{40}$/.test(mw)) next.ptWallet = mw;
  }
  const keyNow = next.useMainKey ? ptMain : ptOwn;
  if (next.live && !cfg.live && !keyNow.keyStatus().active) {
    throw new Error(next.useMainKey ? 'ключ основного бота не активен — разреши его во вкладке «⚡ BTC-бот»' : 'свой ключ «вчера» ещё не разрешён кошельком — нажми «Разрешить боту торговать» или включи «торговать ключом основного»');
  }
  if (patch.ptWallet !== undefined) {
    const w = String(patch.ptWallet).trim().toLowerCase();
    if (!/^0x[0-9a-f]{40}$/.test(w)) throw new Error('кошелёк papertrade: адрес вида 0x… (40 символов)');
    if (w !== cfg.ptWallet && (positions.size || pending.length) && [...positions.values(), ...pending].some((p) => p.live)) throw new Error('есть живые позиции или заявки — сначала закрой их, потом меняй кошелёк');
    next.ptWallet = w;
    if (w !== cfg.ptWallet) { setTimeout(() => { try { pt.walletStream(w); } catch {} try { require('./my-trades.cjs').addWallet(w, 'кошелёк «бота вчера»'); } catch {} }, 0); }
  }
  if (patch.refWallet !== undefined) {
    const w = String(patch.refWallet).trim().toLowerCase();
    if (!/^0x[0-9a-f]{40}$/.test(w)) throw new Error('кошелёк эталона: адрес вида 0x… (40 символов)');
    next.refWallet = w;
  }
  if (patch.bookDirection !== undefined) {
    if (!['follow', 'fade'].includes(patch.bookDirection)) throw new Error('направление: follow или fade');
    next.bookDirection = patch.bookDirection;
  }
  if (Array.isArray(patch.haircut)) {
    const t = patch.haircut.map(([a, b]) => [Number(a), Number(b)]).filter(([a, b]) => Number.isFinite(a) && Number.isFinite(b));
    if (t.length >= 2) next.haircut = t.sort((x, y) => x[0] - y[0]);
  }
  if (next.exitDev >= next.minDev) throw new Error('порог выхода по спреду должен быть меньше порога входа');
  if (!next.bookUseHl && !next.bookUseBf && !next.bookUseBs) throw new Error('для «Лимиток» нужен хотя бы один стакан');
  db.prepare('INSERT INTO btcy2_lag_config (id, json) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET json = excluded.json')
    .run(JSON.stringify(next));
  cfg = next;
  return cfg;
}

// ---------- рыночные данные ----------
const V = ['hl', 'bf', 'bs'];
const feed = Object.fromEntries(V.map((v) => [v, { bid: NaN, ask: NaN, t: 0, bids: [], asks: [], bookT: 0, midOk: NaN, midOkT: 0 }]));
let paperMarketQuote = null;
// У HL при пустеющей стороне стакана спред разъезжается до $20–30, и середина прыгает на $10–15 на долю
// секунды (12.10 07:45:34: решение по 83979.5 при реальной цене 83961). Для решений берём последнюю
// середину с нормальным спредом, если ей не больше 2 с; иначе — что есть.
const HL_MAX_SPREAD_USD = 6;
const mid = (v) => {
  const f = feed[v];
  if (v === 'hl' && Number.isFinite(f.midOk) && Date.now() - f.midOkT <= 2000) return f.midOk;
  return f.bid > 0 && f.ask > 0 ? (f.bid + f.ask) / 2 : NaN;
};
const age = (v) => (feed[v].t ? Date.now() - feed[v].t : Infinity);
const lvl = (arr) => (arr || []).slice(0, 20).map((x) => (Array.isArray(x) ? [+x[0], +x[1]] : [+x.px, +x.sz]));

// Соединение с автопереподключением. Если поток замолчал на 10 с — рвём и идём
// на следующий адрес из списка: Binance переносит потоки между путями без предупреждения.
// 14.10 (перенос из основного бота): старый сторож только закрывал зависший сокет и ждал onclose, который
// не приходил — 14.10 01:40 поток HL у этого бота молчал 45 с и не поднимался. Теперь при тишине >10 с
// всегда создаём НОВОЕ соединение, старое выбрасываем (его события игнорируются по номеру поколения).
function stream(urls, onMsg, { onOpen = null, ping = null } = {}) {
  let i = 0, ws = null, last = 0, pingT = null, gen = 0, silentSince = 0;
  const host = (() => { try { return new URL(urls[0]).host; } catch { return urls[0]; } })();
  const connect = () => {
    const g = ++gen;
    if (pingT) { clearInterval(pingT); pingT = null; }
    const old = ws; ws = null;
    if (old) { try { old.onmessage = null; old.onclose = null; old.onerror = null; old.close(); } catch {} }
    let s;
    try { s = new WS(urls[i % urls.length]); } catch { setTimeout(() => { if (g === gen) connect(); }, 3000); return; }
    ws = s; last = Date.now();
    s.onopen = () => {
      if (g !== gen) return;
      last = Date.now();
      if (silentSince) { console.log(`[btc-вчера2] поток ${host} снова на связи после ${Math.round((Date.now() - silentSince) / 1000)} с тишины`); silentSince = 0; }
      if (onOpen) onOpen(s);
      if (ping) pingT = setInterval(() => { try { s.send(ping); } catch {} }, 30000);
    };
    s.onmessage = (e) => {
      if (g !== gen) return;
      last = Date.now();
      let m;
      try { m = JSON.parse(typeof e.data === 'string' ? e.data : e.data.toString()); } catch { return; }
      try { onMsg(m); } catch {}
    };
    s.onerror = () => {};
    s.onclose = () => { if (g !== gen) return; i++; setTimeout(() => { if (g === gen) connect(); }, 1500); };
  };
  setInterval(() => {
    if (Date.now() - last > 10000) {
      if (!silentSince) { silentSince = last || Date.now(); console.log(`[btc-вчера2] поток ${host} молчит — переподключаю`); }
      i++; connect();
    }
  }, 5000);
  connect();
}

function connectFeeds() {
  stream(['wss://fstream.binance.com/public/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker', 'wss://fstream.binance.com/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker'], (m) => {
    if (marketData.binance('bf', m) !== 'BTC') return;
    const d = m.data || m;
    if (d.e === 'bookTicker') { feed.bf.bid = +d.b; feed.bf.ask = +d.a; feed.bf.t = Date.now(); }
  });
  stream(['wss://fstream.binance.com/public/stream?streams=btcusdt@depth20@100ms/ethusdt@depth20@100ms', 'wss://fstream.binance.com/stream?streams=btcusdt@depth20@100ms/ethusdt@depth20@100ms'], (m) => {
    if (marketData.binance('bf', m) !== 'BTC') return;
    const d = m.data || m;
    if (d.e === 'depthUpdate') { feed.bf.bids = lvl(d.b); feed.bf.asks = lvl(d.a); feed.bf.bookT = Date.now(); }
  });
  stream(['wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@depth20@100ms/ethusdt@bookTicker/ethusdt@depth20@100ms'], (m) => {
    if (marketData.binance('bs', m) !== 'BTC') return;
    const d = m.data;
    if (!d || !m.stream) return;
    if (m.stream.endsWith('@bookTicker')) { feed.bs.bid = +d.b; feed.bs.ask = +d.a; feed.bs.t = Date.now(); }
    else { feed.bs.bids = lvl(d.bids); feed.bs.asks = lvl(d.asks); feed.bs.bookT = Date.now(); }
  });
  stream(['wss://api.hyperliquid.xyz/ws'], (m) => {
    if (marketData.hyperliquid(m) !== 'BTC') return;
    if (m.channel === 'bbo' && m.data) {
      const [b, a] = m.data.bbo || [];
      if (b && a) {
        feed.hl.bid = +b.px; feed.hl.ask = +a.px; feed.hl.t = Date.now();
        if (feed.hl.ask - feed.hl.bid <= HL_MAX_SPREAD_USD) { feed.hl.midOk = (feed.hl.bid + feed.hl.ask) / 2; feed.hl.midOkT = feed.hl.t; }
        paperMarketQuote = paperQuote(m.data);
      }
    } else if (m.channel === 'l2Book' && m.data) {
      const [bids, asks] = m.data.levels || [];
      feed.hl.bids = lvl(bids); feed.hl.asks = lvl(asks); feed.hl.bookT = Date.now();
    }
  }, {
    onOpen: (ws) => {
      for (const coin of ['BTC', 'ETH']) for (const type of ['bbo', 'l2Book']) { const s = { type, coin };
        ws.send(JSON.stringify({ method: 'subscribe', subscription: s }));
      }
    },
    ping: '{"method":"ping"}',   // HL рвёт тихие соединения через минуту
  });
}

// Перевес у цены в полосе ±0.02% от mid: +1 — одни покупатели, −1 — одни продавцы.
// По записи именно эта полоса лучше всего предсказывает ход HL на 2–10 с.
function bookStat(v) {
  const f = feed[v], m = mid(v);
  // HL присылает стакан только при изменениях, поэтому ему допускаем больший возраст
  const maxAge = v === 'hl' ? 10000 : 3000;
  if (!Number.isFinite(m) || !f.bids.length || !f.asks.length || Date.now() - f.bookT > maxAge) return null;
  const lo = m * (1 - 0.0002), hi = m * (1 + 0.0002);
  let b = 0, a = 0;
  for (const [p, q] of f.bids) if (p >= lo) b += p * q;
  for (const [p, q] of f.asks) if (p <= hi) a += p * q;
  return {
    imb: b + a > 0 ? (b - a) / (b + a) : 0,
    l1b: f.bids[0][0] * f.bids[0][1],     // лучший уровень на покупку, $
    l1a: f.asks[0][0] * f.asks[0][1],     // лучший уровень на продажу, $
  };
}

// ---------- спред и фон ----------
const sBuf = [];           // [t, спред] раз в 100 мс
const bfBuf = [];          // [t, mid Binance Fut] за последние 60 с
const chart = [];          // раз в секунду для графика в панели
let baseline = NaN, lastBaseline = 0, lastChart = 0, lastSnap = 0;
let baselineSlow = NaN, lastBaselineSlow = 0;   // медленный фон для cushBoth
let baselineCx = NaN, lastBaselineCx = 0;       // фон за 20 мин для выхода «подушка против» (как у основного)
const mbuf = [];           // [t, hl, bf, откл, перевес×3, верх стакана×3] раз в 250 мс, 30 минут

function median(xs) {
  if (!xs.length) return NaN;
  const s = [...xs].sort((a, b) => a - b);
  return s[s.length >> 1];
}
function bfAgo(sec) {
  const t = Date.now() - sec * 1000;
  for (let i = bfBuf.length - 1; i >= 0; i--) if (bfBuf[i][0] <= t) return bfBuf[i][1];
  return NaN;
}

// ---------- урезание прибыли платформой ----------
function paidPerBtc(fav) {
  if (!(fav > 0)) return fav;                      // убыток платформа не урезает
  const t = cfg.haircut;
  for (let k = 1; k < t.length; k++) {
    if (fav <= t[k][0]) {
      const [x0, y0] = t[k - 1], [x1, y1] = t[k];
      return Math.max(0, y0 + ((fav - x0) / (x1 - x0)) * (y1 - y0));
    }
  }
  return Math.max(0, fav - 2.4 * Math.sqrt(fav));
}

// ---------- позиции ----------
const positions = new Map();   // id -> открытая позиция
const lastEntry = { spread: 0, book: 0 };

const insQ = db.prepare(`INSERT INTO btcy2_lag_trades
  (opened_at, strategy, side, entry, qty, margin, leverage, dev, spread, baseline, bin_move, imb, l1, sig, live, position_id, intent_id, status, max_fav, max_adv)
  VALUES (@opened_at, @strategy, @side, @entry, @qty, @margin, @leverage, @dev, @spread, @baseline, @bin_move, @imb, @l1, @sig, @live, @position_id, @intent_id, 'open', 0, 0)`);
const closeQ = db.prepare(`UPDATE btcy2_lag_trades SET closed_at=@closed_at, exit=@exit, reason=@reason,
  gross_pnl=@gross_pnl, est_pnl=@est_pnl, max_fav=@max_fav, max_adv=@max_adv, status='closed' WHERE id=@id`);

const r1 = (x) => (Number.isFinite(x) ? Math.round(x * 10) / 10 : null);

function openPosition(strategy, side, ctx) {
  // В живом режиме цена и размер — из реального исполнения на платформе
  const fill = ctx.fill || null;
  const previous = fill ? executionJournal.find(String(fill.positionId)) : null;
  if (previous && previous.status === 'closed') return null; // stale wallet state must never resurrect a terminal execution
  const managed = fill && [...positions.values()].find(p => String(p.positionId) === String(fill.positionId));
  if (managed) return managed;
  const entry = fill ? fill.entry : mid('hl');
  const margin = fill ? fill.margin : cfg.margin;
  const leverage = fill ? fill.leverage : cfg.leverage;
  const p = {
    opened_at: fill && fill.openedAt ? fill.openedAt : Date.now(), strategy, side, entry, qty: (margin * leverage) / entry,
    margin, leverage,
    dev: r1(ctx.dev), spread: r1(ctx.spread), baseline: r1(ctx.baseline), bin_move: r1(ctx.binMove),
    sig: Number.isFinite(ctx.sig) ? Math.round(ctx.sig * 1000) / 1000 : null,
    imb: JSON.stringify((ctx.books || []).map((s) => (s ? Math.round(s.imb * 100) : null))),
    l1: JSON.stringify((ctx.books || []).map((s) => (s ? [Math.round(s.l1b), Math.round(s.l1a)] : null))),
    live: fill ? 1 : 0, position_id: fill ? String(fill.positionId) : null, intent_id: fill ? String(fill.intentId || '').toLowerCase() : null,
  };
  p.id = previous ? previous.id : Number(insQ.run(p).lastInsertRowid);
  if (previous) p.closing = executionJournal.closing(previous);
  if (fill) {
    p.live = true; p.positionId = String(fill.positionId); p.intentId = String(fill.intentId || '').toLowerCase(); p.group = fill.group || null;
    // Сколько ждали исполнения входа и на сколько цена ушла за это время (плюс = вошли хуже, чем решали)
    if (fill.decidedAt && !previous) {
      const dir = side === 'L' ? 1 : -1;
      const info = { decidedAt: fill.decidedAt, decidedPx: fill.decidedPx, filledAt: p.opened_at, entry,
        queueMs: p.opened_at - fill.decidedAt, slipUsd: Number.isFinite(fill.decidedPx) ? r1(dir * (entry - fill.decidedPx)) : null,
        rank0: fill.rank0 ?? null, rank: fill.rank ?? null,
        signalGone: !!fill.signalGone, cancelFailed: fill.cancelFailed || null, cancelRequestedAt: fill.cancelRequestedAt || null,
        cancelTries: fill.cancelTries || 0, invalidatedReason: fill.invalidatedReason || null };
      db.prepare('UPDATE btcy2_lag_trades SET entry_info = ? WHERE id = ?').run(JSON.stringify(info), p.id);
      console.log(`[btc-вчера2]   вход #${p.id}: очередь ${info.queueMs} мс${info.rank0 != null ? ` (место ${info.rank0}→${info.rank})` : ''}, вошли на ${info.slipUsd}$ ${info.slipUsd > 0 ? 'хуже' : 'лучше'}, чем решали`);
    }
  }
  p.maxFav = previous ? previous.max_fav || 0 : 0; p.maxAdv = previous ? previous.max_adv || 0 : 0;
  // лучший уровень Binance в сторону позиции с момента входа — от него считаем откат
  p.bBest = (side === 'L' ? 1 : -1) * mid('bf');
  // bust: позиция сгорает целиком, если цена ушла против на (1/плечо − запас)
  p.bustUsd = fill && fill.bust ? Math.abs(fill.bust - entry) : entry * Math.max(0, 1 / leverage - BUST_RESERVE);
  positions.set(p.id, p);
  lastEntry[strategy] = Date.now();
  console.log(`[btc-вчера2] ВХОД${p.live ? ' LIVE' : ''} ${strategy === 'book' ? 'лимитки' : 'спред'} ${side === 'S' ? 'ШОРТ' : 'ЛОНГ'} #${p.id} @${entry}${p.live ? ` позиция ${p.positionId}` : ''}`);
  logEvent('open', `вход #${p.id} ${side === 'S' ? 'ШОРТ' : 'ЛОНГ'} @${entry} · ${strategy === 'book' ? 'лимитки' : 'спред'} · маржа $${margin}${positions.size > 1 ? ' · дополнительная' : ''}`
    + (ctx.dev != null ? ` · подушка ${r1(ctx.dev)}$` : '') + (ctx.binMove != null ? `, Binance ${r1(ctx.binMove)}$` : ''));
  if (fill && (fill.invalidatedReason || !cfg.live || !cfg.enabled || SAFETY.cutoffReached())) {
    if (!p.closing) p.closing = { why: fill.invalidatedReason ? `запоздалый вход: ${fill.invalidatedReason}${fill.cancelFailed ? `; отмена ${fill.cancelFailed}` : ''}` : SAFETY.cutoffReached() ? 'таймер: позднее исполнение входа' : 'вход исполнился после остановки', decidedAt: Date.now(), decidedPx: mid('hl') };
    sendClose(p).catch(e => console.error('[btc-вчера2] late-fill close:', e.message));
  }
  return p;
}

function closePosition(p, reason, price = mid('hl'), filledAt = null) {
  if (!positions.has(p.id)) return null;
  const evidence = p.live ? closePx.get(String(p.positionId)) : null;
  if (p.live && !evidence) return null; // neither HTTP acceptance nor wallet absence proves a fill
  if (evidence) { price = evidence.px; filledAt = evidence.ts; reason = p.closing ? p.closing.why : reason; }
  const dir = p.side === 'L' ? 1 : -1;
  const exit = Number.isFinite(price) ? price : p.entry;
  const fav = dir * (exit - p.entry);
  let gross = fav * p.qty, est = paidPerBtc(fav) * p.qty;
  if (reason === 'ликвидация') { gross = -p.margin; est = -p.margin; }
  closeQ.run({
    id: p.id, closed_at: filledAt || Date.now(), exit, reason,
    gross_pnl: Math.round(gross * 100) / 100, est_pnl: Math.round(est * 100) / 100,
    max_fav: r1(p.maxFav), max_adv: r1(p.maxAdv),
  });
  if (p.live) { executionJournal.evidence(p.id, evidence); pt.confirmClosed(p.positionId, evidence); }
  positions.delete(p.id);
  console.log(`[btc-вчера2] ВЫХОД #${p.id} @${exit} (${reason}) ход ${fav.toFixed(1)}$ → ${est.toFixed(0)}$`);
  logEvent(fav > 0 ? 'close-win' : fav < 0 ? 'close-loss' : 'close', `выход #${p.id} @${exit} · ${reason} · ход ${fav >= 0 ? '+' : ''}${fav.toFixed(1)}$ ≈ ${est >= 0 ? '+' : ''}${est.toFixed(0)}$`
    + (p.closing && p.closing.decidedAt ? ` · очередь ${((Date.now() - p.closing.decidedAt) / 1000).toFixed(1)} с` : ''));
  if (p.live && cfg.tgTrades) tgAlert(`${est > 0 ? '🟢' : est < 0 ? '🔴' : '⚪'} #${p.id} ${p.side === 'S' ? 'ШОРТ' : 'ЛОНГ'} ${p.entry} → ${exit} · ${reason} · ход ${fav >= 0 ? '+' : ''}${fav.toFixed(1)}$ ≈ ${est >= 0 ? '+' : ''}${est.toFixed(0)}$ · маржа $${p.margin}`);
  // Живой выход: сколько ушло на решение → заявку → исполнение в очереди платформы
  // и сколько $ хода потеряно за это время. Нужно, чтобы правила выхода учитывали реальную очередь.
  const cl = p.closing && p.closing.decidedAt ? p.closing : p.prevClose;   // prevClose — решение о выходе до «ложного закрытия»
  if (p.live && cl && cl.decidedAt) {
    const c = cl, done = filledAt || Date.now();
    const info = { why: reason, intentId: c.intentId || null, decidedAt: c.decidedAt, decidedPx: c.decidedPx, sentAt: c.sent || null, filledAt: done, exit,
      lostUsd: Number.isFinite(c.decidedPx) ? r1(dir * (c.decidedPx - exit)) : null, rank0: c.rank0 ?? null, rank: c.rank ?? null };
    db.prepare('UPDATE btcy2_lag_trades SET exit_info = ? WHERE id = ?').run(JSON.stringify(info), p.id);
    console.log(`[btc-вчера2]   выход #${p.id}: решение→заявка ${c.sent ? c.sent - c.decidedAt : '?'} мс, заявка→исполнение ${c.sent ? done - c.sent : '?'} мс${info.rank0 != null ? ` (место в очереди ${info.rank0}→${info.rank})` : ''}, за это время цена ушла на ${info.lostUsd}$`);
  }
  return { id: p.id, exit, reason, gross, est };
}

// Заявки в очереди на вход считаются наравне с открытыми позициями — иначе
// за 3 секунды ожидания бот наставил бы в очередь лишних заявок.
const pending = [];
const countOpen = (strategy) => [...positions.values(), ...pending].filter((p) => p.strategy === strategy).length;
const hasOpposite = (strategy, side) => [...positions.values(), ...pending].some((p) => p.strategy === strategy && p.side !== side);

const cancelQ = db.prepare(`INSERT INTO btcy2_lag_trades
  (opened_at, closed_at, strategy, side, entry, qty, margin, leverage, dev, spread, baseline, reason, status, gross_pnl, est_pnl, live)
  VALUES (@opened_at, @closed_at, @strategy, @side, @entry, 0, @margin, @leverage, @dev, @spread, @baseline, @reason, 'cancelled', 0, 0, @live)`);

// 15.10 ~00:00 Влад: «эфир можно лонг на этих тестовых, проверишь потом с лонгами и без». Лонг, прошедший все проверки,
// но пропущенный из-за «только шорты», запоминаем — лаборатория откроет по нему лонг по эфиру. Не чаще раза в 20 с.
const skippedLongs = []; let skippedSeq = 0, skippedLastAt = 0;
function noteSkippedLong(strategy) {
  const now = Date.now(); if (now - skippedLastAt < 20000) return; skippedLastAt = now;
  skippedLongs.push({ id: ++skippedSeq, t: now, strategy }); if (skippedLongs.length > 100) skippedLongs.shift();
}
const skippedSince = (id) => skippedLongs.filter((x) => x.id > id);
// круглый уровень рядом (18.10): цена HL ближе roundZoneUsd к кратному roundStepUsd — причина отказа, иначе null
function roundBlock(px) {
  const s = cfg.roundStepUsd, z = cfg.roundZoneUsd, za = cfg.roundZoneAboveUsd > 0 ? cfg.roundZoneAboveUsd : z;   // za — зона над уровнем
  if (!(s > 0 && (z > 0 || za > 0) && px > 0)) return null;
  const lv = Math.round(px / s) * s, off = px - lv, d = Math.abs(off), zone = off >= 0 ? za : z;
  return d < zone ? `круглый уровень ${lv}: цена ${off >= 0 ? 'над' : 'под'} ним в ${d.toFixed(0)}$ (зона −${z}$ / +${za}$) — не вхожу` : null;
}
function queueEntry(strategy, side, ctx) {
  if (cfg.live && (!executionSync.ready || Date.now() - executionSync.t > 30000)) { lastDecision[strategy] = { t: Date.now(), ok: false, why: 'LIVE: синхронизация истории исполнений' }; return null; }
  const now = Date.now();
  { const rb = !(ctx && ctx.hedge) && roundBlock(mid('hl')); if (rb) { lastDecision[strategy] = { t: now, ok: false, sig: ctx && ctx.sig, open: positions.size + pending.length, why: rb }; return; } }
  // 🧪 три правила основного (по статистике 13–15.10: ночью шорты 144 сд/15 стопов/+2.7K → 80 сд/1 стоп/+3.3K)
  if (cfg.g3Enabled && ctx && !ctx.hedge) {
    const bm = Number(ctx.binMove), dv = Math.abs(Number(ctx.dev)), bf = ctx.books && ctx.books[1] ? (side === 'L' ? 1 : -1) * ctx.books[1].imb * 100 : NaN;
    let why3 = null;
    if (cfg.g3MaxBinMove > 0 && bm >= cfg.g3MaxBinMove) why3 = `поздний вход: Binance до входа уже прошёл ${Math.round(bm)}$ (порог ${cfg.g3MaxBinMove}$)`;
    else if (cfg.g3MaxCushion > 0 && dv >= cfg.g3MaxCushion) why3 = `подушка ${dv.toFixed(1)}$ — это рывок, а не отставание (порог ${cfg.g3MaxCushion}$)`;
    else if (strategy === 'spread' && cfg.g3MinBfBook > 0 && !(bf >= cfg.g3MinBfBook)) why3 = `стакан фьюча за нас ${Number.isFinite(bf) ? Math.round(bf) + '%' : 'нет данных'} (нужно от ${cfg.g3MinBfBook}%)`;
    if (why3) { lastDecision[strategy] = { t: now, ok: false, sig: ctx.sig, open: positions.size + pending.length, why: why3 }; return; }
  }
  if (cfg.noLongs && side === 'L') { noteSkippedLong(strategy); lastDecision[strategy] = { t: now, ok: false, sig: ctx && ctx.sig, open: positions.size + pending.length, why: 'лонги запрещены (настройка «только шорты») — жду шорт' }; return; }
  // первая позиция — основная маржа, дополнительная (пока есть открытая/в очереди) — маржа добавки
  // (0 = такая же, как первая, как у эталона). Дальше размер режется под запас OI и свободный баланс.
  const spC = splitCont(side);
  const base = spC ? spC.base : (positions.size + pending.length > 0 ? (cfg.addMargin > 0 ? cfg.addMargin : cfg.margin) : cfg.margin);
  const margin = sizeFor(side, base);
  if (margin < base) logEventThrottled('size', `${side === 'L' ? 'лонг' : 'шорт'}: вхожу частью — $${margin} из $${base} (запас OI / баланс)`);
  // платформа не принимает позицию дешевле minimumOpenNotional ($10 000): при 700× это маржа от $14.29
  const minNotional = Number.isFinite(oi.minNotional) && oi.minNotional > 0 ? oi.minNotional : 10000;
  if (margin * cfg.leverage < minNotional) {
    lastDecision[strategy] = { t: now, ok: false, sig: ctx.sig, open: positions.size + pending.length,
      why: `маржа × плечо = $${Math.round(margin * cfg.leverage)} меньше минимума платформы $${minNotional} — нужна маржа от $${Math.ceil((minNotional / cfg.leverage) * 100) / 100}` };
    return null;
  }
  // упрёмся в потолок OI — заявку отклонят, а секунды уйдут; не ставим
  if (oiBlocked(side, (margin * cfg.leverage) / mid('hl')) || Date.now() - oiReject[side] < OI_REJECT_PAUSE_MS) {
    lastDecision[strategy] = { t: now, ok: false, why: `сторона ${side === 'L' ? 'лонг' : 'шорт'} упёрлась в потолок OI платформы`, sig: ctx.sig, open: countOpen(strategy) };
    // 19.10 Влад: «подключить эфир прямо в этом»: без лесенки (только полная маржа) биток не пустил — тот же вход отдаём эфирной ветке («Спред 2»)
    if (!(cfg.splitParts > 1) && !(ctx && ctx.hedge)) ethWant = { side, t: now, margin: base, lev: cfg.leverage };
    return null;
  }
  // 19.10 Влад: «только полная маржа — всё, что я поставил»: без лесенки (splitParts ≤ 1) живой вход урезанным не делаем —
  // свободной маржи на кошельке меньше настройки (например, её держит хедж) → пропуск
  if (liveReady() && !(cfg.splitParts > 1) && margin < base - 0.01) {
    lastDecision[strategy] = { t: now, ok: false, sig: ctx.sig, open: positions.size + pending.length, why: `на кошельке свободно меньше полной маржи: вышло бы $${margin} из $${base} — пропускаю (только полная маржа)` };
    return null;
  }
  lastEntry[strategy] = now;
  if (liveReady()) {
    // 17.10 Влад: «не надо изначально дробить — постепенно: сначала вся маржа; не пускает OI — дроби; пускает на больше — заходим,
    // но не больше моей маржи». Лесенка: 1 × вся маржа → отказ → 2 × ½ → 4 × ¼ … до splitParts частей; пустило часть — добираем остаток
    // частями того же размера, пустило всё — следующие крупнее (×2). Повтор — сразу после ответа платформы (пауза 0). Части — одна группа.
    if (cfg.splitParts > 1) {
      const pk = spC || (split[side] = { base, filled: 0, parts: 0, group: 'g' + now, until: now + 20000, partSize: cfg.splitStartUsd > 0 ? Math.min(base, cfg.splitStartUsd) : base, inflight: 0, acc: 0, rej: 0, t0: now, strategy, ctx });
      packFire(side, pk);
      return null;
    }
    return liveEntry(strategy, side, ctx, margin);
  }
  if (cfg.live) { lastDecision[strategy] = { t: now, ok: false, sig: ctx.sig, open: positions.size + pending.length, why: 'LIVE: ключ или кошелёк ещё не готовы' }; return null; }
  // 📝 Бумага (перенесено из основного бота 14.10, Влад: «надо чтобы бот вчера тоже на бумаге торговал и давал
  // статистику»): та же сделка без платформы — вход и выход через реальные задержки очереди (медианы последних
  // живых сделок основного бота), отмена не работает, как и в жизни. Правила входа/выхода — как 12.10 в обед.
  const wait = paperEntryWait();
  const q = { strategy, side, ctx, sig: ctx.sig, margin, createdAt: now, fillAt: now + wait, px: mid('hl'), paper: true };
  pending.push(q);
  lastDecision[strategy] = { t: now, ok: true, sig: ctx.sig, open: positions.size + pending.length, why: `📝 бумага: вхожу ${side === 'L' ? 'ЛОНГ' : 'ШОРТ'}, исполнение через ~${(wait / 1000).toFixed(1)} с` };
  logEvent('open', `📝 бумага: решил войти ${side === 'S' ? 'ШОРТ' : 'ЛОНГ'} (${strategy === 'book' ? 'лимитки' : 'спред'}) · подушка ${r1(ctx.dev)}$, Binance ${r1(ctx.binMove)}$ · исполнится через ~${(wait / 1000).toFixed(1)} с`);
  return q;
}

// ============================================================
// ЖИВОЕ ИСПОЛНЕНИЕ
// Вход: заявка уходит на платформу и встаёт в очередь. Бот смотрит место в очереди
// и отменяет, если впереди больше maxQueue или пока стояли — пропал сигнал.
// Исполнение ловим по открытым позициям кошелька: у позиции есть id заявки.
// Выход: заявка на закрытие НИКОГДА не отменяется и не дублируется — ждёт своей очереди.
// ============================================================
// поток кошелька молчит больше 2 минут — платформа или связь легли, новых входов не делаем
const liveReady = () => cfg.live && pt.keyStatus().active && pt.wallet.t > 0 && Date.now() - pt.wallet.t < 120000;

// Размер входа как у эталона: берём сколько влезает. Режем под запас OI стороны (по данным платформы,
// с запасом 20% — наши единицы неточные), под свободный баланс, и вдвое после каждого отказа по OI
// (oiSize), пока платформа не примет; принятая заявка возвращает полный размер.
const oiSize = { L: 1, S: 1 };
// 17.10 Влад: «если не пускает OI на нужную маржу — дробит до трёх поз (1000, 1000, 500 — всё, что пролезло), но только на 3;
// закрываться всё вместе». splitParts — сколько частей максимум на один сигнал (1 = выкл). Пачка живёт 20 с от первой части,
// добор — только пока сигнал есть (decideEntry), той же стороной, без заявок в очереди.
const split = { L: null, S: null };
const PACK_PUSH_MS = 3000, PACK_FILL_MS = 8000;   // 17.10 ~15:15 Влад: «пусть газуют» — зашёл хоть кусок → доливаем до 8 с   // 17.10 ~04:55 Влад: «3 с долбить биток, потом сразу 3 с эфир» (было 10 → 5 → 3)
const minPart = (pk) => Math.min(pk.base, cfg.splitMinUsd > 0 ? cfg.splitMinUsd : pk.base / cfg.splitParts);   // самый мелкий кусок лесенки   // сколько лесенка долбит сама, без нового решения (17.10: было 5 с)
// 17.10 Влад: «если не получается зайти в биток — заходят в эфир». Лесенка дошла до самых мелких частей (10%) и платформа всё равно
// не пустила → ethWant: «Спред 2» (общая эфирная ветка ботов) открывает тот же вход в ETH. Сам бот эфир не открывает.
let ethWant = null;
function splitCont(side) {
  const sp = side ? split[side] : null;
  if (!(cfg.splitParts > 1) || !sp) return null;
  if (Date.now() > sp.until || sp.parts >= cfg.splitParts || sp.filled >= sp.base * 0.98) { if (!sp.inflight) split[side] = null; return null; }
  if (sp.inflight > 0 || [...positions.values(), ...pending].some((p) => p.side !== side)) return null;
  return sp;
}
// попытка лесенки закончилась (все ответы пришли): никого не пустило — части вдвое мельче; всех пустило — крупнее; остаток кончился — пачка собрана
// отправить очередную ступеньку лесенки: сколько частей текущего размера влезает в остаток (не больше splitParts всего)
function packFire(side, pk) {
  if (!liveReady() || split[side] !== pk) return;
  const minNotional = Number.isFinite(oi.minNotional) && oi.minNotional > 0 ? oi.minNotional : 10000;
  const rest = pk.base - pk.filled; let size = Math.floor(Math.min(pk.partSize, rest) * 100) / 100;
  // 17.10 ~02:35: 10 заявок разом платформа режет «rate_limited» (429) — за раз не больше 4, на долбёжке (pk.hammer) по 2;
  // на долбёжке часть не больше свободного места OI по данным платформы (×0.9), но не меньше минимума позиции — «зайти хоть на что-то»
  if (pk.hammer && !(cfg.splitMinUsd > 0)) { const room = oiHeadroom(side), px = mid('hl'); if (Number.isFinite(room) && room > 0 && px > 0) size = Math.min(size, Math.max(Math.ceil((minNotional / cfg.leverage) * 100) / 100, Math.floor(((room * 0.9 * px) / cfg.leverage) * 100) / 100)); }
  if (!(size * cfg.leverage >= minNotional)) { split[side] = null; return; }
  const n = Math.max(1, Math.min(pk.hammer ? 2 : 4, Math.floor(rest / size + 1e-6), cfg.splitParts - pk.parts));
  for (let i = 0; i < n; i++) { pk.inflight++; liveEntry(pk.strategy, side, pk.ctx, size, pk.base, pk); }
  if (n > 1 || pk.parts > 0) logEventThrottled('size', `${side === 'L' ? 'лонг' : 'шорт'}: дроблю — ${n} × $${size} (зашло $${Math.round(pk.filled)} из $${pk.base})`, 2000);
}
function packStep(side, pk) {
  if (pk.inflight > 0) return;
  const rest = pk.base - pk.filled;
  // 17.10 Влад: «подними хотя бы до 10 секунд» — на самых мелких частях долбим дальше, пока не пройдёт 10 с от решения (пауза только на rate_limited);
  // не пустило за 10 с — отдаём вход эфирной ветке
  if (pk.acc === 0 && pk.partSize <= minPart(pk) + 1e-6 && rest >= 1 && pk.parts < cfg.splitParts && split[side] === pk && pk.t0 && Date.now() - pk.t0 < (pk.filled > 0 ? PACK_FILL_MS : PACK_PUSH_MS)) { pk.rej = 0; pk.hammer = true; setTimeout(() => packFire(side, pk), Math.max(150, oiReject[side] - Date.now())); return; }
  if (pk.acc === 0 && pk.partSize <= minPart(pk) + 1e-6 && rest >= 1) { ethWant = { side, t: Date.now(), margin: Math.round(rest * 100) / 100, lev: cfg.leverage }; logEvent('size', `${side === 'L' ? 'лонг' : 'шорт'}: в BTC не пустило даже по $${Math.round(minPart(pk))} — отдаю вход эфирной ветке ($${Math.round(rest)})`); if (split[side] === pk) split[side] = null; return; }
  // 17.10 ~02:45 Влад: отказ → кусок меньше (вся → ⅔ → ½ → ½ … до splitMinUsd), пустило весь кусок → следующий ×1.6 (500 → 800), не больше остатка
  if (pk.acc === 0) pk.partSize = Math.max(minPart(pk), pk.partSize >= pk.base - 1e-6 ? (pk.base * 2) / 3 : pk.partSize / 2);
  else if (pk.rej === 0) pk.partSize = Math.min(Math.max(rest, 0), pk.partSize * 1.6);
  pk.acc = 0; pk.rej = 0;
  if (rest < 1 || pk.parts >= cfg.splitParts) { if (split[side] === pk) split[side] = null; return; }
  // 17.10 Влад: «очень быстро отправлять, чтобы сигнал не пропадал» — следующая ступенька сразу, без нового решения, 5 с от первого решения
  if (split[side] === pk && pk.t0 && Date.now() - pk.t0 < (pk.filled > 0 ? PACK_FILL_MS : PACK_PUSH_MS)) setTimeout(() => packFire(side, pk), Math.max(0, oiReject[side] - Date.now()));
}
// части одной пачки закрываются вместе: у одной сработал выход — закрываем остальные той же причиной
function closeGroupMates() {
  for (const p of positions.values()) {
    if (!p.group || !p.closing) continue;
    for (const o of positions.values()) if (o !== p && o.group === p.group && !o.closing) {
      o.closing = { why: String(p.closing.why || '') + ' (вся пачка)', decidedAt: Date.now(), decidedPx: mid('hl') };
      if (o.live) sendClose(o); else closePosition(o, o.closing.why);
    }
  }
}
function sizeFor(side, base) {
  if (Date.now() - oiReject[side] > 10000) oiSize[side] = 1;   // 10 с без отказов — снова полный размер
  let m = base;
  if (cfg.sizeToOi) {   // выключено — размер всегда полный, под OI не режем (пользователь: «пока одна позиция, без дробления»)
    m = base * oiSize[side];
    const head = oiHeadroom(side), px = mid('hl');
    if (Number.isFinite(head) && head > 0 && Number.isFinite(px)) m = Math.min(m, (head * 0.8 * px) / cfg.leverage);
  }
  const bal = pt.wallet.available;
  if (!Number.isFinite(bal)) return 0;
  // Available already excludes the platform's locked margin; subtract only not-yet-visible own pending exposure.
  if (bal >= 0) m = Math.min(m, Math.max(0, bal - pending.reduce((s,q) => s + (q.margin || 0), 0)) * 0.95);
  return Math.max(0, Math.floor(m * 100) / 100);
}
const throttled = {};
function logEventThrottled(kind, text, ms = 5000) {
  const now = Date.now();
  if (throttled[kind] && now - throttled[kind] < ms) return;
  throttled[kind] = now; logEvent(kind, text);
}

function liveEntry(strategy, side, ctx, margin = cfg.margin, target = margin, spC = null) {
  const q = { strategy, side, ctx, sig: ctx.sig, margin, createdAt: Date.now(), fillAt: Infinity, px: mid('hl'), live: true, intentId: null, rank: null };
  pending.push(q);
  pt.open({ isLong: side === 'L', margin, leverage: cfg.leverage }, { onPrepared: ({ intentId }) => { q.intentId = intentId; } }).then((res) => {
    if (!pending.includes(q)) return;
    if (res.uncertain) { q.intentId = res.intentId || q.intentId; q.signalGone = true; q.transportUnknown = true; logEvent('guard', 'ответ на вход потерян: слот удерживается, проверяю исполнение/отмену по тому же intent'); return; }
    if (!res.ok) {
      const code = (res.data && (res.data.code || res.data.error || res.data.message)) || `HTTP ${res.status}`;
      // Потолок OI: место освобождается, когда кто-то закрылся, и забирает его тот, кто долбится чаще.
      // Отказ ничего не стоит (заявка не создаётся) — пробуем снова через OI_REJECT_PAUSE_MS, в журнал не пишем,
      // только считаем, иначе таблица сделок утонет в отказах.
      const rateLimited = res.status === 429 || (res.data && res.data.code === 'rate_limited');
      if (rateLimited || (res.data && res.data.reason === 'openInterest')) {
        // rate_limited — платформа просит сбавить темп: пауза 1 с вместо 300 мс
        oiReject[side] = Date.now() + (rateLimited ? 2000 - OI_REJECT_PAUSE_MS : 0); oiRejects[side]++;
        if (!rateLimited) oiSize[side] = Math.max(0.05, oiSize[side] / 2);   // следующая попытка — вдвое меньше
        if (spC) { spC.inflight = Math.max(0, spC.inflight - 1); spC.rej++; packStep(side, spC); }
        if (rateLimited) { console.log('[btc-вчера2] платформа ограничила частоту заявок (rate_limited) — пауза 2 с'); logEvent('reject', 'платформа: rate_limited — пауза 2 с'); }
        else if (oiRejects[side] % 20 === 1) {
          console.log(`[btc-вчера2] потолок OI (${side === 'L' ? 'лонг' : 'шорт'}): отказов ${oiRejects[side]}, долбимся дальше`);
          logEvent('reject', `потолок OI по ${side === 'L' ? 'лонгам' : 'шортам'}: платформа отклонила (всего ${oiRejects[side]}), долблюсь дальше каждые 300 мс`);
        }
        const i = pending.indexOf(q); if (i >= 0) pending.splice(i, 1);
        return;
      }
      console.log(`[btc-вчера2] платформа отклонила вход: ${JSON.stringify(res.data).slice(0, 200)}`);
      if (spC) { spC.inflight = Math.max(0, spC.inflight - 1); spC.rej++; packStep(side, spC); }
      return cancelPending(q, `платформа отклонила: ${String(code).slice(0, 60)}`);
    }
    q.intentId = res.intentId; q.sentMs = res.ms;
    if (spC) {   // часть лесенки принята
      spC.inflight = Math.max(0, spC.inflight - 1); spC.filled += margin; spC.parts++; spC.acc++; q.group = spC.group;
      logEvent('size', `пачка: зашло $${margin}, всего $${Math.round(spC.filled)} из $${spC.base} (${spC.parts} поз.)`);
      packStep(side, spC);
    } else oiSize[side] = 1;   // приняли — следующий вход снова полным размером
  }).catch((e) => { if (spC) { spC.inflight = Math.max(0, spC.inflight - 1); spC.rej++; packStep(side, spC); } if (pending.includes(q)) cancelPending(q, `ошибка отправки: ${e.message.slice(0, 60)}`); });
  return null;
}

// Раз в 300 мс: место в очереди, отмена, исполнение
let liveBusy = false, liveBusyAt = 0, liveStage = '';
async function liveTick() {
  try { closeGroupMates(); } catch {}
  if (liveBusy) {
    if (Date.now() - liveBusyAt < 20000) return;
    console.log(`[btc-вчера2] живой цикл висел ${Math.round((Date.now() - liveBusyAt) / 1000)} с на шаге «${liveStage}» — запускаю заново`);
    logEvent('guard', `живой цикл висел ${Math.round((Date.now() - liveBusyAt) / 1000)} с («${liveStage}») — перезапустил цикл`);
  }
  liveBusy = true; liveBusyAt = Date.now(); liveStage = 'начало';
  try {
    const live = pending.filter((q) => q.live && q.intentId);
    const closing = [...positions.values()].filter((p) => p.live && p.closing && p.closing.intentId);
    let ranks = null;
    if (live.length || closing.length) { liveStage = 'место в очереди'; try { ranks = await pt.queueRank(); } catch {} }
    // место заявки на закрытие в очереди: первое и текущее — чтобы понять, от чего зависит скорость выхода
    if (ranks) for (const p of closing) {
      const r = ranks[p.closing.intentId];
      if (r != null) { if (p.closing.rank0 == null) p.closing.rank0 = r; p.closing.rank = r; }
    }
    const openById = new Map(pt.wallet.openPositions.map((p) => [String(p.intentId || '').toLowerCase(), p]));
    // Запасной путь, если id заявки в позиции не совпадёт: новая позиция той же стороны
    // и маржи, открытая после отправки и ещё никем не занятая.
    const taken = new Set([...positions.values()].filter((p) => p.live && p.positionId != null).map((p) => String(p.positionId)));   // 17.10: строки — иначе число и строка одного id «не совпадали»
    // наше открытие из публичного потока: та же сторона, тот же размер, после отправки заявки
    // чьи позиции: по снимку кошелька (номер заявки) и общему реестру ботов на этом кошельке
    const walletIntent = new Map(pt.wallet.openPositions.map((w) => [String(w.positionId), String(w.intentId || '').toLowerCase()]));
    const owned = global.__ptOwned || (global.__ptOwned = new Map());
    const notMine = (pid, q) => { const wi = walletIntent.get(String(pid)); if (wi && q && q.intentId && wi !== String(q.intentId).toLowerCase()) return true; const o = owned.get(String(pid)); return !!(o && o !== __filename); };
    const fallback = (q) => {
      const margin = q.margin || cfg.margin, notional = margin * cfg.leverage;
      const o = myOpens.find((x) => !closePx.has(String(x.positionId)) && !taken.has(String(x.positionId)) && !notMine(x.positionId, q) && x.isLong === (q.side === 'L')
        && Math.abs(x.notional - notional) <= notional * 0.01 + 1 && x.ts >= q.createdAt - 3000);
      if (!o) return null;
      return { positionId: o.positionId, entry: o.entry, margin, leverage: o.leverage || cfg.leverage,
        bust: o.entry * (1 + (q.side === 'L' ? -1 : 1) * Math.max(0, 1 / cfg.leverage - BUST_RESERVE)), openedAt: o.ts };
    };
    for (const q of live) {
      let filled = openById.get(q.intentId.toLowerCase()); if (filled && taken.has(String(filled.positionId))) filled = null;   // позиция уже у другого куска
      filled = filled || fallback(q);
      if (filled) { taken.add(String(filled.positionId)); owned.set(String(filled.positionId), __filename); if (owned.size > 3000) owned.delete(owned.keys().next().value); }
      if (filled) {                                         // исполнилась — открываем по реальной цене
        pending.splice(pending.indexOf(q), 1);
        openPosition(q.strategy, q.side, { ...q.ctx, fill: buildLiveFill(q, filled) });
        continue;
      }
      if (ranks && ranks[q.intentId] != null) { q.rank = ranks[q.intentId]; q.missingSince = null; if (q.rank0 == null) q.rank0 = q.rank; }
      else if (ranks) q.missingSince = q.missingSince || Date.now();
      // Как у основного (12.10): заявка пропала из очереди платформы или висит больше 2 минут — снимаем с учёта,
      // иначе занимает слот навсегда (16.10 05:11 «вчера 2» LIVE: отмена «поздно», 9 мин без единого входа).
      // В cancelledLive она остаётся: если всё же исполнится — подхватим.
      if ((q.missingSince && Date.now() - q.missingSince > 10000) || Date.now() - q.createdAt > 120000) {
        cancelPending(q, q.missingSince ? 'заявка исчезла из очереди платформы' : 'не исполнилась за 2 мин — снята с учёта');
        continue;
      }
      // Влад 16.10: отмену долбим на каждом тике (~300 мс), пока не пройдёт; на rate_limited — пауза 1 с (ключ общий с основным)
      if (q.cancelling && Date.now() >= (q.cancelRetryAt || 0)) q.cancelling = false;
      if (q.cancelling) continue;
      const tooFar = q.rank != null && q.rank > cfg.maxQueue;
      const tooLong = Date.now() - q.createdAt > cfg.entryMaxWaitMs;
      if (tooFar || q.signalGone || q.missed || tooLong) {
        q.cancelling = true; q.cancelTries = (q.cancelTries || 0) + 1;
        const why = tooFar ? `в очереди ${q.rank}-й, больше ${cfg.maxQueue}` : q.missed ? `HL уже прошёл ${cfg.cancelMissedUsd}$ без нас`
          : q.signalGone ? 'сигнал пропал в очереди' : `не исполнилась за ${(cfg.entryMaxWaitMs / 1000).toFixed(1)} с`;
        q.cancelReason = why; q.cancelRequestedAt = q.cancelRequestedAt || Date.now();
        liveStage = 'отмена входа'; const r = await pt.cancel(q.intentId).catch((e) => ({ ok: false, data: { code: e.message } }));
        if (r.ok || q.cancelTries === 1 || q.cancelTries % 20 === 0) console.log(`[btc-вчера2] отмена входа (${why}), попытка ${q.cancelTries}, через ${Date.now() - q.createdAt} мс: ${r.ok ? 'принята' : `отказ ${JSON.stringify(r.data || r.status).slice(0, 120)}`}`);
        if (r.ok) cancelPending(q, q.cancelTries > 1 ? `${why} (отмена прошла с ${q.cancelTries}-й попытки)` : why);
        else {
          // 409 = заявка уже исполняется — отменить нельзя; пробуем дальше и ждём позицию
          q.cancelFailed = (r.data && r.data.code) || `HTTP ${r.status}`;
          const rl = r.status === 429 || (r.data && r.data.code === 'rate_limited');
          q.cancelRetryAt = Date.now() + (rl ? 1000 : 0);
        }
      }
    }
    // Отменённая заявка могла всё же исполниться (отмена принята, когда вход уже шёл) —
    // такую позицию берём в работу, иначе она повисла бы на кошельке без присмотра
    for (const [id, c] of cancelledLive) {
      if (Date.now() - c.t > 120000) { cancelledLive.delete(id); continue; }
      let filled = openById.get(id); if (filled && taken.has(String(filled.positionId))) filled = null;
      filled = filled || fallback(c.q);
      if (!filled) continue;
      cancelledLive.delete(id); taken.add(String(filled.positionId)); owned.set(String(filled.positionId), __filename);
      console.log(`[btc-вчера2] заявка на вход была отменена, но исполнилась — веду позицию ${filled.positionId}`);
      openPosition(c.q.strategy, c.q.side, { ...c.q.ctx, fill: { ...buildLiveFill(c.q, filled), invalidatedReason: c.q.cancelReason || 'вход исполнился после отмены' } });
    }
    // Only a close event or confirmed account history releases the position slot.
    for (const p of [...positions.values()]) {
      if (!p.live) continue;
      const c = closePx.get(String(p.positionId));
      if (c) {
        closePosition(p, p.closing ? p.closing.why : 'закрытие подтверждено платформой', c.px, c.ts);
        continue;
      }
      if (p.closing && ranks && ranks[p.closing.intentId] != null) {
        p.closing.sent = p.closing.sent || Date.now(); p.closing.uncertain = false;
        executionJournal.saveClosing(p);
      }
      if (p.closing && !p.closing.sent && !p.closing.sending) { liveStage = 'закрытие'; await sendClose(p); }
    }
    // A stale wallet snapshot must NEVER reopen a terminal row. Missed closes are reconciled from history.
  } finally { liveBusy = false; }
}
// Заявка на закрытие уходит сразу из тика, не дожидаясь liveTick (это экономило бы ~1 с
// из 7–8 с пути до исполнения), и только один раз: повторяем лишь если платформа её не приняла.
async function sendClose(p) {
  if (!p.live || !p.closing || p.closing.sent || p.closing.sending || Date.now() < (p.closing.retryAt || 0)) return;
  const cl = p.closing;
  cl.sending = true;
  executionJournal.saveClosing(p);
  const r = await pt.close([String(p.positionId)], { request: cl.request || null, onPrepared: ({ message, intentId }) => {
    cl.request = message; cl.intentId = intentId;
    executionJournal.saveClosing(p);
  } }).catch(() => ({ ok: false, data: { code: 'close_prepare_failed' } }));
  cl.sending = false;
  if (r.ok) { cl.sent = Date.now(); cl.uncertain = false; }
  else if (r.data && r.data.reason === 'noOpenPositions') {
    cl.sent = Date.now(); cl.alreadyClosed = true;
  } else {
    cl.uncertain = !!r.uncertain; cl.retryAt = Date.now() + 5000; cl.fails = (cl.fails || 0) + 1;
    if (cl.fails === 1 || cl.fails % 20 === 0) logEvent('guard', `закрытие #${p.id}: ответ не подтверждён; сверяю очередь и повторяю тот же intent`);
  }
  executionJournal.saveClosing(p);
}
// Публичный поток сделок — главный источник правды о наших исполнениях:
// он приходит мгновенно, а состояние кошелька обновляется с задержкой.
const closePx = new Map();   // позиция -> цена выхода
const myOpens = [];          // наши открытия: {positionId, isLong, entry, notional, ts}
pt.onTrade((t) => {
  if (!cfg.ptWallet || String(t.wallet).toLowerCase() !== cfg.ptWallet || t.instrumentId !== 0) return;
  if (t.kind === 'open' && t.positionId != null && Number.isFinite(t.entry)) {
    myOpens.push({ positionId: String(t.positionId), isLong: t.side === 'long', entry: t.entry, notional: t.notionalUsd, ts: t.ts, leverage: t.leverage });
    if (myOpens.length > 200) myOpens.shift();
  }
  if (t.kind === 'close' && t.positionId != null && Number.isFinite(t.exit)) {
    closePx.set(String(t.positionId), { px: t.exit, ts: Number(t.ts), source: 'public-trades' });
    if (closePx.size > 500) closePx.delete(closePx.keys().next().value);
  }
});

// Recover missed public fills from this wallet's history; absence is not a close proof.
const executionSync = { ready: false, t: 0, err: null, busy: false, retryAt: 0 };
async function livePnlSync() {
  if (!cfg.ptWallet || executionSync.busy || Date.now() < executionSync.retryAt) return;
  executionSync.busy = true;
  try {
    const r = await fetch(`https://exchange.papertrade.xyz/query/accounts/${cfg.ptWallet}/trades`, { headers: UA, signal: AbortSignal.timeout(8000) });
    if (!r.ok) { executionSync.retryAt = Date.now() + (r.status === 429 ? 60000 : 5000); throw new Error(`history HTTP ${r.status}`); }
    const j = await r.json();
    if (!Array.isArray(j.items)) throw new Error('Invalid account history');
    for (const x of j.items) {
      const c = parseCloseTrade(x); if (!c) continue;
      const id = String(x.positionId);
      closePx.set(id, c); pt.confirmClosed(id, c);
      const row = executionJournal.find(id);
      if (!row || row.status !== 'closed') continue;
      db.prepare('UPDATE btcy2_lag_trades SET real_pnl=?,est_pnl=?,exit=?,closed_at=?,close_evidence=? WHERE id=?')
        .run(c.realPnl, c.realPnl, c.px, c.ts, JSON.stringify(c), row.id);
    }
    executionSync.ready = true; executionSync.t = Date.now(); executionSync.err = null;
  } catch (e) { executionSync.err = e.message; executionSync.retryAt = Math.max(executionSync.retryAt, Date.now()+5000); }
  finally { executionSync.busy = false; }
}
// Explicit user START only; never resets protection state or changes exposure.
function startTrading() {
  const now = Date.now();
  const reasons = startBlockers({ now, margin: cfg.margin, available: pt.wallet.available,
    cutoffReached: SAFETY.cutoffReached(now), highWaterTripped: guard.tripped || SAFETY.highWaterTrippedToday(dayStart()),
    pausedUntil: guard.pausedUntil, keyActive: pt.keyStatus().active,
    walletFresh: pt.wallet.t > 0 && now - pt.wallet.t < 120000,
    marketFresh: age('hl') <= cfg.maxFeedAgeMs && age('bf') <= cfg.maxFeedAgeMs,
    executionFresh: executionSync.ready && now - executionSync.t <= 30000,
    positionCount: positions.size + orphans.size, pendingCount: pending.length + cancelledLive.size,
    walletPositions: pt.wallet.openPositions.length, walletOperations: pt.wallet.operations.length });
  if (reasons.length) return { ok: false, reasons };
  SAFETY.setManualStop(false);
  cfg.enabled = true; cfg.live = true;
  logEvent('control', 'Пользователь включил LIVE; параметры и защиты сохранены');
  return { ok: true, LIVE: true, message: 'LIVE включён; входы только по сигналам и действующим защитам.' };
}

// Аварийная остановка: бот выключен, все входы в очереди отменяются, все позиции закрываются
async function killSwitch() {
  cfg.live = false; cfg.enabled = false;
  try { SAFETY.setManualStop(true); } catch { console.error('[btc-вчера2] manual stop persistence failed; trading remains OFF'); }
  try { setConfig({ live: false, enabled: false }); } catch {}
  const out = { cancelled: 0, closing: 0 };
  for (const q of pending.filter((x) => x.live && x.intentId)) {
    const r = await pt.cancel(q.intentId).catch(() => ({ ok: false }));
    if (r.ok) { cancelPending(q, 'аварийная остановка'); out.cancelled++; }
  }
  // Include managed fills even when the wallet snapshot has not caught up yet.
  for (const p of positions.values()) if (p.live) {
    if (!p.closing) p.closing = { why: 'аварийная остановка', decidedAt: Date.now(), decidedPx: mid('hl') };
    await sendClose(p); out.closing++;
  }
  console.log(`[btc-вчера2] АВАРИЙНАЯ ОСТАНОВКА: отменено ${out.cancelled}, на закрытие ${out.closing}`);
  return out;
}

// ---------- решатель: входить ли и добавлять ли позицию ----------
// Количество позиций не задаётся числом: бот смотрит на свои же закрытые сделки
// с сигналом той же силы и решает, приносит ли такой вход деньги после среза
// платформы. Пока статистики мало — учится, держа одну позицию.
const EDGES = { spread: [0, 10, 15, 20, 30, Infinity], book: [0, 0.6, 0.75, 0.9, 1.01] };
function bucketOf(strategy, sig) {
  const e = EDGES[strategy];
  if (!Number.isFinite(sig)) return null;
  for (let i = 0; i < e.length - 1; i++) {
    if (sig >= e[i] && sig < e[i + 1]) return `${e[i]}–${e[i + 1] === Infinity ? '∞' : e[i + 1] > 1 && strategy === 'book' ? 1 : e[i + 1]}`;
  }
  return null;
}
let evCache = { t: 0, data: {} };
// Окно решателя: иначе плохая серия блокирует корзину навсегда — без новых сделок статистика не меняется
const EV_WINDOW_MS = 3 * 3600e3;
function evStats() {
  if (Date.now() - evCache.t < 30000) return evCache.data;
  const rows = db.prepare(`SELECT strategy, sig, est_pnl, qty FROM btcy2_lag_trades
    WHERE status = 'closed' AND live = 1 AND sig IS NOT NULL AND qty > 0 AND reason != 'перезапуск сервера' AND opened_at >= ?
    ORDER BY id DESC LIMIT 3000`).all(Date.now() - EV_WINDOW_MS);   // только реальные сделки за последние часы
  const data = {};
  for (const r of rows) {
    const s = r.strategy || 'spread', b = bucketOf(s, r.sig);
    if (!b) continue;
    const d = data[`${s}|${b}`] || (data[`${s}|${b}`] = { strategy: s, bucket: b, n: 0, sum: 0, wins: 0 });
    if (d.n >= 100) continue;          // только последние 100: рынок меняется
    const perBtc = r.est_pnl / r.qty;  // на 1 BTC — сравнимо при любой марже
    d.n++; d.sum += perBtc; if (perBtc > 0) d.wins++;
  }
  for (const d of Object.values(data)) { d.ev = d.sum / d.n; d.winRate = d.wins / d.n; }
  evCache = { t: Date.now(), data };
  return data;
}
const lastDecision = { spread: null, book: null };
// Порог отката Binance для позиции: обычный, а при прибыли ≥ bigProfitUsd (в деньгах, без среза) — широкий
function binExitFor(p, fav) {
  const profitUsd = fav * p.qty;
  return cfg.bigProfitUsd > 0 && profitUsd >= cfg.bigProfitUsd ? cfg.binExitBigUsd : cfg.binExitUsd;
}

// Первая позиция — на cfg.margin, дополнительная (пока открыта первая) — на cfg.addMargin и только
// в ту же сторону, когда открытая не в минусе. Эталон докупает пачками по 3; нам хватит 2.
// ---- режим волатильности: размах Binance за 5 мин по mbuf, раз в секунду ----
const regimeMod = require('./regime');
let regime = null, regR5m = NaN;
function regimeEngine() {
  const o = { low: cfg.regLowUsd, high: cfg.regHighUsd, hystPct: cfg.regHystPct, holdMs: cfg.regHoldSec * 1000 };
  if (!regime || regime.opts.low !== o.low || regime.opts.high !== o.high || regime.opts.hystPct !== o.hystPct || regime.opts.holdMs !== o.holdMs) { try { regime = regimeMod.createRegime(o); } catch { return null; } }
  return regime;
}
function regimeTick() {
  const now = Date.now(); let hi = -Infinity, lo = Infinity, n = 0;
  for (let i = mbuf.length - 1; i >= 0 && mbuf[i][0] >= now - 300000; i--) { const v = mbuf[i][2]; if (!Number.isFinite(v)) continue; if (v > hi) hi = v; if (v < lo) lo = v; n++; }
  regR5m = n >= 200 ? hi - lo : NaN;   // минимум ~50 с истории
  const R = regimeEngine(); if (!R) return;
  const u = R.update(regR5m, now);
  if (u.changed) { const m = `режим ${u.regime} (размах за 5 мин ~${Math.round(regR5m)}$)${u.regime === 'HIGH' && cfg.regHaltHigh ? ' — входы остановлены, позиции довожу' : ''}`; logEvent('vol', m); console.log('[btc-вчера2] ' + m); }
}
// 15.10 ~23:55: прогрев после старта — первые 5 минут входов нет (с 21:00 сервер перезапускали 17 раз; на холодных данных
// «вчера» потерял −1.2K за 62 сделки, после прогрева заработал +2.2K за 71)
const STARTED_AT = Date.now(); let WARMUP_MS = 90000;   // 16.10: было 5 мин; с тёплым стартом (запись рынка моложе 3 мин) — 60 с
function decideEntry(strategy, sig, side) {
  const all = [...positions.values(), ...pending];
  if (Date.now() - STARTED_AT < WARMUP_MS) { lastDecision[strategy] = { t: Date.now(), ok: false, why: `прогрев после старта: входы через ${Math.ceil((WARMUP_MS - (Date.now() - STARTED_AT)) / 1000)} с`, sig, open: all.length }; return false; }
  { const fgx = FG.noEntry(); if (fgx) { lastDecision[strategy] = { t: Date.now(), ok: false, why: fgx, sig, open: all.length }; return false; } }
  // 19.10: стена — только против этой стороны (снимок основного: wallBlockL / wallBlockS; у основного включено wallAgainstOnly)
  { const why = SAFETY.walls.reason(side); if (why) { lastDecision[strategy] = { t: Date.now(), ok: false, why: `стена: ${why}`, sig, open: all.length }; return false; } }
  { const why = SAFETY.tradingWindowReason(); if (why) { lastDecision[strategy] = { t: Date.now(), ok: false, why, sig, open: all.length }; return false; } }
  if (cfg.regHaltHigh && regime && regime.regime === 'HIGH') { lastDecision[strategy] = { t: Date.now(), ok: false, why: `режим HIGH (размах за 5 мин ~${Math.round(regR5m)}$, порог ${cfg.regHighUsd}$) — не торгую`, sig, open: all.length }; return false; }
  // «только шорты» проверяется в queueEntry, уже после всех проверок: там же запоминаются пропущенные лонги (для зеркала по эфиру)
  const st = evStats()[`${strategy}|${bucketOf(strategy, sig)}`];
  const known = !!st && st.n >= cfg.minSamples;
  const hl = mid('hl');
  const fav = (p) => (p.entry == null || !Number.isFinite(hl) ? 0 : (p.side === 'L' ? 1 : -1) * (hl - p.entry));
  const f = (x) => `${x >= 0 ? '+' : ''}${x.toFixed(1)}$`;
  let ok = true, why;
  // потолок — на весь бот (обе стратегии вместе), включая заявки в очереди
  const spD = splitCont(side);
  if (spD) why = `добор пачки: часть ${spD.parts + 1} из ${cfg.splitParts}, зашло $${Math.round(spD.filled)} из $${spD.base}`;
  else if (all.length >= cfg.maxPositions) { ok = false; why = `потолок позиций ${cfg.maxPositions} (открыто/в очереди ${all.length})`; }
  else if (!all.length) {
    if (known && st.ev < cfg.evMinUsd) { ok = false; why = `такой сигнал в среднем ${f(st.ev)} на BTC за ${st.n} сделок — пропускаю`; }
    else why = known ? `сигнал в среднем ${f(st.ev)} на BTC, плюс в ${Math.round(st.winRate * 100)}%` : `учусь: по сигналу такой силы ${st ? st.n : 0} из ${cfg.minSamples} сделок`;
  } else if (side && all.some((p) => p.side !== side)) { ok = false; why = 'открыта позиция в другую сторону — не хеджируюсь'; }
  else if (known && st.ev < cfg.evAddUsd) { ok = false; why = `для добавки нужно ≥ ${cfg.evAddUsd}$ на BTC, сигнал даёт ${f(st.ev)}`; }
  else if (cfg.noAddToLoser && all.some((p) => fav(p) < 0)) { ok = false; why = 'открытая позиция в минусе — не усредняюсь'; }
  else why = `дополнительная позиция на $${cfg.addMargin}: открытая не в минусе`;
  lastDecision[strategy] = { t: Date.now(), ok, why, sig, open: all.length };
  return ok;
}
// Сила перевеса — средний модуль по включённым стаканам
// Взвешенный перевес включённых стаканов: Binance (фьючерс и спот) с весом binWeight, HL — с остатком.
// NaN, если ни одного включённого стакана нет.
function bookWeighted(books, f) {
  const use = { hl: cfg.bookUseHl, bf: cfg.bookUseBf, bs: cfg.bookUseBs };
  const w = { hl: 1 - cfg.binWeight, bf: cfg.binWeight, bs: cfg.binWeight };
  let sum = 0, ws = 0;
  V.forEach((v, k) => { if (use[v] && books[k] && w[v] > 0) { sum += w[v] * f(books[k].imb); ws += w[v]; } });
  return ws > 0 ? sum / ws : NaN;
}
const bookStrength = (books) => bookWeighted(books, (imb) => Math.abs(imb));
// Перевес в сторону сделки: +1 — у цены одни покупатели (за лонг), −1 — одни продавцы
const bookFavor = (books, side) => bookWeighted(books, (imb) => (side === 'L' ? 1 : -1) * imb);
const cancelledLive = new Map();   // intentId -> {q, t}: отменённые живые заявки, за которыми ещё следим
function cancelPending(q, reason) {
  const i = pending.indexOf(q);
  if (i >= 0) pending.splice(i, 1);
  if (q.live && q.intentId) cancelledLive.set(q.intentId.toLowerCase(), { q, t: Date.now() });
  logEvent('cancel', `вход ${q.side === 'S' ? 'ШОРТ' : 'ЛОНГ'} (${q.strategy === 'book' ? 'лимитки' : 'спред'}) отменён: ${reason}`);
  cancelQ.run({
    opened_at: q.createdAt, closed_at: Date.now(), strategy: q.strategy, side: q.side, entry: q.px,
    margin: q.margin || cfg.margin, leverage: cfg.leverage, dev: r1(q.ctx.dev), spread: r1(q.ctx.spread), baseline: r1(q.ctx.baseline), reason,
    live: q.live ? 1 : 0,
  });
}

// ---------- сигнал «Лимиток» ----------
// Перевес считается, только если все включённые стаканы смотрят в одну сторону
// с силой ≥ порога, а на HL у цены стоит крупная заявка со стороны перевеса.
let bookSig = { side: null, since: 0 };
function bookSignal(books) {
  const use = { hl: cfg.bookUseHl, bf: cfg.bookUseBf, bs: cfg.bookUseBs };
  const picked = V.map((v, k) => (use[v] ? books[k] : undefined)).filter((x) => x !== undefined);
  if (!picked.length || picked.some((s) => !s)) return null;
  // Перевес считаем взвешенно (Binance binWeight, HL — остаток), а не «каждый стакан по отдельности»:
  // Binance решает, HL подтверждает
  const w = bookWeighted(books, (imb) => imb);
  let side = null;
  if (w >= cfg.bookMinImb) side = 'L';        // покупатели
  else if (w <= -cfg.bookMinImb) side = 'S';  // продавцы
  if (!side) return null;
  const hl = books[0];
  if (cfg.bookMinSizeUsd > 0) {
    if (!hl) return null;
    const wall = side === 'L' ? hl.l1b : hl.l1a;
    if (wall < cfg.bookMinSizeUsd) return null;
  }
  return side;
}

// ---------- главный цикл (10 раз в секунду) ----------
function tick() {
  const now = Date.now();
  const hl = mid('hl'), bf = mid('bf');
  if (Number.isFinite(bf)) {
    bfBuf.push([now, bf]);
    while (bfBuf.length && bfBuf[0][0] < now - 60000) bfBuf.shift();
  }
  const fresh = age('hl') <= cfg.maxFeedAgeMs && age('bf') <= cfg.maxFeedAgeMs;
  let spread = NaN;
  if (fresh && Number.isFinite(hl) && Number.isFinite(bf)) {
    spread = hl - bf;
    sBuf.push([now, spread]);
  }
  while (sBuf.length && sBuf[0][0] < now - cfg.baselineSec * 1000) sBuf.shift();
  // фон пересчитываем раз в секунду и только по прошлому
  if (now - lastBaseline >= 1000) {
    lastBaseline = now;
    baseline = sBuf.length >= 300 ? median(sBuf.map((x) => x[1])) : NaN;   // минимум 30 с истории
  }
  const dev = spread - baseline;
  // медленный фон (cushBoth): медиана спреда за slowBaselineMin минут по записи рынка, раз в 5 с
  if (cfg.cushBoth && now - lastBaselineSlow >= 5000) {
    lastBaselineSlow = now;
    const xs = []; for (let i = mbuf.length - 1; i >= 0 && mbuf[i][0] >= now - (cfg.slowBaselineMin || 30) * 60000; i--) xs.push(mbuf[i][1] - mbuf[i][2]);
    baselineSlow = xs.length >= 240 ? median(xs) : NaN;
  }
  if (cfg.cushionExitUsd > 0 && now - lastBaselineCx >= 5000) {
    lastBaselineCx = now;
    const xs = []; for (let i = mbuf.length - 1; i >= 0 && mbuf[i][0] >= now - 20 * 60000; i--) xs.push(mbuf[i][1] - mbuf[i][2]);
    baselineCx = xs.length >= 240 ? median(xs) : NaN;
  }
  const books = V.map(bookStat);

  // сигнал «Лимиток» и сколько он уже держится
  const bs = bookSignal(books);
  const bookW = bookWeighted(books, (imb) => imb);   // взвешенный перевес: + покупатели, − продавцы
  const rawSide = bs ? (cfg.bookDirection === 'fade' ? (bs === 'L' ? 'S' : 'L') : bs) : null;
  if (rawSide !== bookSig.side) bookSig = { side: rawSide, since: now };

  if (now - lastChart >= 1000 && Number.isFinite(hl)) {
    lastChart = now;
    chart.push([now, hl, bf, r1(dev), books[0] ? Math.round(books[0].imb * 100) : null]);
    if (chart.length > 600) chart.shift();
  }
  // Буфер рынка на 30 минут раз в 250 мс: когда через API придёт закрытая сделка
  // эталона, по нему восстанавливаем, что было на рынке перед его входом.
  if (now - lastSnap >= 250 && Number.isFinite(hl) && Number.isFinite(bf)) {
    lastSnap = now;
    mbuf.push([now, hl, bf, Number.isFinite(dev) ? r1(dev) : null,
      books.map((b) => (b ? Math.round(b.imb * 100) : null)),
      books.map((b) => (b ? [Math.round(b.l1b), Math.round(b.l1a)] : null))]);
    while (mbuf.length && mbuf[0][0] < now - 30 * 60000) mbuf.shift();
  }

  // --- заявки на вход, стоящие в очереди платформы ---
  for (const q of [...pending]) {
    const dir = q.side === 'L' ? 1 : -1;
    const gone = q.strategy === 'spread'
      ? !(Number.isFinite(dev) && dir * -dev >= cfg.minDev / 2)   // отставание исчезло или развернулось
      : bookSig.side !== q.side;                                   // перевес пропал
    if (q.live) {   // живую отменяет liveTick
      if (cfg.cancelIfSignalGone && gone) q.signalGone = true;
      // ход уже случился, пока заявка стояла в очереди — входить поздно
      if (cfg.cancelMissedUsd > 0 && Number.isFinite(hl) && Number.isFinite(q.px) && dir * (hl - q.px) >= cfg.cancelMissedUsd) q.missed = true;
      continue;
    }
    // 19.10 Влад: «все должны входить так, как в реальности»: бумага снимает заявку по тем же правилам, что живой бот
    // (пропал сигнал / HL прошёл N$ / вышло время ожидания), но отмена успевает, только если до исполнения больше PAPER_CANCEL_MS — иначе исполнится, как в жизни
    if (gone && !q.signalGone) q.signalGone = true;
    if (!q.cancelWhy && q.fillAt > now) {
      const why = cfg.cancelIfSignalGone && gone ? 'сигнал пропал' : cfg.cancelMissedUsd > 0 && Number.isFinite(hl) && Number.isFinite(q.px) && dir * (hl - q.px) >= cfg.cancelMissedUsd ? `HL прошёл ${cfg.cancelMissedUsd}$ без нас` : now - q.createdAt >= cfg.entryMaxWaitMs ? `не исполнилась за ${(cfg.entryMaxWaitMs / 1000).toFixed(0)} с` : null;
      if (why) {
        q.cancelWhy = why;
        if (q.fillAt - now >= PAPER_CANCEL_MS) { pending.splice(pending.indexOf(q), 1); paperCancels.ok++; paperCancels[why.startsWith('сигнал') ? 'gone' : why.startsWith('HL') ? 'missed' : 'timeout']++; logEvent('cancel', `📝 бумага: заявка ${q.side === 'S' ? 'ШОРТ' : 'ЛОНГ'} снята в очереди — ${why} (как вживую)`); continue; }
        paperCancels.late++;   // поздно: заявка уже на подходе — исполнится
      }
    }
    if (now >= q.fillAt && Number.isFinite(hl)) {
      pending.splice(pending.indexOf(q), 1);
      const p = openPosition(q.strategy, q.side, { ...q.ctx, queuedMs: now - q.createdAt });
      if (p) { const dir = q.side === 'L' ? 1 : -1; db.prepare('UPDATE btcy2_lag_trades SET entry_info = ? WHERE id = ?').run(JSON.stringify({ decidedAt: q.createdAt, decidedPx: q.px, filledAt: p.opened_at, entry: p.entry, queueMs: p.opened_at - q.createdAt, slipUsd: Number.isFinite(q.px) ? r1(dir * (p.entry - q.px)) : null, paper: true, signalGone: !!q.signalGone }), p.id); }
    }
  }

  // --- сопровождение позиций (по цене HL — по ней платформа и считает) ---
  if (Number.isFinite(hl)) {
    for (const p of [...positions.values()]) {
      const dir = p.side === 'L' ? 1 : -1;
      const fav = dir * (hl - p.entry);
      if (fav > p.maxFav) { p.maxFav = fav; p.maxFavAt = now; }
      if (fav < p.maxAdv) p.maxAdv = fav;
      // Ликвидацию платформа делает сама и сразу; остальное — заявка на выход в очередь,
      // и закрывает она по цене момента исполнения, а не решения.
      // живую позицию ликвидирует и закрывает сама платформа — тут только бумага
      if (!p.live && p.bustUsd > 0 && -fav >= p.bustUsd) { closePosition(p, 'ликвидация', hl); continue; }
      if (p.closing) { if (!p.live && now >= p.closing.at) closePosition(p, p.closing.why, hl); continue; }
      const held = (now - p.opened_at) / 1000;
      const maxHold = p.strategy === 'book' ? cfg.bookMaxHoldSec : cfg.spreadMaxHoldSec;
      // «пошла» = позиция хоть раз была в плюсе на величину безубытка
      const going = p.maxFav >= Math.max(cfg.beStartUsd, 1);
      if (Number.isFinite(bf)) p.bBest = Number.isFinite(p.bBest) ? Math.max(p.bBest, dir * bf) : dir * bf;
      const bBack = Number.isFinite(bf) && Number.isFinite(p.bBest) ? p.bBest - dir * bf : 0;
      let why = null;
      // подушка против: худшая из «от фона 2 мин» и «от фона 20 мин»; должна продержаться cushionExitMs подряд
      let cushBad = false;
      if (cfg.cushionExitUsd > 0 && Number.isFinite(dev)) {
        const c = Math.min(dir * -dev, Number.isFinite(baselineCx) ? dir * -(spread - baselineCx) : dir * -dev);
        if (c <= -cfg.cushionExitUsd) { if (!p.cushionBadSince) p.cushionBadSince = now; cushBad = now - p.cushionBadSince >= (cfg.cushionExitMs || 0); }
        else p.cushionBadSince = 0;
      }
      if (FG.mustClose()) why = FG.CLOSE_REASON;
      else if (-fav >= cfg.stopUsd) why = 'стоп';
      else if (cushBad) why = `подушка против (HL ${p.side === 'S' ? 'ниже' : 'выше'} Binance на ${cfg.cushionExitUsd}$+)`;
      // Binance развернулся — HL догонит его через доли секунды, закрываем, пока HL на пике
      // В большой прибыли (в деньгах, bigProfitUsd) откат Binance терпим шире — чтобы трендовые ходы
      // ступеньками (12.10 08:53–08:57, +309$ за 3.5 мин) не резались на первой ступеньке
      else if (going && binExitFor(p, fav) > 0 && bBack >= binExitFor(p, fav)) why = 'Binance развернулся';
      else if (cfg.takeProfitUsd > 0 && fav >= cfg.takeProfitUsd) why = 'тейк';
      else if (trailingHit(cfg, p.maxFav, fav)) why = 'разворот (трейлинг)';
      else if (cfg.beStartUsd > 0 && going && fav <= 0) why = 'безубыток';
      else if (going && cfg.stallSec > 0 && fav > 0 && now - (p.maxFavAt || p.opened_at) >= cfg.stallSec * 1000) why = 'движение встало';
      // Сигнал пропал — выходим только пока позиция не в хорошем плюсе:
      // идущую прибыль ведут стоп/трейлинг, а не исходный сигнал.
      // Схлопывание спреда само по себе — не повод выходить (exitDev = 0 → выключено): платформа считает по HL,
      // и пока HL не пошёл против, минуса нет. Выход — по стопу, трейлингу, развороту Binance или крупных заявок.
      else if (!going && p.strategy === 'spread' && cfg.exitDev > 0 && Number.isFinite(dev) && Math.abs(dev) <= cfg.exitDev) why = 'HL догнал';
      // Крупные заявки в стаканах развернулись против позиции — для любой стратегии и в любом состоянии
      // (раньше только пока позиция не была в плюсе: 12.10 08:31 шорт стоял против 2.5M покупателей на HL
      // и 2.3M на Binance, бот ждал стоп/трейлинг, закрыл руками пользователь)
      // Пока позиция не пошла — обычный порог (bookMinImb). В плюсе — только очень жёсткий разворот
      // (bookFlipProfitImb, «2 миллиона против 30 тысяч»): 12.10 08:53 выход по перевесу 69% на +41$
      // выбросил из хода, который дошёл до +127$.
      // 17.10 ~16:25 Влад: «67% покупателей против шорта — бот не закрывается». Сигнал «лимиток» (bookSig) требует ещё крупную заявку
      // на HL (bookMinSizeUsd) и выдержку — разворот стакана Binance на 70–91% 10 с подряд выход не давал. Теперь хватает и самого перевеса стакана.
      else if (cfg.bookExitOnFlip && ((bookSig.side && bookSig.side !== p.side) || (Number.isFinite(bookW) && dir * bookW <= -cfg.bookMinImb))
        && (!going || Math.abs(bookW) >= cfg.bookFlipProfitImb)) why = 'перевес развернулся';
      // «Не пошла за N с» — только если подушка спреда уже съедена. Пока HL стоит выше Binance (для шорта)
      // на ≥ половины порога входа, Binance может ходить против нас внутри подушки, а HL не двигается —
      // убытка нет, сидим (но не дольше cushionMaxHoldSec).
      // …или пока крупные заявки у цены стоят на нашей стороне (перевес ≥ bookFilterImb) — тоже держим
      else if (!going && held >= maxHold
        && !(dir * -dev >= cfg.minDev / 2 || (cfg.bookHoldImb > 0 && bookFavor(books, p.side) >= cfg.bookHoldImb)) ) why = 'время';
      else if (!going && held >= cfg.cushionMaxHoldSec) why = 'время (предел ожидания)';
      if (why) {
        if (p.live) { p.closing = { why, decidedAt: now, decidedPx: hl }; sendClose(p); }   // заявка уходит сразу, один раз
        else p.closing = { why, at: now + paperExitDelay((now - p.opened_at) / 1000), decidedAt: now, decidedPx: hl };   // бумага: выход через реальную задержку очереди
      }
    }
  }
  SAFETY.walls.update(feed, now);

  if (!cfg.enabled || !fresh || !Number.isFinite(hl)) return;
  const ctxBase = { dev, spread, baseline, books };
  // Размах Binance за 10 с: в быстром рынке очередь 8 с на выход съедает любой вход (12.10 06:40–07:00
  // эталон потерял $8K за 40 минут, мы −$190) — такие минуты пропускаем
  const binRange10 = (() => { let lo = Infinity, hi = -Infinity; for (let i = mbuf.length - 1; i >= 0 && mbuf[i][0] >= now - 10000; i--) { const v = mbuf[i][2]; if (v < lo) lo = v; if (v > hi) hi = v; } return hi >= lo ? hi - lo : 0; })();
  const tooFast = cfg.maxBinRange10s > 0 && binRange10 > cfg.maxBinRange10s;
  if (cfg.live && now < guard.pausedUntil) {   // пауза после серии убытков — только в LIVE, бумага не стоит
    const d = { t: now, ok: false, why: `пауза после серии убытков до ${new Date(guard.pausedUntil).toLocaleTimeString('ru-RU')}`, sig: null, open: positions.size + pending.length };
    lastDecision.spread = d; lastDecision.book = d;
    return;
  }
  if (tooFast) {
    const d = { t: now, ok: false, why: `рынок слишком быстрый: Binance прошёл ${r1(binRange10)}$ за 10 с (порог ${cfg.maxBinRange10s})`, sig: null, open: positions.size + pending.length };
    lastDecision.spread = d; lastDecision.book = d;
    return;
  }

  // --- вход «Спред» ---
  if (cfg.spreadEnabled && Number.isFinite(dev) && now - lastEntry.spread >= cfg.cooldownSec * 1000) {
    const ad = Math.abs(dev);
    if (ad >= cfg.minDev && ad <= cfg.maxDev) {
      // HL дороже фона относительно Binance -> HL должен опуститься -> шорт. И наоборот.
      const side = dev > 0 ? 'S' : 'L';
      const dir = side === 'L' ? 1 : -1;
      const binMove = dir * (bf - bfAgo(cfg.binMoveSec));
      const vetoed = cfg.vetoAgainstUsd > 0 && binMove <= -cfg.vetoAgainstUsd;   // Binance идёт против
      // Спред работает вместе со стаканами: крупные заявки у цены должны быть на нашей стороне
      // Блокируем только когда стаканы ПРОТИВ нас (перевес ≤ −порога); ровные — не мешают.
      // Стакан HL у цены тонкий и прыгает (+100% → −75% за секунду), требовать «за нас» = пропускать ходы Binance.
      const favor = bookFavor(books, side);
      const bookOk = !(cfg.bookFilterImb > 0) || !(favor <= -cfg.bookFilterImb);
      if (!bookOk) {
        lastDecision.spread = { t: now, ok: false, sig: ad, open: positions.size + pending.length,
          why: `стаканы против нас: перевес ${Math.round(favor * 100)}% (блок при ≤ −${Math.round(cfg.bookFilterImb * 100)}%)` };
      }
      // cushBoth: подушка от minDev и от среднего спреда за slowBaselineMin мин (нет медленного фона — не входим)
      const slowCush = Number.isFinite(baselineSlow) ? dir * -(spread - baselineSlow) : NaN;
      const bothBad = cfg.cushBoth && !(slowCush >= cfg.minDev);
      if (bookOk && bothBad) {
        lastDecision.spread = { t: now, ok: false, sig: ad, open: positions.size + pending.length,
          why: `подушка от среднего за ${cfg.slowBaselineMin} мин ${Number.isFinite(slowCush) ? r1(slowCush) + '$' : 'ещё не посчитана'} (нужно от ${cfg.minDev}$)` };
      }
      if (!vetoed && bookOk && !bothBad && !hasOpposite('spread', side) && !(cfg.minBinMove > 0 && !(binMove >= cfg.minBinMove))
          && decideEntry('spread', ad, side)) {
        queueEntry('spread', side, { ...ctxBase, binMove, sig: ad });
      }
    }
  }

  // --- вход «Лимитки» ---
  if (cfg.bookEnabled && bookSig.side && now - bookSig.since >= cfg.bookConfirmMs
      && now - lastEntry.book >= cfg.cooldownSec * 1000 && !hasOpposite('book', bookSig.side)) {
    const sig = bookStrength(books);
    const dir = bookSig.side === 'L' ? 1 : -1;
    const binMove = dir * (bf - bfAgo(cfg.binMoveSec));
    const vetoed = cfg.vetoAgainstUsd > 0 && binMove <= -cfg.vetoAgainstUsd;
    if (!vetoed && !(cfg.minBinMove > 0 && !(binMove >= cfg.minBinMove)) && decideEntry('book', sig, bookSig.side)) {
      queueEntry('book', bookSig.side, { ...ctxBase, binMove, sig });
    }
  }
  // Пропуск сигнала — в ленту, но без спама: одна запись на причину, не чаще раза в 3 с
  for (const s of ['spread', 'book']) {
    const d = lastDecision[s];
    if (!d || d.ok || now - d.t > 500) continue;
    if (d.why === skipLogged[s].why && now - skipLogged[s].t < 3000) continue;
    skipLogged[s] = { why: d.why, t: now };
    logEvent('skip', `${s === 'book' ? 'лимитки' : 'спред'}: сигнал есть, не вхожу — ${d.why}`);
  }
}
const skipLogged = { spread: { why: null, t: 0 }, book: { why: null, t: 0 } };

// ---------- для панели ----------
function liveOpen() {
  const hl = mid('hl'), now = Date.now();
  const queued = pending.map((q) => ({
    id: null, strategy: q.strategy, side: q.side, entry: null, mark: hl, live: !!q.live,
    state: q.live ? (q.intentId ? `LIVE в очереди${q.missingSince ? ' — на платформе её уже нет, снимаю' : q.rank != null ? `: ${q.rank}-й` : ''}${q.cancelTries ? `, отменяю (попытка ${q.cancelTries})` : ''}` : 'LIVE отправляю…') : 'в очереди на вход',
    left: q.live ? null : Math.max(0, (q.fillAt - now) / 1000), held: (now - q.createdAt) / 1000,
  }));
  return queued.concat([...positions.values()].map((p) => {
    const dir = p.side === 'L' ? 1 : -1;
    const fav = Number.isFinite(hl) ? dir * (hl - p.entry) : 0;
    return {
      id: p.id, strategy: p.strategy, side: p.side, entry: p.entry, mark: hl, qty: p.qty,
      margin: p.margin, leverage: p.leverage, opened_at: p.opened_at, held: (now - p.opened_at) / 1000,
      live: !!p.live, positionId: p.positionId || null,
      state: p.closing ? (p.live ? `LIVE выход ${p.closing.sent ? `в очереди${p.closing.rank != null ? `: ${p.closing.rank}-й` : ''}` : 'отправляю'} (${p.closing.why})` : `выход в очереди (${p.closing.why})`) : (p.live ? 'LIVE открыта' : 'открыта'),
      left: p.closing ? Math.max(0, (p.closing.at - now) / 1000) : null,
      fav, maxFav: p.maxFav, gross: fav * p.qty, est: paidPerBtc(fav) * p.qty, bust: p.entry - dir * p.bustUsd,
    };
  }));
}

// Бумажный режим: задержки исполнения = медианы последних 30 живых сделок ОСНОВНОГО бота (у этого своих живых
// нет; очередь платформы одна на всех), пока живых мало — entryDelayMs/exitDelayMs из настроек. Кэш на минуту.
const paperSince = Date.now();
let paperDelayCache = { t: 0, entry: NaN, exit: NaN };
// 17.10 Влад: «после минуты закрывает очень быстро… 10 секунд в позе — и закрытие 10 секунд». Живые закрытия всех ботов: держали
// < 10 с → очередь выхода ~10 с, 10–30 с → ~7.5, 30–60 → ~3.6, 1–2 мин → ~1.8, дольше → ~1.4. Бумага берёт задержку выхода по корзине
// времени в позе — медиана живых закрытий всех ботов за 3 дня (кэш 10 мин); в корзине меньше 5 замеров — общая (paperDelay).
const HOLD_B = [10, 30, 60, 120, Infinity];
let exitByHoldCache = { t: 0, b: null };
function exitByHold() {
  if (exitByHoldCache.b && Date.now() - exitByHoldCache.t < 600000) return exitByHoldCache.b;
  const collect = (from) => {
    const b = HOLD_B.map(() => []);
    for (const t of ['btc_lag_trades', 'btcy2_lag_trades', 'btcy2_lag_trades', 'eth_lag_trades']) {
      try { for (const r of db.prepare(`SELECT opened_at, exit_info FROM ${t} WHERE live = 1 AND status = 'closed' AND exit_info IS NOT NULL AND closed_at >= ?`).all(from)) { try { const i = JSON.parse(r.exit_info), q = i.filledAt - i.decidedAt, h = (i.decidedAt - r.opened_at) / 1000; if (q > 0 && q < 60000 && h >= 0) b[HOLD_B.findIndex((x) => h < x)].push(q); } catch {} } } catch {}
    }
    return b;
  };
  const med = (a) => { a.sort((x, y) => x - y); return a.length >= 5 ? a[a.length >> 1] : NaN; };
  let out = collect(Date.now() - 3 * 86400e3).map(med);
  // 17.10: мало замеров за 3 дня → вся история; всё равно мало → соседняя корзина (долгую позу не считаем по общему среднему 12 с)
  if (out.some((v) => !Number.isFinite(v))) { const all = collect(0).map(med); out = out.map((v, i) => (Number.isFinite(v) ? v : all[i])); }
  for (let i = out.length - 1; i >= 0; i--) if (!Number.isFinite(out[i])) out[i] = [out[i + 1], out[i - 1]].find(Number.isFinite);
  for (let i = 0; i < out.length; i++) if (!Number.isFinite(out[i])) out[i] = [out[i - 1], out[i + 1]].find(Number.isFinite);
  exitByHoldCache = { t: Date.now(), b: out };
  return exitByHoldCache.b;
}
function paperExitDelay(heldSec) { const v = exitByHold()[HOLD_B.findIndex((x) => heldSec < x)]; return Number.isFinite(v) ? v : paperDelay('exit'); }
function paperDelay(kind) {
  if (Date.now() - paperDelayCache.t > 60000) {
    const med = (a) => { a.sort((x, y) => x - y); return a.length >= 10 ? a[Math.floor(a.length / 2)] : NaN; };
    const e = [], x = [];
    // 16.10 ~23:20 Влад: «каждые полчаса смотрят задержку из моих лайв-сделок и берут среднюю за эти полчаса» — среднее по
    // замерам очереди его кошельков за последние 30 мин (зависшие дольше 60 с не считаем); меньше 5 замеров — как раньше
    const avg = (a) => { const b = a.filter((v) => v > 0 && v <= 60000); return b.length >= 5 ? b.reduce((s, v) => s + v, 0) / b.length : NaN; };
    const e30 = [], x30 = [];
    // 20.10: отмены (kind = 'cancel', ~2 с) не считаем — раньше попадали в выход и делали бумажный выход вдвое быстрее живого
    try { for (const r of db.prepare("SELECT kind, ms FROM queue_samples WHERE t >= ? AND kind IN ('entry', 'exit')").all(Date.now() - 30 * 60000)) (r.kind === 'entry' ? e30 : x30).push(r.ms); } catch {}
    // 14.10: сначала замеры очереди по любым заявкам кошелька за 2 часа (queue_samples: бот и ручные Влада), потом живые сделки основного бота
    try { for (const r of db.prepare("SELECT kind, ms FROM queue_samples WHERE t >= ? AND kind IN ('entry', 'exit') ORDER BY id DESC LIMIT 60").all(Date.now() - 2 * 3600e3)) (r.kind === 'entry' ? e : x).push(r.ms); } catch {}
    try {
      if (e.length < 10 || x.length < 10) for (const r of db.prepare("SELECT entry_info, exit_info FROM btc_lag_trades WHERE live = 1 AND status = 'closed' ORDER BY id DESC LIMIT 30").all()) {
        try { const i = JSON.parse(r.entry_info || '{}'); if (e.length < 10 && i.queueMs > 0 && i.queueMs < 120000) e.push(i.queueMs); } catch {}
        try { const i = JSON.parse(r.exit_info || '{}'); if (x.length < 10 && i.filledAt > i.decidedAt && i.filledAt - i.decidedAt < 120000) x.push(i.filledAt - i.decidedAt); } catch {}
      }
    } catch {}
    const ea = avg(e30), xa = avg(x30);
    paperDelayCache = { t: Date.now(), entry: Number.isFinite(ea) ? Math.round(ea) : med(e), exit: Number.isFinite(xa) ? Math.round(xa) : med(x), from30: { entry: e30.length, exit: x30.length }, src: 'замеры' };
    // 19.10: своих замеров за 30 мин мало → вход по потоку биржи, выход = вход × (выход/вход по замерам за 2 ч, иначе ×2.5)
    let qs = null; try { qs = require('./queue-stream.cjs').entryWaitMs(); } catch {}
    if (Number.isFinite(qs) && qs > 0) {
      const me = med([...e]), mx = med([...x]), ratio = Number.isFinite(me) && Number.isFinite(mx) && me > 0 ? Math.min(5, Math.max(1.2, mx / me)) : 2.5;
      if (!Number.isFinite(ea)) { paperDelayCache.entry = qs; paperDelayCache.src = 'поток биржи'; }
      if (!Number.isFinite(xa)) paperDelayCache.exit = Math.round(qs * ratio);
    }
  }
  const v = kind === 'entry' ? paperDelayCache.entry : paperDelayCache.exit;
  return Number.isFinite(v) ? v : (kind === 'entry' ? cfg.entryDelayMs : cfg.exitDelayMs);
}
// 19.10 Влад: «статистика обнуляется при перезапуске — мы теряем информацию»: разбивка по базе — с полуночи и за 24 ч (бумага и живые отдельно)
function breakdown(sinceMs, live) {
  const rows = db.prepare(`SELECT side, strategy, reason, opened_at, closed_at, COALESCE(real_pnl, est_pnl) pnl FROM btcy2_lag_trades
    WHERE status = 'closed' AND live = ? AND closed_at >= ? AND COALESCE(real_pnl, est_pnl) IS NOT NULL AND COALESCE(reason, '') != 'перезапуск сервера'`).all(live ? 1 : 0, sinceMs);
  const one = (a) => ({ n: a.length, wins: a.filter((r) => r.pnl > 0.005).length, losses: a.filter((r) => r.pnl < -0.005).length, pnl: Math.round(a.reduce((s, r) => s + r.pnl, 0)) });
  const g = {}; for (const r of rows) { const k = String(r.reason || '—').replace(/\s*\(вся пачка\)/g, '').replace(/[−-]?\d+([.,]\d+)?/g, '#').slice(0, 42); (g[k] = g[k] || []).push(r); }
  const exits = Object.entries(g).map(([reason, a]) => ({ reason, ...one(a) })).sort((a, b) => b.n - a.n).slice(0, 10);
  return { all: one(rows), spread: one(rows.filter((r) => r.strategy === 'spread')), book: one(rows.filter((r) => r.strategy === 'book')), long: one(rows.filter((r) => r.side === 'L')), short: one(rows.filter((r) => r.side === 'S')),
    fast: one(rows.filter((r) => r.closed_at - r.opened_at < 30000)), slow: one(rows.filter((r) => r.closed_at - r.opened_at >= 30000)), exits };
}
function getBreakdowns() { const d = new Date(); d.setHours(0, 0, 0, 0); return { today: breakdown(d.getTime(), 0), d24: breakdown(Date.now() - 864e5, 0), liveToday: breakdown(d.getTime(), 1) }; }
// 19.10: время очереди на вход для бумаги — у каждой заявки своё: случайный замер исполнения входа с кошельков за 2 ч (как в жизни разброс),
// мало замеров — оценка из потока биржи ± разброс, иначе прежняя средняя
const PAPER_CANCEL_MS = 1500;   // отмена успевает, если до исполнения больше 1.5 с (вживую отмена идёт через платформу и бывает со 2–3 попытки)
const paperCancels = { ok: 0, gone: 0, missed: 0, timeout: 0, late: 0, since: Date.now() };
let qPool = [], qPoolAt = 0;
function paperEntryWait() {
  if (Date.now() - qPoolAt > 60000) { qPoolAt = Date.now(); try { const seen = new Set(); qPool = db.prepare("SELECT intent, ms FROM queue_samples WHERE kind = 'entry' AND t >= ? AND ms > 0 AND ms < 120000").all(Date.now() - 2 * 3600e3).filter((r) => (seen.has(r.intent) ? false : seen.add(r.intent))).map((r) => r.ms); } catch { qPool = []; } }
  if (qPool.length >= 8) return qPool[Math.floor(Math.random() * qPool.length)];
  let qs = null; try { qs = require('./queue-stream.cjs').entryWaitMs(); } catch {}
  if (Number.isFinite(qs) && qs > 0) return Math.round(qs * (0.6 + Math.random() * 0.8));
  return paperDelay('entry');
}
function getPaperStats() {
  return { since: paperSince, cancels: paperCancels, delays: { entry: Math.round(paperDelay('entry')), exit: Math.round(paperDelay('exit')) },
    all: getStats(paperSince, null, 0), spread: getStats(paperSince, 'spread', 0), book: getStats(paperSince, 'book', 0) };
}
function getStats(sinceMs = 0, strategy = null, live = 1) {
  // статистика — по реальным сделкам (live = 1) или по бумажным (live = 0), отдельно
  const rows = db.prepare(`SELECT gross_pnl, est_pnl, reason, opened_at, closed_at FROM btcy2_lag_trades
    WHERE status = 'closed' AND live = ${live ? 1 : 0} AND opened_at >= ? ${strategy ? 'AND strategy = ?' : ''}`)
    .all(...(strategy ? [sinceMs, strategy] : [sinceMs]));
  const st = { trades: rows.length, wins: 0, losses: 0, zeros: 0, gross: 0, est: 0, avgHold: 0, byReason: {} };
  let hold = 0;
  for (const r of rows) {
    if (r.reason === 'перезапуск сервера') { st.trades--; continue; }
    if (r.est_pnl > 0.005) st.wins++; else if (r.est_pnl < -0.005) st.losses++; else st.zeros++;
    st.gross += r.gross_pnl || 0;
    st.est += r.est_pnl || 0;
    hold += ((r.closed_at || r.opened_at) - r.opened_at) / 1000;
    const b = st.byReason[r.reason] || (st.byReason[r.reason] = { n: 0, est: 0 });
    b.n++; b.est += r.est_pnl || 0;
  }
  st.avgHold = st.trades ? hold / st.trades : 0;
  st.cancelled = db.prepare(`SELECT COUNT(*) n FROM btcy2_lag_trades WHERE status = 'cancelled' AND live = 1 AND opened_at >= ?
    ${strategy ? 'AND strategy = ?' : ''}`).get(...(strategy ? [sinceMs, strategy] : [sinceMs])).n;
  return st;
}

// Мы против эталона за одно окно времени: только реальные сделки; деньги нормируем на 1 BTC,
// потому что маржа разная (эталон ~$5K, мы $120–600) — сравнимы винрейт и выплата на 1 BTC.
function getCompare() {
  const now = Date.now();
  const stat = (rows) => {
    const n = rows.length;
    if (!n) return { n: 0 };
    const wins = rows.filter((r) => r.pnl > 0.005).length, losses = rows.filter((r) => r.pnl < -0.005).length;
    return {
      n, wins, losses, zeros: n - wins - losses, winRate: wins + losses ? wins / (wins + losses) : null,
      avgMove: rows.reduce((s, r) => s + (r.move || 0), 0) / n,
      perBtc: rows.reduce((s, r) => s + (r.qty > 0 ? r.pnl / r.qty : 0), 0) / n,
      pnl: rows.reduce((s, r) => s + (r.pnl || 0), 0),
      avgHold: rows.reduce((s, r) => s + ((r.closed_at || r.opened_at) - r.opened_at) / 1000, 0) / n,
    };
  };
  const out = {};
  for (const [name, ms] of [['30m', 30 * 60000], ['2h', 2 * 3600e3], ['24h', 864e5]]) {
    const since = now - ms;
    const mine = db.prepare(`SELECT opened_at, closed_at, qty, COALESCE(real_pnl, est_pnl, 0) pnl,
      (exit - entry) * (CASE side WHEN 'L' THEN 1 ELSE -1 END) move FROM btcy2_lag_trades
      WHERE live = 1 AND status = 'closed' AND qty > 0 AND opened_at >= ?`).all(since);
    const ref = cfg.refWallet ? db.prepare('SELECT opened_at, closed_at, qty, pnl, move FROM btcy2_ref_trades WHERE wallet = ? AND opened_at >= ?').all(cfg.refWallet, since) : [];
    out[name] = { mine: stat(mine), ref: stat(ref) };
  }
  return out;
}

function getState() {
  const spread = mid('hl') - mid('bf');
  const books = V.map(bookStat);
  return {
    config: cfg,
    feeds: { ...Object.fromEntries(V.map((v, k) => [v, {
      bid: feed[v].bid, ask: feed[v].ask, mid: mid(v),
      ageMs: Number.isFinite(age(v)) ? age(v) : null,
      bookAgeMs: feed[v].bookT ? Date.now() - feed[v].bookT : null,
      imb: books[k] ? books[k].imb : null, l1b: books[k] ? books[k].l1b : null, l1a: books[k] ? books[k].l1a : null,
    }])), paper: paperMarketQuote ? { ...paperMarketQuote, ageMs: Math.max(0, Date.now()-paperMarketQuote.t, Date.now()-paperMarketQuote.sourceTimeMs) } : { bid:null,ask:null,mid:null,ageMs:null,bookAgeMs:null,imb:null,bookNA:true,source:'PaperTrade website BBO quote; waiting for data' } },
    spread, baseline, dev: spread - baseline, warmup: !Number.isFinite(baseline),
    paperSpread: paperMarketQuote && Number.isFinite(mid('bf')) ? paperMarketQuote.mid-mid('bf') : null,
    bookSignal: bookSig.side ? { side: bookSig.side, heldMs: Date.now() - bookSig.since } : null,
    decisions: lastDecision,
    regime: { regime: regime ? regime.regime : null, pending: regime ? regime.pending : null, r5m: Number.isFinite(regR5m) ? Math.round(regR5m) : null, haltHigh: cfg.regHaltHigh, low: cfg.regLowUsd, high: cfg.regHighUsd },
    tune: getTune(),
    oi: { ...oi, headroomL: oiHeadroom('L'), headroomS: oiHeadroom('S'), rejectL: oiReject.L, rejectS: oiReject.S, rejectsL: oiRejects.L, rejectsS: oiRejects.S },
    pt: (() => { const s = pt.status(); return { key: s.key, mainKey: ptUsingMain(), connected: s.wallet.connected, tradesConnected: s.tradesConnected,
      balance: s.wallet.balance, openOnPlatform: s.wallet.openPositions.length, walletAt: s.wallet.t, err: s.wallet.err, live: cfg.live,
      ready: liveReady() }; })(),
    ev: Object.values(evStats()).sort((a, b) => (a.strategy + a.bucket).localeCompare(b.strategy + b.bucket)),
    compare: getCompare(),
    events: events.slice(-60).reverse(),
    guard: { dayPnl: Math.round(guard.dayPnl * 100) / 100, tripped: guard.tripped, pausedUntil: guard.pausedUntil > Date.now() ? guard.pausedUntil : null },
    executionSync: { ready: executionSync.ready, t: executionSync.t, err: executionSync.err },
    profiles: getProfiles(),
    open: liveOpen(),
    paper: getPaperStats(),
    bd: (() => { try { return getBreakdowns(); } catch { return null; } })(),
    stats: { all: getStats(0), spread: getStats(0, 'spread'), book: getStats(0, 'book') },
    stats24h: { all: getStats(Date.now() - 864e5), spread: getStats(Date.now() - 864e5, 'spread'), book: getStats(Date.now() - 864e5, 'book') },
    // 🧪 тестовый «вчера + 3 правила основного» (scanner/btcy-guard3.js): сегодня и за всё время, рядом сам «вчера»
    guard3: null,
    chart: chart.slice(-300),
  };
}

function getTrades(limit = 300) {
  return db.prepare('SELECT * FROM btcy2_lag_trades ORDER BY id DESC LIMIT ?').all(Math.min(2000, Math.max(1, limit | 0)));
}
function closeById(id) {
  const p = positions.get(Number(id));
  if (!p) return null;
  if (p.live) { p.closing = p.closing || { why: 'вручную', decidedAt: Date.now() }; return { id: p.id, queued: true }; }
  return closePosition(p, 'вручную');
}
function closeAll() {
  return [...positions.values()].map((p) => {
    if (p.live) { p.closing = p.closing || { why: 'вручную', decidedAt: Date.now() }; return { id: p.id, queued: true }; }
    return closePosition(p, 'вручную');
  });
}

// ============================================================
// ПРОФИЛИ НАСТРОЕК: сохранить текущие под именем («спокойный рынок», «пила») и переключать одной кнопкой.
// Не переносятся: LIVE, enabled, кошельки — только торговые параметры.
// ============================================================
db.exec('CREATE TABLE IF NOT EXISTS btcy2_lag_profiles (name TEXT PRIMARY KEY, json TEXT NOT NULL, saved_at INTEGER)');
const PROFILE_SKIP = new Set(['live', 'enabled', 'ptWallet', 'refWallet', 'haircut']);
function getProfiles() { return db.prepare('SELECT name, saved_at FROM btcy2_lag_profiles ORDER BY name').all(); }
function saveProfile(name) {
  name = String(name || '').trim().slice(0, 40);
  if (!name) throw new Error('имя профиля пустое');
  const data = Object.fromEntries(Object.entries(cfg).filter(([k]) => !PROFILE_SKIP.has(k)));
  db.prepare('INSERT INTO btcy2_lag_profiles (name, json, saved_at) VALUES (?, ?, ?) ON CONFLICT(name) DO UPDATE SET json = excluded.json, saved_at = excluded.saved_at')
    .run(name, JSON.stringify(data), Date.now());
  logEvent('profile', `профиль «${name}» сохранён`);
  return getProfiles();
}
function applyProfile(name) {
  const row = db.prepare('SELECT json FROM btcy2_lag_profiles WHERE name = ?').get(String(name || ''));
  if (!row) throw new Error('профиль не найден');
  const data = JSON.parse(row.json);
  for (const k of PROFILE_SKIP) delete data[k];
  setConfig(data);
  logEvent('profile', `применён профиль «${name}»`);
  return cfg;
}
function deleteProfile(name) { db.prepare('DELETE FROM btcy2_lag_profiles WHERE name = ?').run(String(name || '')); return getProfiles(); }

// ============================================================
// ЗАЩИТА БЕЗ ПРИСМОТРА: дневной лимит убытка, пауза после серии убытков, потеря данных рынка.
// Срабатывания — в ленту, в лог и в Telegram (ADMIN_TELEGRAM_ID, иначе TELEGRAM_CHAT_ID).
// ============================================================
const guard = { dayPnl: 0, dayKey: null, tripped: false, pausedUntil: 0, streakAt: 0, lastAlert: {} };
async function tgAlert(text) {
  if (!cfg.tgEnabled) return;   // общий выключатель: пока пользователь не разрешил — в Telegram не пишем вообще
  const token = process.env.TELEGRAM_BOT_TOKEN, chat = process.env.ADMIN_TELEGRAM_ID;   // только личка админа, никогда в общий чат
  if (!token || !chat) return;
  try {
    await fetch(`https://api.telegram.org/bot${token}/sendMessage`, { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chat, text: '⚡ BTC-бот: ' + text }) });
  } catch {}
}
const dayStart = () => { const d = new Date(); d.setHours(0, 0, 0, 0); return d.getTime(); };
// Сводка в Telegram раз в tgSummaryMin минут: мы против эталона за этот отрезок и итог дня
let summaryAt = Date.now();
function tgSummary() {
  if (!(cfg.tgSummaryMin > 0) || Date.now() - summaryAt < cfg.tgSummaryMin * 60000) return;
  const since = summaryAt; summaryAt = Date.now();
  const st = (rows) => { const n = rows.length; if (!n) return 'сделок нет'; const w = rows.filter((r) => r.pnl > 0.005).length, l = rows.filter((r) => r.pnl < -0.005).length;
    return `${n} сд, винрейт ${w + l ? Math.round(100 * w / (w + l)) : '—'}%, ${(rows.reduce((s, r) => s + (r.qty > 0 ? r.pnl / r.qty : 0), 0) / n).toFixed(1)} $/BTC, итог ${rows.reduce((s, r) => s + r.pnl, 0) >= 0 ? '+' : ''}${rows.reduce((s, r) => s + r.pnl, 0).toFixed(0)}$`; };
  const mine = db.prepare("SELECT qty, COALESCE(real_pnl, est_pnl, 0) pnl FROM btcy2_lag_trades WHERE live = 1 AND status = 'closed' AND qty > 0 AND opened_at >= ?").all(since);
  const ref = cfg.refWallet ? db.prepare('SELECT qty, pnl FROM btcy2_ref_trades WHERE wallet = ? AND opened_at >= ?').all(cfg.refWallet, since) : [];
  const live = cfg.live && cfg.enabled ? 'торгую' : cfg.live ? 'LIVE, входы выключены' : 'LIVE выключен';
  tgAlert(`сводка за ${cfg.tgSummaryMin} мин (${live})\nмы: ${st(mine)}\nэталон: ${st(ref)}\nсегодня: ${guard.dayPnl >= 0 ? '+' : ''}${guard.dayPnl.toFixed(0)}$ · открыто позиций ${positions.size} · маржа $${cfg.margin}`);
}
function closeAllFor(why) {
  for (const p of positions.values()) {
    if (p.live) { if (!p.closing) { p.closing = { why, decidedAt: Date.now(), decidedPx: mid('hl') }; sendClose(p); } }
    else closePosition(p, why);
  }
}
function guardTick() {
  const now = Date.now();
  // дневной итог: закрытые реальные сделки с полуночи + нереализованное по открытым живым
  const key = dayStart();
  if (guard.dayKey !== key) { guard.dayKey = key; guard.tripped = false; }
  // Стопы и паузы — только в LIVE: на бумаге бот торгует без ограничений и копит статистику
  // (Влад 16.10: «только в лайве пусть работает стоп»). Закрытие живых позиций без данных рынка остаётся.
  if (!cfg.live) {
    guard.wasLive = false;
    if (Math.max(age('hl'), age('bf')) > 15000 && [...positions.values()].some((p) => p.live && !p.closing)) closeAllFor('нет данных рынка');
    return;
  }
  // LIVE только что включили (или старт с LIVE): старые живые сделки свою паузу уже отработали — полоса с этого момента
  if (!guard.wasLive) { guard.wasLive = true; guard.streakAt = now; }
  const closed = db.prepare("SELECT COALESCE(SUM(COALESCE(real_pnl, est_pnl, 0)), 0) s FROM btcy2_lag_trades WHERE live = 1 AND status = 'closed' AND opened_at >= ?").get(key).s;
  const hl = mid('hl');
  const open = [...positions.values()].filter((p) => p.live).reduce((s, p) => s + (Number.isFinite(hl) ? (p.side === 'L' ? 1 : -1) * (hl - p.entry) * p.qty : 0), 0);
  guard.dayPnl = closed + open;
  // Влад 20.10: лимит дня — откат от лучшей точки дня (закрытые реальные + нереализованное), не абсолютный убыток.
  const hw = SAFETY.highWater(key, guard.dayPnl);
  if (hw.tripped && !guard.tripped) {
    guard.tripped = true; cfg.live = false; try { setConfig({ live: false }); } catch {}
    for (const q of pending.filter((x) => x.live && x.intentId)) { pt.cancel(q.intentId).catch(() => {}); }
    closeAllFor('лимит дня: откат от лучшей точки');
    const msg = `лимит дня: откат ${hw.drawdown.toFixed(0)}$ от лучшей точки ${hw.best.toFixed(0)}$ (лимит ${SAFETY.config.independentGuards.dayHighWaterDrawdownUsd}$) — LIVE выключен, позиции закрываю`;
    logEvent('guard', msg); console.log('[btc-вчера2] ЗАЩИТА: ' + msg);
  }
  // Таймер «торговать до»: в момент отсечки LIVE выключается, заявки снимаются, позиции закрываются.
  if (SAFETY.cutoffReached(now) && !guard.cutoffDone) {
    guard.cutoffDone = true; cfg.live = false; cfg.enabled = false; try { setConfig({ live: false, enabled: false }); } catch {}
    for (const q of pending.filter((x) => x.live && x.intentId)) { pt.cancel(q.intentId).catch(() => {}); }
    closeAllFor('таймер: торговать до');
    const msg = `таймер «торговать до ${SAFETY.config.independentGuards.tradeUntilAbsolute}» — LIVE выключен, заявки сняты, позиции закрываю`;
    logEvent('guard', msg); console.log('[btc-вчера2] ЗАЩИТА: ' + msg);
  }
  // Плохая полоса → пауза входов. Смотрим последние lossWindow сделок: убытков ≥ maxLossStreak И минус по окну
  // ≥ lossUsd — пауза; либо минус по окну ≥ lossHardUsd в любом случае (волатильность разнесла — постоять).
  if (now >= guard.pausedUntil && cfg.lossWindow > 0) {
    const last = db.prepare("SELECT COALESCE(real_pnl, est_pnl, 0) pnl, closed_at FROM btcy2_lag_trades WHERE live = 1 AND status = 'closed' AND qty > 0 ORDER BY id DESC LIMIT ?").all(cfg.lossWindow);
    const losses = last.filter((r) => r.pnl < -0.005).length, net = last.reduce((s, r) => s + r.pnl, 0);
    const byCount = cfg.maxLossStreak > 0 && losses >= cfg.maxLossStreak && net <= -cfg.lossUsd;
    const byHard = cfg.lossHardUsd > 0 && net <= -cfg.lossHardUsd;
    if (last.length && (byCount || byHard) && last[0].closed_at > guard.streakAt) {
      guard.streakAt = last[0].closed_at; guard.pausedUntil = now + cfg.streakPauseMin * 60000;
      const msg = `плохая полоса: за последние ${last.length} сделок ${losses} убытков, итог ${net.toFixed(0)}$ — пауза входов ${cfg.streakPauseMin} мин (до ${new Date(guard.pausedUntil).toLocaleTimeString('ru-RU')})`;
      logEvent('guard', msg); console.log('[btc-вчера2] ЗАЩИТА: ' + msg); tgAlert(msg);
    }
  }
  // потеря данных рынка при открытой позиции → закрываем, вслепую не сидим
  if (Math.max(age('hl'), age('bf')) > 15000 && [...positions.values()].some((p) => p.live && !p.closing)) {
    closeAllFor('нет данных рынка');
    const msg = 'нет данных рынка больше 15 с при открытой позиции — закрываю';
    logEvent('guard', msg); console.log('[btc-вчера2] ЗАЩИТА: ' + msg);
    if (now - (guard.lastAlert.feed || 0) > 600000) { guard.lastAlert.feed = now; tgAlert(msg); }
  }
}

// После перезапуска: живые позиции, открытые ботом, подхватываем обратно по id заявки.
// Чужие позиции кошелька (открытые руками) бот не трогает никогда.
const orphans = new Map();   // intentId -> строка сделки
function adoptOrphans() {
  if (!orphans.size || !pt.wallet.t) return;
  const byIntent = new Map(pt.wallet.openPositions.map((p) => [String(p.intentId || '').toLowerCase(), p]));
  for (const [intentId, row] of orphans) {
    const live = byIntent.get(intentId);
    // Missing from the snapshot is not a close. Restore the SAME row and wait for execution proof.
    const p = { ...row, id: row.id, qty: row.qty, live: true, positionId: String(live ? live.positionId : row.position_id), intentId,
      maxFav: row.max_fav || 0, maxAdv: row.max_adv || 0, bBest: NaN,
      bustUsd: live ? Math.abs(live.bust - row.entry) : row.entry * Math.max(0, 1 / row.leverage - BUST_RESERVE),
      closing: executionJournal.closing(row) };
    if ((!cfg.live || !cfg.enabled || SAFETY.cutoffReached()) && !p.closing) p.closing = { why: 'остановка: восстановленная позиция', decidedAt: Date.now(), decidedPx: mid('hl') };
    positions.set(p.id, p);
    console.log(`[btc-вчера2] восстановлена та же строка #${p.id}, позиция ${p.positionId}`);
    orphans.delete(intentId);
  }
}
function clearClosed() { return db.prepare("DELETE FROM btcy2_lag_trades WHERE status = 'closed'").run().changes; }

// ============================================================
// ЭТАЛОН: бот сам забирает закрытые сделки эталонного кошелька через API
// papertrade и прикладывает к каждой состояние рынка перед его входом.
// Так копится размеченная база «что было на рынке -> что он сделал -> чем кончилось»,
// по которой видно, от чего он отталкивается, без скриншотов и ручной сверки.
// ============================================================
db.exec(`
CREATE TABLE IF NOT EXISTS btcy2_ref_trades (
  position_id TEXT PRIMARY KEY,
  wallet TEXT, opened_at INTEGER, closed_at INTEGER, side TEXT,
  entry REAL, exit REAL, margin REAL, leverage REAL, qty REAL,
  move REAL, pnl REAL, fee REAL, paper REAL,
  lag REAL,          -- макс. отставание HL от Binance в сторону сделки за 6 с до исполнения, $
  bin_move REAL,     -- ход Binance Fut за 10 с до исполнения в сторону сделки, $
  imb TEXT,          -- лучший перевес у цены в сторону сделки за 6 с: HL/BinF/BinS, %
  l1 TEXT,           -- верх стакана за 3 с до исполнения
  has_ctx INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_btcy2_ref_opened ON btcy2_ref_trades(opened_at);
`);
const refHas = db.prepare('SELECT 1 FROM btcy2_ref_trades WHERE position_id = ?');
const refIns = db.prepare(`INSERT OR IGNORE INTO btcy2_ref_trades
  (position_id, wallet, opened_at, closed_at, side, entry, exit, margin, leverage, qty, move, pnl, fee, paper, lag, bin_move, imb, l1, has_ctx)
  VALUES (@position_id, @wallet, @opened_at, @closed_at, @side, @entry, @exit, @margin, @leverage, @qty, @move, @pnl, @fee, @paper, @lag, @bin_move, @imb, @l1, @has_ctx)`);
const E18 = (s) => Number(BigInt(s || '0') * 1000000n / 10n ** 18n) / 1e6;

// Состояние рынка перед исполнением сделки эталона — из буфера mbuf
function refContext(t, side) {
  if (!mbuf.length || mbuf[0][0] > t - 10000 || mbuf.at(-1)[0] < t) return null;
  const dir = side === 'L' ? 1 : -1;
  const near = (x) => { let b = null; for (const s of mbuf) { if (s[0] <= x) b = s; else break; } return b; };
  let lag = -Infinity;
  const imbBest = [-Infinity, -Infinity, -Infinity];
  for (const s of mbuf) {
    if (s[0] < t - 6000) continue;
    if (s[0] > t) break;
    if (s[3] != null) lag = Math.max(lag, dir * -s[3]);
    s[4].forEach((v, k) => { if (v != null) imbBest[k] = Math.max(imbBest[k], dir * v); });
  }
  const a = near(t), b = near(t - 10000), c = near(t - 3000);
  return {
    lag: Number.isFinite(lag) ? lag : null,
    bin_move: a && b ? r1(dir * (a[2] - b[2])) : null,
    imb: JSON.stringify(imbBest.map((v) => (Number.isFinite(v) ? v : null))),
    l1: c ? JSON.stringify(c[5]) : null,
  };
}

const UA = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/130 Safari/537.36', Accept: 'application/json' };
let refLast = { t: 0, ok: null, added: 0, err: null };
async function refPoll() {
  if (!cfg.refEnabled || !cfg.refWallet) return;
  try {
    let path = `/query/accounts/${cfg.refWallet}/trades?filter=all&limit=75`, j = null;
    for (let k = 0; k < 2; k++) {
      const r = await fetch('https://exchange.papertrade.xyz' + path, { headers: UA });
      j = await r.json().catch(() => ({}));
      if (r.status === 400 && j.canonical) { path = j.canonical; continue; }   // сервер требует свою форму адреса
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      break;
    }
    let added = 0;
    for (const x of (j && j.items) || []) {
      if (x.kind !== 'PositionClosed' || refHas.get(String(x.positionId))) continue;
      const sc = Number(x.priceScaleRaw) || 10;
      const entry = Number(x.entryPriceRaw) / sc, exit = Number(x.exitPriceRaw) / sc;
      const margin = E18(x.marginRaw), side = x.isLong ? 'L' : 'S';
      const ctx = refContext(x.openedAtMs, side);
      refIns.run({
        position_id: String(x.positionId), wallet: cfg.refWallet, opened_at: x.openedAtMs, closed_at: x.timestampMs,
        side, entry, exit, margin, leverage: x.leverage, qty: (margin * x.leverage) / entry,
        move: r1((x.isLong ? 1 : -1) * (exit - entry)), pnl: E18(x.adjustedPnlRaw), fee: E18(x.userPaidFeeRaw),
        paper: E18(x.paperMintedRaw), lag: ctx ? r1(ctx.lag) : null, bin_move: ctx ? ctx.bin_move : null,
        imb: ctx ? ctx.imb : null, l1: ctx ? ctx.l1 : null, has_ctx: ctx ? 1 : 0,
      });
      added++;
    }
    refLast = { t: Date.now(), ok: true, added, err: null };
  } catch (e) { refLast = { t: Date.now(), ok: false, added: 0, err: e.message }; }
}

// Разбор сделок эталона по признакам — тот же, что в ручном анализе
function refSummary(sinceMs = 0) {
  const rows = db.prepare('SELECT * FROM btcy2_ref_trades WHERE has_ctx = 1 AND opened_at >= ?').all(sinceMs)
    .map((r) => { let im = []; try { im = JSON.parse(r.imb || '[]'); } catch {} return { ...r, im }; });
  const book = (r) => r.im[0] >= 60 && r.im[1] >= 60;
  const groups = [
    ['Binance прошёл ≥ $10 в сторону сделки за 10 с', (r) => r.bin_move >= 10],
    ['HL отстал от Binance ≥ $10 (спред)', (r) => r.lag >= 10],
    ['отставание $5–10', (r) => r.lag >= 5 && r.lag < 10],
    ['перевес у цены ≥ 60% на HL и BinF', book],
    ['перевес есть, отставания нет', (r) => book(r) && !(r.lag >= 5)],
    ['Binance прошёл ≥ $10 ПРОТИВ сделки', (r) => r.bin_move <= -10],
    ['ни отставания, ни перевеса', (r) => !book(r) && !(r.lag >= 5)],
  ];
  return {
    total: rows.length,
    groups: groups.map(([name, f]) => {
      const g = rows.filter(f);
      return {
        name, n: g.length, wins: g.filter((r) => r.pnl > 0).length, losses: g.filter((r) => r.pnl < 0).length,
        pnl: g.reduce((a, r) => a + r.pnl, 0), avgMove: g.length ? g.reduce((a, r) => a + r.move, 0) / g.length : 0,
      };
    }),
  };
}
function getRef(limit = 300) {
  const tot = db.prepare(`SELECT COUNT(*) n, COALESCE(SUM(pnl),0) pnl, SUM(pnl>0) w, SUM(pnl<0) l,
    MIN(opened_at) since FROM btcy2_ref_trades WHERE wallet = ?`).get(cfg.refWallet);
  return {
    wallet: cfg.refWallet, last: refLast, totals: tot, summary: refSummary(0),
    trades: db.prepare('SELECT * FROM btcy2_ref_trades WHERE wallet = ? ORDER BY opened_at DESC LIMIT ?').all(cfg.refWallet, Math.min(2000, limit)),
  };
}

// ============================================================
// САМООБУЧЕНИЕ ВЫХОДА
// ============================================================
try { db.exec('ALTER TABLE btcy2_lag_trades ADD COLUMN path TEXT'); } catch {}
try { db.exec('ALTER TABLE btcy2_lag_trades ADD COLUMN exit_info TEXT'); } catch {}
try { db.exec('ALTER TABLE btcy2_lag_trades ADD COLUMN entry_info TEXT'); } catch {}
try { db.exec('ALTER TABLE btcy2_ref_trades ADD COLUMN path TEXT'); } catch {}
db.exec(`CREATE TABLE IF NOT EXISTS btcy2_lag_tune_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT, t INTEGER, n INTEGER,
  before TEXT, after TEXT, train_before REAL, train_after REAL, test_before REAL, test_after REAL
)`);

const PATH_SEC = 180;      // сколько секунд пути цены после входа храним
const STEP_MS = 250;       // шаг пути = шаг буфера рынка

// Путь цены от момента исполнения, шаг 250 мс: h — HL от цены входа, b — Binance Fut
// от своего уровня в момент входа. Binance нужен, потому что главный выход — по нему.
function pathFrom(t, entry) {
  // Сетка 250 мс от момента исполнения, в каждую точку — последний снимок не старше 1.5 с.
  // Буфер пишется из тика, который под нагрузкой проскальзывает (снимки раз в ~300 мс),
  // и без пересчёта на сетку пути браковались как «с дырами» — с 05:16 12.10 не сохранился ни один.
  const end = t + PATH_SEC * 1000;
  const pts = mbuf.filter((s) => s[0] >= t - 3000 && s[0] <= end + 3000);
  if (!pts.length || pts[pts.length - 1][0] < end - 1500) return null;   // путь ещё не дописан
  const h = [], b = [];
  let j = 0, b0 = null;
  for (let g = t; g <= end; g += STEP_MS) {
    while (j + 1 < pts.length && pts[j + 1][0] <= g) j++;
    const s = pts[j];
    if (Math.abs(s[0] - g) > 3000) return null;   // дыра в буфере больше 3 с (тик встал) — путь не годится
    if (b0 == null) b0 = s[2];
    h.push(r1(s[1] - entry)); b.push(r1(s[2] - b0));
  }
  return JSON.stringify({ h, b });
}
// старые пути — просто массив HL; новые — {h, b}
const parsePath = (s) => { const x = JSON.parse(s); return Array.isArray(x) ? { h: x, b: null } : x; };
function fillPaths() {
  const now = Date.now(), from = now - 28 * 60000, to = now - (PATH_SEC + 5) * 1000;
  for (const [table, cond] of [['btcy2_lag_trades', "status = 'closed' AND qty > 0"], ['btcy2_ref_trades', '1=1']]) {
    const key = table === 'btcy2_lag_trades' ? 'id' : 'position_id';
    const rows = db.prepare(`SELECT ${key} k, opened_at, entry FROM ${table} WHERE path IS NULL AND ${cond} AND opened_at BETWEEN ? AND ?`).all(from, to);
    const up = db.prepare(`UPDATE ${table} SET path = ? WHERE ${key} = ?`);
    for (const r of rows) { const p = pathFrom(r.opened_at, r.entry); if (p) up.run(p, r.k); }
  }
}

// Рыночный буфер пишем на диск раз в 30 с — data/btcy2-market-ГГГГММДД.jsonl, строка = снимок mbuf
// [t, hl, bf, dev, [imb×100 по стаканам], [[l1b, l1a] по стаканам]]. По нему проверяется ВРЕМЯ входа:
// что делает HL через 3–4 с после сигнала (наш фил в очереди) и через 30 с — без этого не понять,
// какой сигнал переживает очередь. ~25 МБ в день.
let mdumpT = 0;
// 16.10 ~23:50 Влад: «5 минут прогрева — слишком много; пусть ориентируется на старое, если перезагрузка была недавно,
// минута-полторы и всё». При старте подхватываем свою же запись рынка (data/btcy2-market-ГГГГММДД.jsonl) за последние 30 мин:
// mbuf (фон за 20 мин, размах для режима), фон спреда за baselineSec, Binance за минуту. Возвращает возраст последней точки, мс.
function warmFromDisk() {
  try {
    const now = Date.now(), from = now - 30 * 60000, rows = [];
    const nameOf = (t) => { const d = new Date(t); return `btcy2-market-${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}.jsonl`; };
    // 17.10: после полуночи сегодняшний файл короткий — берём и вчерашний (полчаса истории через полночь)
    const names = [...new Set([nameOf(from), nameOf(now)])];
    for (const name of names) {
      const p = path.join(__dirname, '..', 'data', name); if (!fs.existsSync(p)) continue;
      const size = fs.statSync(p).size, len = Math.min(size, 8 * 1048576), fd = fs.openSync(p, 'r'), buf = Buffer.alloc(len);
      fs.readSync(fd, buf, 0, len, size - len); fs.closeSync(fd);
      for (const l of buf.toString('utf8').split('\n')) { if (!l || l[0] !== '[') continue; try { const r = JSON.parse(l); if (Array.isArray(r) && r[0] >= from && r[0] < now && r[1] > 0 && r[2] > 0) rows.push(r); } catch {} }
    }
    if (!rows.length) return null;
    rows.sort((a, b) => a[0] - b[0]);
    for (const r of rows) { mbuf.push(r); if (r[0] >= now - cfg.baselineSec * 1000) sBuf.push([r[0], r[1] - r[2]]); if (r[0] >= now - 60000) bfBuf.push([r[0], r[2]]); }
    mdumpT = rows[rows.length - 1][0];   // прочитанное обратно в файл не пишем
    const age = now - mdumpT;
    console.log(`[btc-вчера2] тёплый старт: подхватил запись рынка, ${rows.length} точек за ${Math.round((mdumpT - rows[0][0]) / 60000)} мин, последняя ${Math.round(age / 1000)} с назад`);
    return age;
  } catch (e) { console.log('[btc-вчера2] тёплый старт не вышел:', e.message); return null; }
}
function dumpMarket() {
  const now = Date.now();
  const rows = mbuf.filter((s) => s[0] > mdumpT && s[0] <= now);
  if (!rows.length) return;
  mdumpT = rows[rows.length - 1][0];
  const d = new Date(now);
  const name = `btcy2-market-${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}.jsonl`;
  fs.appendFileSync(path.join(__dirname, '..', 'data', name), rows.map((s) => JSON.stringify(s)).join('\n') + '\n');
}

// Переигрывание одной сделки с заданными параметрами выхода — те же правила, что в живом цикле.
// Выходы по исходному сигналу («HL догнал», «перевес развернулся») здесь не моделируются:
// они срабатывают только у сделок, которые так и не пошли, и их заменяет таймер.
function replay(dir, path, p, delayMs = cfg.exitDelayMs || 0, qty = 0) {
  const delay = Math.round(delayMs / STEP_MS);
  const H = path.h, Bp = path.b;
  let maxFav = 0, maxAt = 0, decided = -1, bBest = 0;
  for (let i = 0; i < H.length; i++) {
    const fav = dir * H[i];
    if (decided >= 0) { if (i >= decided + delay) return fav; continue; }
    if (fav > maxFav) { maxFav = fav; maxAt = i; }
    const going = maxFav >= Math.max(p.beStartUsd, 1);
    const held = (i * STEP_MS) / 1000;
    let bBack = 0;
    if (Bp && Bp[i] != null) { bBest = Math.max(bBest, dir * Bp[i]); bBack = bBest - dir * Bp[i]; }
    // в большой прибыли (в деньгах этой сделки) откат Binance терпим шире — как в живом цикле
    const binExit = p.bigProfitUsd > 0 && qty > 0 && fav * qty >= p.bigProfitUsd ? p.binExitBigUsd : p.binExitUsd;
    if (-fav >= p.stopUsd
      || (going && binExit > 0 && Bp && bBack >= binExit)
      || (p.trailStartUsd > 0 && maxFav >= p.trailStartUsd && maxFav - fav >= p.trailUsd)
      || (p.beStartUsd > 0 && going && fav <= 0)
      || (going && p.stallSec > 0 && fav > 0 && (i - maxAt) * STEP_MS >= p.stallSec * 1000)
      || (!going && held >= p.maxHoldSec)) decided = i;
  }
  return dir * H[H.length - 1];
}
function score(entries, p, delayMs) {
  let s = 0;
  for (const e of entries) s += paidPerBtc(replay(e.dir, e.path, p, delayMs, e.qty));
  return s / Math.max(1, entries.length);   // средняя выплата на 1 BTC за сделку
}

const GRID = {
  binExitUsd: [0, 3, 5, 8, 12, 16, 20, 30],
  binExitBigUsd: [8, 10, 12, 15, 20, 25, 30],
  stopUsd: [3, 4, 5, 6, 8, 10, 13, 16, 20, 25],
  beStartUsd: [0, 3, 5, 8, 12],
  trailStartUsd: [0, 1, 3, 5, 10, 15, 25, 40],
  trailUsd: [1, 2, 3, 4, 6, 8, 12, 18],
  stallSec: [0, 2, 3, 4, 6, 10, 15],
  maxHoldSec: [5, 8, 15, 30, 60],
};
let tuneLast = null;
function tune() {
  // по 400 последних из каждой таблицы: перебор занимает доли секунды и не тормозит торговый цикл
  // только реальные свои сделки и сделки эталона — бумажные в обучение не идут
  const load = (table, cond) => db.prepare(`SELECT side, path, opened_at, qty FROM ${table} WHERE path IS NOT NULL AND ${cond} ORDER BY opened_at DESC LIMIT 400`).all();
  const entries = [...load('btcy2_lag_trades', 'live = 1'), ...load('btcy2_ref_trades', '1=1')]
    .map((r) => { try { return { dir: r.side === 'L' ? 1 : -1, path: parsePath(r.path), t: r.opened_at, qty: r.qty || 0 }; } catch { return null; } })
    .filter((e) => e && e.path.h && e.path.h.length > 20)
    .sort((a, b) => a.t - b.t);
  const cur = {
    binExitUsd: cfg.binExitUsd, binExitBigUsd: cfg.binExitBigUsd, bigProfitUsd: cfg.bigProfitUsd,   // bigProfitUsd не перебирается — задаёт пользователь
    stopUsd: cfg.stopUsd, beStartUsd: cfg.beStartUsd, trailStartUsd: cfg.trailStartUsd, trailUsd: cfg.trailUsd,
    stallSec: cfg.stallSec, maxHoldSec: Math.max(cfg.spreadMaxHoldSec, 1),
  };
  // Задержка исполнения в переигрывании — реальная медиана очереди по последним живым выходам,
  // а не предположение: при 3 с и 8 с выигрывают разные параметры.
  const lats = db.prepare("SELECT exit_info FROM btcy2_lag_trades WHERE live = 1 AND exit_info IS NOT NULL ORDER BY id DESC LIMIT 30").all()
    .map((r) => { try { const i = JSON.parse(r.exit_info); return i.filledAt - i.decidedAt; } catch { return null; } })
    .filter((v) => Number.isFinite(v) && v > 0).sort((a, b) => a - b);
  const delayMs = lats.length >= 5 ? lats[Math.floor(lats.length / 2)] : (cfg.exitDelayMs || 0);
  const res = { t: Date.now(), n: entries.length, current: cur, applied: false, note: '', delayMs };
  if (entries.length < cfg.tuneMinSamples) {
    res.note = `мало сделок с сохранённым путём цены: ${entries.length} из ${cfg.tuneMinSamples}`;
    tuneLast = res; return res;
  }
  const cut = Math.floor(entries.length * 0.7);
  const train = entries.slice(0, cut), test = entries.slice(cut);
  // Покоординатный поиск: по очереди подбираем каждый параметр при остальных зафиксированных
  let best = { ...cur }, bestS = score(train, best, delayMs);
  for (let pass = 0; pass < 2; pass++) {
    for (const [k, vals] of Object.entries(GRID)) {
      for (const v of vals) {
        const cand = { ...best, [k]: v };
        const s = score(train, cand, delayMs);
        if (s > bestS + 1e-9) { best = cand; bestS = s; }
      }
    }
  }
  Object.assign(res, {
    best, trainCur: score(train, cur, delayMs), trainBest: bestS, testCur: score(test, cur, delayMs), testBest: score(test, best, delayMs),
  });
  const changed = Object.keys(cur).some((k) => cur[k] !== best[k]);
  const better = res.trainBest > res.trainCur * 1.1 + 0.05 && res.testBest > res.testCur;
  // Само применяет только ужесточение (защита срабатывает раньше). Ослабление — стоп шире, откат шире,
  // трейлинг дальше, защита выключена — только как рекомендация, с кнопкой «Применить» в панели.
  const loosening = Object.keys(cur).filter((k) => cur[k] !== best[k] && !tuneIsTighter(k, cur[k], best[k]));
  res.loosening = loosening;
  if (!changed) res.note = 'текущие параметры уже лучшие';
  else if (!better) res.note = 'нашлись другие, но они не лучше на свежих сделках — не меняю';
  else if (!cfg.autoTune || process.env.BTC_LAG_DRYRUN) { res.note = 'нашлись лучшие — автонастройка выключена, только рекомендация'; res.canApply = true; }
  else if (loosening.length) { res.note = `нашлись лучшие, но это ослабление защиты (${loosening.map((k) => TUNE_NAMES[k] || k).join(', ')}) — само не меняю, жду подтверждения`; res.canApply = true; logEvent('tune', `самообучение: ${res.note}`); }
  else {
    const r = applyTuneBest(res, cur); res.applied = r.ok; res.note = r.ok ? 'применил новые параметры (ужесточение)' : 'не применил: ' + r.err;
    logEvent('tune', `самообучение: ${res.note} — ${Object.keys(cur).filter((k) => cur[k] !== best[k]).map((k) => `${TUNE_NAMES[k] || k} ${cur[k]} → ${best[k]}`).join(', ')}`);
  }
  tuneLast = res;
  return res;
}
const TUNE_NAMES = { binExitUsd: 'откат Binance', binExitBigUsd: 'откат Binance в большой прибыли', stopUsd: 'стоп', beStartUsd: 'безубыток',
  trailStartUsd: 'начало трейлинга', trailUsd: 'трейлинг', stallSec: 'движение встало', maxHoldSec: 'время без плюса' };
// Ужесточение = защита срабатывает раньше: значение меньше, или защитная функция включается (0 → >0)
function tuneIsTighter(k, from, to) {
  if (['trailStartUsd', 'beStartUsd', 'stallSec'].includes(k)) { if (from === 0 && to > 0) return true; if (to === 0 && from > 0) return false; }
  return to < from;
}
function applyTuneBest(res, cur) {
  const best = res.best;
  const patch = { ...best, spreadMaxHoldSec: best.maxHoldSec, bookMaxHoldSec: Math.max(best.maxHoldSec, cfg.bookMaxHoldSec) };
  delete patch.maxHoldSec; delete patch.bigProfitUsd;
  try {
    setConfig(patch);
    db.prepare(`INSERT INTO btcy2_lag_tune_log (t, n, before, after, train_before, train_after, test_before, test_after)
      VALUES (?,?,?,?,?,?,?,?)`).run(res.t, res.n, JSON.stringify(cur), JSON.stringify(best), res.trainCur, res.trainBest, res.testCur, res.testBest);
    console.log(`[btc-вчера2] самообучение: ${JSON.stringify(cur)} -> ${JSON.stringify(best)}`);
    return { ok: true };
  } catch (e) { return { ok: false, err: e.message }; }
}
// Кнопка «Применить рекомендацию» в панели — подтверждение пользователя на ослабление
function applyTune() {
  if (!tuneLast || !tuneLast.best || !tuneLast.canApply) return { ok: false, err: 'нечего применять' };
  const r = applyTuneBest(tuneLast, tuneLast.current);
  if (r.ok) { tuneLast.applied = true; tuneLast.canApply = false; tuneLast.note = 'применил по подтверждению пользователя'; }
  return r;
}
function getTune() {
  return { last: tuneLast, log: db.prepare('SELECT * FROM btcy2_lag_tune_log ORDER BY id DESC LIMIT 20').all() };
}

// ============================================================
// ПОТОЛОК ОТКРЫТОГО ИНТЕРЕСА papertrade
// Платформа не даёт открыть сторону, где OI упёрся в потолок («Limited by long OI cap»).
// В /state/trading у BTC поля [19]/[20] меняются со сделками, [21]/[22] стоят —
// по наблюдению 2026-10-12 это OI лонгов/шортов и их потолки в BTC. Это вывод из данных,
// а не документация: при реальной торговле сверим с ответом платформы на заявку.
// ============================================================
let oi = { t: 0, long: NaN, short: NaN, longCap: NaN, shortCap: NaN, maxNetUsd: NaN, minNotional: NaN, paused: false, err: null };
// когда платформа отклонила вход по OI (admission_rejected/openInterest) — сторона на паузе
const oiReject = { L: 0, S: 0 };
const oiRejects = { L: 0, S: 0 };   // счётчик отказов по потолку за время работы
const OI_REJECT_PAUSE_MS = 300;    // отказ → новая попытка через 300 мс (время ответа платформы ~200 мс): кто чаще долбится, тот и войдёт
// Опрос раз в секунду с ETag, как это делает сам сайт: пока состояние не менялось,
// сервер отвечает 304 без тела — нагрузка мизерная, а свежесть данных — секунда.
let oiEtag = null, oiBusy = false;
async function oiPoll() {
  if (oiBusy) return;                      // прошлый запрос ещё в пути — не накладываем
  oiBusy = true;
  try {
    const r = await fetch('https://exchange.papertrade.xyz/state/trading', { headers: { ...UA, ...(oiEtag ? { 'If-None-Match': oiEtag } : {}) }, signal: AbortSignal.timeout(8000) });
    if (r.status === 304) { oi = { ...oi, t: Date.now(), err: null }; return; }
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    oiEtag = r.headers.get('etag') || null;
    const j = await r.json();
    const b = (j.instruments || []).find((x) => x[1] === 'BTC');
    if (!b) throw new Error('нет BTC в ответе');
    // intake = [revision, минимальный стейк, maximumNetOpenInterest ($), отключённые действия]
    const maxNet = Array.isArray(j.intake) && j.intake[2] ? E18(j.intake[2]) : NaN;
    oi = { t: Date.now(), long: E18(b[19]), short: E18(b[20]), longCap: E18(b[21]), shortCap: E18(b[22]), maxNetUsd: maxNet,
      instrumentEnabled: b[4], maximumLeverage: b[14], instrumentTuple: b, intakeControls: j.intakeControls || null,
      minNotional: j.minimumOpenNotionalRaw ? E18(j.minimumOpenNotionalRaw) : NaN, paused: !!j.tradingPaused, err: null };
  } catch (e) { oi = { ...oi, err: e.message }; }
  finally { oiBusy = false; }
}
// Правило платформы (функция gn() в коде сайта): сторону ограничивает и её потолок [21]/[22],
// и «чистый» OI — лонгов не может быть больше шортов более чем на maximumNetOpenInterest ($60M)
// в пересчёте по цене. Запас стороны (BTC) = min(потолок, OI другой стороны + maxNet/цена) − OI стороны.
// 12.10 06:35: LONG $147M − SHORT $83M = $64M > $60M → все лонги получали admission_rejected/openInterest.
function oiHeadroom(side) {
  const mine = side === 'L' ? oi.long : oi.short, other = side === 'L' ? oi.short : oi.long;
  const cap = side === 'L' ? oi.longCap : oi.shortCap;
  const px = mid('hl');
  let lim = Number.isFinite(cap) && cap > 0 && cap < 1e12 ? cap : Infinity;   // 1e12+ — «без потолка»
  if (Number.isFinite(oi.maxNetUsd) && oi.maxNetUsd > 0 && Number.isFinite(px) && px > 0) lim = Math.min(lim, other + oi.maxNetUsd / px);
  return Number.isFinite(lim) && Number.isFinite(mine) ? lim - mine : NaN;
}
// Не шлём заявку только если торги на паузе или по данным платформы запаса нет совсем (≤ 0).
// Запас «меньше позиции» не блокируем: наши единицы OI неточные (UI показывает на ~18% больше),
// а отказ платформы бесплатен — пусть решает она.
function oiBlocked(side, qty) {
  if (!oi.t || Date.now() - oi.t > 30000) return false;
  if (oi.paused) return true;
  const head = oiHeadroom(side);
  return Number.isFinite(head) && head <= 0;
}

let timer = null;
function start() {
  if (timer) return;
  // No archive market data is imported, no duplicate recording collector is started.
  WARMUP_MS = 180000;
  setInterval(() => { try { regimeTick(); } catch {} }, 1000);
  // Живые позиции после перезапуска подхватываем по id заявки (adoptOrphans),
  // бумажные в памяти потеряны — закрываем их записью, чтобы не висели «открытыми».
  for (const row of db.prepare("SELECT * FROM btcy2_lag_trades WHERE status='open' AND live=1 AND intent_id IS NOT NULL").all()) {
    orphans.set(String(row.intent_id).toLowerCase(), row);
  }
  for (const row of db.prepare("SELECT position_id,exit,closed_at,close_evidence FROM btcy2_lag_trades WHERE live=1 AND status='closed' AND position_id IS NOT NULL").all()) {
    const c = row.close_evidence ? JSON.parse(row.close_evidence) : { px: row.exit, ts: row.closed_at, source: 'persisted-execution' };
    if (Number.isFinite(c.px) && Number.isFinite(c.ts)) { closePx.set(String(row.position_id), c); pt.confirmClosed(row.position_id, c); }
  }
  db.prepare(`UPDATE btcy2_lag_trades SET status='closed', closed_at=?, reason='перезапуск сервера',
    gross_pnl=COALESCE(gross_pnl,0), est_pnl=COALESCE(est_pnl,0) WHERE status='open' AND COALESCE(live,0)=0`).run(Date.now());
  connectFeeds();
  // на ключе основного: кошелёк = кошелёк основного, свой поток не открываем (основной уже слушает его; лишний поток
  // к тому же кошельку только приближает лимит запросов платформы)
  if (cfg.useMainKey) { const mw = String(ptMain.keyStatus().master || '').toLowerCase(); if (/^0x[0-9a-f]{40}$/.test(mw)) cfg.ptWallet = mw; pt.tradesStream(); }
  else if (cfg.ptWallet) { pt.walletStream(cfg.ptWallet); pt.tradesStream(); }
  setInterval(() => { adoptOrphans(); liveTick().catch((e) => console.error('[btc-вчера2] live:', e.message)); }, 300);
  livePnlSync().catch(() => {}); setInterval(() => { livePnlSync().catch(() => {}); }, 5000);
  setInterval(() => { try { guardTick(); tgSummary(); } catch (e) { console.error('[btc-вчера2] защита:', e.message); } }, 1000);
  timer = setInterval(tick, 100);
  oiPoll(); setInterval(oiPoll, 500);
  console.log(`BTC-бот: ${cfg.enabled ? 'включён' : 'выключен'}; спред ${cfg.spreadEnabled ? `от $${cfg.minDev}` : 'выкл'}, лимитки ${cfg.bookEnabled ? 'вкл' : 'выкл'}, плечо ${cfg.leverage}×`);
}

// Разрешение ключа бота: данные для подписи в MetaMask и приём подписи.
// После смены кошелька поток его состояния переподключается.
function ptRegisterData(wallet) {
  if (wallet) setConfig({ ptWallet: wallet });
  pt.walletStream(cfg.ptWallet);
  return pt.registrationTypedData(cfg.ptWallet);
}
async function ptRegister(signature) { return pt.submitRegistration(signature); }

module.exports = { ethWantGet: () => ethWant, isLive: () => !!cfg.live, noteEth: (t) => logEvent('eth', t), skippedSince, start, getState, getTrades, getStats, setConfig, closeById, closeAll, clearClosed, getRef, refPoll, getTune, tune, applyTune,
  killSwitch, startTrading, ptRegisterData, ptRegister, DEFAULTS, getProfiles, saveProfile, applyProfile, deleteProfile };
