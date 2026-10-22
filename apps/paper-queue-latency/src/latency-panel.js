// Read-only visible extension card. It never clicks, blocks, signs or submits an order.
(() => {
  'use strict';
  const host = document.getElementById('ptq-host'), root = host?.shadowRoot;
  if (!root || root.getElementById('ptq-latency')) return;
  const L = globalThis.PTQLatency;
  const style = document.createElement('style');
  style.textContent = `
+    .latency-card{padding:5px 0;border-bottom:1px solid #23272e}
+    .latency-row{display:flex;align-items:center;gap:8px;white-space:nowrap}
+    .latency-status{display:inline-flex;align-items:center;gap:5px;font-weight:650;color:#edc16d;flex:none}
+    .latency-status::before{content:'';width:7px;height:7px;border-radius:50%;background:currentColor;flex:none}
+    .latency-status[data-state=allow]{color:#68d69a}
+    .latency-status[data-state=block]{color:#ff9290}
+    .latency-est{margin-left:auto;color:#8b929c;font-size:11px}
+    .latency-est b{color:#eef0f4;font-weight:650;font-size:12px}
+    .latency-reasons{min-height:14px;margin:2px 0 0;font-size:10.5px;color:#bfc5cf;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
+    .latency-note{font-size:10.5px;line-height:1.4;color:#939aa5;margin:3px 0}
+    .latency-settings-message{font-size:10.5px;color:#edc16d}
+  `.replace(/^\+/gm, '');
  root.appendChild(style);
  const card = document.createElement('section'); card.className = 'latency-card'; card.id = 'ptq-latency';
  card.setAttribute('aria-label', 'Оценка задержки входа и выхода');
  card.innerHTML = `
    <div class="latency-row"><span class="latency-status" role="status" data-state="unknown">Жду данные биржи</span>
      <span class="latency-est">вход <b data-estimate="entry">—</b> · выход <b data-estimate="exit">—</b></span></div>
    <div class="latency-reasons"></div>`;
  const NOTES = 'Эвристика очереди, не точное время и не p90. Выход оценён при текущей загрузке. '
    + 'Только наблюдение: кнопки заявок не блокируются. Прибыльность не проверена. '
    + 'Фактические send→fill: нет телеметрии. Алерты пока не подключены.';
  (root.querySelector('.q') || root.querySelector('.body').firstChild).after(card);
  const VERSION = 'v' + chrome.runtime.getManifest().version;
  const definitions = [
    ['gateEntryBudgetSec', 'Лимит входа, с', 6],
    ['gateExitBudgetSec', 'Лимит выхода, с', 15],
    ['gateSignalTtlSec', 'Жизнь сигнала, с', 5],
  ];
  const settings = root.querySelector('.set'), inputs = new Map();
  const heading = document.createElement('h3'); heading.textContent = 'Timing-фильтр · пример лимитов'; settings.appendChild(heading);
  const message = document.createElement('p'); message.className = 'latency-settings-message';
  const active = Object.fromEntries(definitions.map(([k, , value]) => [k, value]));
  for (const [key, label, value] of definitions) {
    const row = document.createElement('label'); row.appendChild(document.createTextNode(label));
    const input = document.createElement('input'); input.type = 'number'; input.min = '.1'; input.max = '600'; input.step = '.1';
    input.value = String(value); input.dataset.latencyKey = key; row.appendChild(input); settings.appendChild(row); inputs.set(key, input);
    input.addEventListener('change', () => {
      const n = input.valueAsNumber;
      if (!Number.isFinite(n) || n <= 0 || n > 600) { message.textContent = 'Не применено: укажите от 0,1 до 600 секунд.'; return; }
      message.textContent = '';
      chrome.storage.local.set({ [key]: n }).catch(() => { message.textContent = 'Не удалось сохранить лимит'; });
    });
  }
  settings.appendChild(message);
  const note = document.createElement('p'); note.className = 'latency-note';
  note.textContent = 'Исследовательские лимиты, не обученная модель. База вход/выход: 3/10 с; запас ×1,5.'; settings.appendChild(note);
  function apply(values) {
    for (const [key] of definitions) if (key in values) {
      active[key] = values[key];
      if (root.activeElement !== inputs.get(key)) inputs.get(key).value = String(values[key]);
    }
  }
  chrome.storage.local.get(definitions.map(x => x[0])).then(apply).catch(() => {});
  chrome.storage.onChanged.addListener((changes, area) => {
    if (area === 'local') apply(Object.fromEntries(Object.entries(changes).filter(([k]) => inputs.has(k)).map(([k, v]) => [k, v.newValue ?? definitions.find(x => x[0] === k)[2]])));
  });
  const time = x => typeof x !== 'number' || !Number.isFinite(x) ? '—' : x < 120000 ? '≈ ' + (x / 1000).toLocaleString('ru-RU', { maximumFractionDigits: 1 }) + ' с' : '≈ ' + (x / 60000).toLocaleString('ru-RU', { maximumFractionDigits: 1 }) + ' мин';
  function render(state) {
    const r = state?.timing || { ...L.empty(), reasons: ['PRESSURE_MISSING'] };
    const status = card.querySelector('.latency-status');
    const measured = r.entry_ms !== null && r.exit_ms !== null;
    status.dataset.state = r.allow ? 'allow' : measured ? 'block' : 'unknown';
    status.textContent = r.allow ? 'Допустимо' : measured ? 'Пропуск' : 'Нет оценки';
    status.title = r.allow ? 'По времени допустимо · shadow' : measured ? 'Пропустить вход: превышены лимиты' : 'Нет надёжной оценки';
    card.querySelector('[data-estimate=entry]').textContent = time(r.entry_ms);
    card.querySelector('[data-estimate=exit]').textContent = time(r.exit_ms);
    const why = (r.reasons || []).map((code) => L.reason(code)).join(' · '), rs = card.querySelector('.latency-reasons');
    rs.textContent = why; rs.title = why;
    const limits = state?.timingLimits || active;
    card.title = `Задержка · shadow ${VERSION}\nЛимиты: вход ${limits.gateEntryBudgetSec} с · выход ${limits.gateExitBudgetSec} с · сигнал ${limits.gateSignalTtlSec} с\n${NOTES}`;
    card.dataset.status = r.status;
    card.dataset.economics = r.economics;
  }
  globalThis.PTQLatencyPanel = { render }; render(null);
})();
