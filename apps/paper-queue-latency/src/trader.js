// Быстрые кнопки «Лонг / Шорт / Закрыть всё» на вкладке биржи. Расширение НИЧЕГО не подписывает и не хранит ключей:
// оно нажимает штатные элементы самой биржи (стороны, поля маржи/плеча, «Place Trade», «Close All») в вашей сессии,
// кошелёк и session key подписывают как обычно. Режимы взвода (панель):
//   off  — кнопки заблокированы;  dry — ставит сторону/маржу/плечо и проверяет Order Value, «Place Trade» НЕ нажимает;
//   live — то же и нажимает. Ни при каком режиме не работает без явного клика/горячей клавиши пользователя.
(() => {
  'use strict';
  if (window.top !== window || globalThis.PTQTrader) return;

  const sleep0 = () => new Promise((r) => setTimeout(r, 0));
  const setNative = (el, v) => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(el, String(v));
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
  };
  // "$399K" / "$1.26M" / "$1,260,000" → число (null — не разобрать).
  function parseUsd(s) {
    const m = /\$?\s*([\d,]+(?:\.\d+)?)\s*([KMB])?/i.exec(String(s || ''));
    if (!m) return null;
    const n = Number(m[1].replace(/,/g, '')), k = { K: 1e3, M: 1e6, B: 1e9 }[(m[2] || '').toUpperCase()] || 1;
    return Number.isFinite(n) ? n * k : null;
  }
  function orderValue(form) {
    for (const row of form.querySelectorAll('.trade-summary')) {
      const t = row.innerText || '';
      const m = /Order Value\s*\n?\s*(\$[\d.,]+\s*[KMB]?)/i.exec(t);
      if (m) return parseUsd(m[1]);
    }
    return null;
  }
  const els = () => {
    const form = document.querySelector('form.order-ticket');
    return form && { form, long: form.querySelector('button.dup'), short: form.querySelector('button.ddn'), margin: form.querySelector('input#im'), lev: form.querySelector('input#il'), go: form.querySelector('button#go') };
  };
  const marketOnPage = () => { const m = /\/trade\/([a-z0-9]+)/i.exec(location.pathname); return m ? m[1].toUpperCase() : null; };

  // side: 'L' | 'S'. Возвращает { ok, ms, error?, filled?, clicked }.
  async function open(side, { margin, lev, mode }) {
    const t0 = performance.now();
    const fail = (error) => ({ ok: false, error, ms: Math.round(performance.now() - t0), clicked: false });
    if (mode !== 'dry' && mode !== 'live') return fail('кнопки не взведены');
    if (!(margin > 0) || !(lev > 0)) return fail('нет пресета: маржа/плечо');
    const e = els();
    if (!e || !e.long || !e.short || !e.margin || !e.lev || !e.go) return fail('не нашёл форму заявки на странице');
    const want = side === 'L' ? e.long : e.short;
    if (!want.classList.contains('on')) want.click();
    setNative(e.margin, margin);
    setNative(e.lev, lev);
    // ждём, пока форма примет значения: обе стороны/поля совпадают, Order Value ≈ маржа×плечо, кнопка активна
    const target = margin * lev;
    let state = 'ждём форму';
    for (let i = 0; i < 400 && performance.now() - t0 < 700; i++) {
      const ov = orderValue(e.form);
      const sideOk = want.classList.contains('on');
      const valsOk = Math.abs(Number(e.margin.value) - margin) < 1e-6 * Math.max(1, margin) + 0.005 && Number(e.lev.value) === lev;
      const ovOk = ov !== null && Math.abs(ov - target) <= 0.02 * target + 1;
      if (sideOk && valsOk && ovOk && !e.go.disabled) { state = 'ok'; break; }
      state = !sideOk ? 'сторона не переключилась' : !valsOk ? `поля: маржа ${e.margin.value}, плечо ${e.lev.value}` : !ovOk ? `Order Value ${ov === null ? '—' : ov} ≠ ${target}` : `кнопка: ${e.go.innerText.trim() || 'заблокирована'}`;
      await sleep0();
    }
    if (state !== 'ok') return fail(state);
    if (mode === 'dry') return { ok: true, clicked: false, dry: true, ms: Math.round(performance.now() - t0), market: marketOnPage() };
    e.go.click();
    return { ok: true, clicked: true, ms: Math.round(performance.now() - t0), market: marketOnPage() };
  }

  // «Закрыть всё»: штатная кнопка Close All под формой.
  async function closeAll({ mode }) {
    const t0 = performance.now();
    const fail = (error) => ({ ok: false, error, ms: Math.round(performance.now() - t0), clicked: false });
    if (mode !== 'dry' && mode !== 'live') return fail('кнопки не взведены');
    const b = document.querySelector('.open-pos-row .open-pos-close-btn') || document.querySelector('.open-pos-close-btn');
    if (!b) return fail('нет кнопки Close All');
    if (b.disabled) return fail(`кнопка заблокирована: ${(b.innerText || '').trim() || '—'}`);
    if (mode === 'dry') return { ok: true, clicked: false, dry: true, ms: Math.round(performance.now() - t0) };
    b.click();
    return { ok: true, clicked: true, ms: Math.round(performance.now() - t0) };
  }

  globalThis.PTQTrader = { open, closeAll, parseUsd, marketOnPage };
})();
