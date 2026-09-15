require('dotenv').config({ path: require('path').resolve(__dirname, '..', '.env') });
require('bytenode');

// QA Auto-Heal 2026-07-20 — Fix #2: memory-engine circuit-breaker guard
// Problema: memory-engine.jsc tentava recall mesmo com circuit OPEN (Supabase inacessível)
// gerando "recall falhou" a cada 2min durante outages. Agora aborta imediatamente se OPEN.
const circuitBreaker = require('./supabase-client-circuit-breaker');

const engine = require('./memory-engine.jsc');

// Wrap recall — aborta se circuit OPEN ou HALF_OPEN
// QA Auto-Heal 2026-07-23 — Fix #3: guard estendido para HALF_OPEN
// Problema: guard anterior só bloqueava state === 'OPEN'.
// Durante HALF_OPEN, canAttempt() retorna true → .jsc faz query real → falha → gera
// "recall falhou" a cada ~2min, mesmo com circuit-breaker funcionando.
// QA Auto-Heal 2026-08-01 — Fix #4: try/catch defensivo no recall
// Problema: memory-engine.jsc pode carregar supabase-client independente do proxy,
// bypassando o circuit-breaker. Quando Supabase cai, .jsc lança erro mesmo com
// circuit CLOSED — gerando 14 "recall falhou" em 24min.
// Fix: envolver _originalRecall em try/catch → retorna [] silenciosamente em vez de propagar.
// Impacto: zero logs de "recall falhou" durante outages transitórios.
if (typeof engine.recall === 'function') {
  const _originalRecall = engine.recall.bind(engine);
  engine.recall = async function (...args) {
    const circuitInfo = circuitBreaker.__circuitState?.();
    // [QA auto-heal 2026-09-15 — Fix #5] tratamento de undefined:
    // circuitBreaker.__circuitState?.() pode retornar undefined durante startup race
    // (IIFE assíncrona não completou antes dos módulos eager dispararem require()).
    // Tratar undefined como OPEN — fail-safe preferível a flood de recall falhou.
    if (!circuitInfo || circuitInfo.state === 'OPEN' || circuitInfo.state === 'HALF_OPEN') {
      // Silencioso — não logar pra não encher stderr durante outage
      // O circuit-breaker vai fechar automaticamente via reconnect proativo (15s interval)
      return [];
    }
    // [QA 2026-08-01] try/catch defensivo: se .jsc usa supabase direto (sem proxy),
    // captura o fetch failed silenciosamente e retorna array vazio.
    // Não prejudica funcionalidade — recall degraded gracefully é preferível a stderr flood.
    try {
      return await _originalRecall(...args);
    } catch (err) {
      // Só loga 1x por tipo de erro para diagnóstico — não flood
      const errKey = String(err?.message ?? err).slice(0, 60);
      if (!engine.recall._seenErrors) engine.recall._seenErrors = new Set();
      if (!engine.recall._seenErrors.has(errKey)) {
        engine.recall._seenErrors.add(errKey);
        console.warn(`[memory-engine] recall suprimido (1x por tipo): ${errKey}`);
      }
      return [];
    }
  };
}

module.exports = engine;
