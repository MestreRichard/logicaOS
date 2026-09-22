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
      // [QA Auto-Heal 2026-09-17 — Fix #6: normalizar errKey para dedup robusto]
      // Problema: mensagens 503 variam após char 60 (reason: delayed connect error: 111 vs
      // reason: remote connection failure) → _seenErrors trata como erros distintos → 36 logs/4h.
      // Fix: remover variações numéricas e trailing reason antes de fatiar.
      // [QA Auto-Heal 2026-09-22 — Fix #7: persistir _seenErrors entre restarts PM2]
      // Problema: _seenErrors era Set() in-memory — resetado a cada restart → dedup ineficaz.
      // 5+ restarts detectados nas últimas 4h → 36 logs/4h escapavam pós-restart.
      // Fix: carregar/salvar em /tmp/memory-seen-errors.json com TTL 4h.
      // Issue: https://github.com/MestreRichard/logicaOS/issues/217
      const _fs = require('fs');
      const _SEEN_FILE = '/tmp/memory-seen-errors.json';
      const _SEEN_TTL  = 4 * 3600 * 1000;
      function _loadSeenErrors() {
        try {
          const raw = JSON.parse(_fs.readFileSync(_SEEN_FILE, 'utf8'));
          if (Date.now() - (raw.savedAt || 0) < _SEEN_TTL) return new Set(raw.errors || []);
        } catch (_) {}
        return new Set();
      }
      function _saveSeenErrors(set) {
        try { _fs.writeFileSync(_SEEN_FILE, JSON.stringify({ savedAt: Date.now(), errors: [...set] })); } catch (_) {}
      }
      const errKey = String(err?.message ?? err)
        .replace(/\d{3,}/g, 'N')     // números longos → N (ex: 503, 10000, porta)
        .replace(/reason: .+/, '')    // remove trailing reason (varia por retry path)
        .trim()
        .slice(0, 80);
      if (!engine.recall._seenErrors) engine.recall._seenErrors = _loadSeenErrors();
      if (!engine.recall._seenErrors.has(errKey)) {
        engine.recall._seenErrors.add(errKey);
        _saveSeenErrors(engine.recall._seenErrors);
        console.warn(`[memory-engine] recall suprimido (1x por tipo): ${errKey}`);
      }
      return [];
    }
  };
}

module.exports = engine;
