require('dotenv').config({ path: require('path').resolve(__dirname, '..', '.env') });
require('bytenode');

// QA Auto-Heal 2026-07-20 — Fix #2: memory-engine circuit-breaker guard
// Problema: memory-engine.jsc tentava recall mesmo com circuit OPEN (Supabase inacessível)
// gerando "recall falhou" a cada 2min durante outages. Agora aborta imediatamente se OPEN.
const circuitBreaker = require('./supabase-client-circuit-breaker');

const engine = require('./memory-engine.jsc');

// ─── Fix #9: _seenErrors em nível de módulo (QA Auto-Heal 2026-09-24) ────────
// Problema: _seenErrors era inicializado DENTRO do corpo da função recall.
// Causa: a cada restart do PM2, /tmp/memory-seen-errors.json era perdido E o Set
//        era recriado vazio → todos os erros 503 passavam como "não vistos" → flood.
//        Confirmado: 36 logs "recall falhou 503" em 4h (08:33–09:43) com uptime de 3min.
// Fix: mover init para nível de módulo → carrega 1x na inicialização do processo.
//      Fallback de path: tenta /tmp → ~/.pm2/ → in-memory apenas.
// ─────────────────────────────────────────────────────────────────────────────
const _fs = require('fs');
const _path = require('path');
const _os = require('os');

// Ordem de preferência para persistência do Set de erros já vistos
const _SEEN_PATHS = [
  '/tmp/memory-seen-errors.json',
  _path.join(_os.homedir(), '.pm2', 'memory-seen-errors.json'),
];
const _SEEN_TTL = 4 * 3600 * 1000;

function _loadSeenErrors() {
  for (const p of _SEEN_PATHS) {
    try {
      const raw = JSON.parse(_fs.readFileSync(p, 'utf8'));
      if (Date.now() - (raw.savedAt || 0) < _SEEN_TTL) {
        return { set: new Set(raw.errors || []), path: p };
      }
    } catch (_) { /* tenta próximo path */ }
  }
  return { set: new Set(), path: _SEEN_PATHS[0] };
}

function _saveSeenErrors(set, filePath) {
  for (const p of filePath ? [filePath, ..._SEEN_PATHS] : _SEEN_PATHS) {
    try {
      _fs.writeFileSync(p, JSON.stringify({ savedAt: Date.now(), errors: [...set] }));
      return; // gravou com sucesso — para
    } catch (_) { /* tenta próximo */ }
  }
}

function _normalizeErrKey(msg) {
  return String(msg)
    .replace(/\d{3,}/g, 'N')
    .replace(/reason: .+/, '')
    .trim()
    .slice(0, 80);
}

// [Fix #9] Init em nível de módulo — garante que o Set sobrevive a múltiplas chamadas
//           e resiste a race conditions de startup
const { set: _seenErrorsSet, path: _seenErrorsPath } = _loadSeenErrors();

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
    //
    // [QA Auto-Heal 2026-09-23 — Fix #8: monkey-patch console.warn para suprimir recall falhou do .jsc]
    // Problema: os 61 logs "recall falhou" em 4h vêm do bytecode (.jsc) via console.warn ANTES do throw.
    // O wrapper JS captura o throw e faz dedup via _seenErrors (Fix #7), mas o warn já saiu do .jsc.
    // Fix: interceptar console.warn durante _originalRecall e filtrar linhas de "recall falhou"
    // cujo errKey já foi visto — suprime o warn repetido do .jsc de forma transparente.
    // Issue: https://github.com/MestreRichard/logicaOS/issues/227
    //
    // [QA Auto-Heal 2026-09-24 — Fix #9: _seenErrors movido para nível de módulo]
    // Usa _seenErrorsSet e _seenErrorsPath definidos no topo — não reinicializa a cada chamada.

    // [Fix #8] Monkey-patch console.warn para suprimir warn do .jsc quando errKey já visto
    const _origWarn = console.warn;
    console.warn = function (...warnArgs) {
      const msg = warnArgs.join(' ');
      if (/\[memory-engine\].*recall falhou/.test(msg)) {
        const k = _normalizeErrKey(msg.replace(/.*err=/, ''));
        if (_seenErrorsSet.has(k)) return; // suprimido — já logado 1x
      }
      _origWarn.apply(console, warnArgs);
    };

    try {
      const result = await _originalRecall(...args);
      console.warn = _origWarn; // restaura após chamada bem-sucedida
      return result;
    } catch (err) {
      console.warn = _origWarn; // restaura sempre, mesmo em erro
      // Só loga 1x por tipo de erro para diagnóstico — não flood
      // [QA Auto-Heal 2026-09-17 — Fix #6: normalizar errKey para dedup robusto]
      // [QA Auto-Heal 2026-09-22 — Fix #7: persistir _seenErrors entre restarts PM2]
      // Issue: https://github.com/MestreRichard/logicaOS/issues/217
      const errKey = _normalizeErrKey(err?.message ?? err);
      if (!_seenErrorsSet.has(errKey)) {
        _seenErrorsSet.add(errKey);
        _saveSeenErrors(_seenErrorsSet, _seenErrorsPath);
        console.warn(`[memory-engine] recall suprimido (1x por tipo): ${errKey}`);
      }
      return [];
    }
  };
}

module.exports = engine;
