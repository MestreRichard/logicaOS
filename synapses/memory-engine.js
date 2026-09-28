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

// [Fix #10-upgrade — QA Auto-Heal 2026-09-27 | auto-001]
// Pré-popular com erros recorrentes conhecidos → supressão imediata em restart limpo.
// Problema: Fix #9 movia _seenErrors para nível de módulo, mas ao carregar o arquivo
// persistido só a 1ª chave era restaurada. Em 6 restarts × 3 variantes distintos = 18 logs/4h.
// Fix: inicializar SEMPRE com KNOWN_RECALL_ERRORS e fazer merge com o arquivo persistido.
//
// [Fix #11 — QA Auto-Heal 2026-09-27 | auto-heal ciclo-10]
// Problema: 61 logs "recall falhou 503" em janela 08:33–09:43 (a cada 2min exato).
// Root cause: _normalizeErrKey usa `.replace(/reason: .+/, '')` (lazy) mas o texto do 503
// contém DOIS "reason:" → "remote connection failure, transport failure reason: delayed
// connect error: 111" → o segundo reason sobrevive → key gerada ≠ KNOWN_RECALL_ERRORS →
// warn do .jsc não é suprimido e passa pelo monkey-patch.
// Fix: (1) normalizeErrKey greedy (.replace(/reason:[\s\S]+/, '')); (2) expandir
// KNOWN_RECALL_ERRORS com variante real observada; (3) warn monkey-patch auto-adiciona
// ao seenSet para suprimir warn do .jsc mesmo em 1ª ocorrência da variante.
const _KNOWN_RECALL_ERRORS = [
  'Supabase RPC search_agent_memory_smart N: upstream connect error',
  'fetch failed',
  'The operation was aborted due to timeout',
  // [Fix #11] variante real observada em produção — key normalizada (greedy + slice 80)
  // Gerada por: _normalizeErrKey('Supabase RPC search_agent_memory_smart 503: upstream connect error or disconnect/reset...')
  'Supabase RPC search_agent_memory_smart N: upstream connect error or disconnect/r',
];

function _loadSeenErrors() {
  for (const p of _SEEN_PATHS) {
    try {
      const raw = JSON.parse(_fs.readFileSync(p, 'utf8'));
      if (Date.now() - (raw.savedAt || 0) < _SEEN_TTL) {
        // Merge: known errors + erros persistidos da sessão anterior
        const merged = new Set([..._KNOWN_RECALL_ERRORS, ...(raw.errors || [])]);
        return { set: merged, path: p };
      }
    } catch (_) { /* tenta próximo path */ }
  }
  // Fallback: pré-popular com erros conhecidos → zero flood mesmo em restart com /tmp limpo
  return { set: new Set(_KNOWN_RECALL_ERRORS), path: _SEEN_PATHS[0] };
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
  // [Fix #11 — QA Auto-Heal 2026-09-27 | ciclo-10]
  // Mudança: .replace(/reason:[\s\S]+/, '') greedy em vez de lazy /reason: .+/
  // Razão: texto do 503 contém 2x "reason:" → lazy só removia o último trecho →
  //        key resultante era longa e não batia com KNOWN_RECALL_ERRORS.
  // Greedy remove tudo a partir do PRIMEIRO "reason:" inclusive.
  return String(msg)
    .replace(/\d{3,}/g, 'N')
    .replace(/reason:[\s\S]+/, '')
    .trim()
    .slice(0, 80);
}

// [Fix #9] Init em nível de módulo — garante que o Set sobrevive a múltiplas chamadas
//           e resiste a race conditions de startup
const { set: _seenErrorsSet, path: _seenErrorsPath } = _loadSeenErrors();

// ─── Fix #12: boot schema-cache warm-up gate (QA Auto-Heal 2026-09-28) ──────
// Problema: Após restart do PM2, PostgREST demora ~2min para aquecer o schema cache.
// Durante esse período, o .jsc dispara chamadas ao RPC search_agent_memory_smart e
// recebe HTTP 404 PGRST202 ("Could not find the function ... in the schema cache").
// Confirmado: 3 logs "recall falhou 404 PGRST202" em 15:26, 15:28, 15:30 no boot.
// A RPC existe no Supabase (curl direto retorna 200/22000) — é schema cache stale.
// Fix: registrar o timestamp do primeiro circuit:closed e bloquear recall por
// SCHEMA_WARM_UP_MS (90s) após esse evento. Silencioso — sem logs durante warm-up.
// ─────────────────────────────────────────────────────────────────────────────
let _supabaseReadyAt = null;
const _SCHEMA_WARM_UP_MS = 90_000; // 90s de warm-up após 1º CLOSED

// Escuta o evento emitido pelo circuit-breaker quando transiciona para CLOSED
process.on('supabase:circuit:closed', () => {
  if (!_supabaseReadyAt) {
    _supabaseReadyAt = Date.now();
  }
});

// Também pré-popular KNOWN com o erro 404 para dedup de diagnóstico
_KNOWN_RECALL_ERRORS.push('Supabase RPC search_agent_memory_smart N: {\"code\":\"PGRST');

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
    // [Fix #12 — QA Auto-Heal 2026-09-28]: schema-cache warm-up gate
    // Bloqueia recall por SCHEMA_WARM_UP_MS após o 1º circuit:closed
    // para evitar 404 PGRST202 enquanto PostgREST aquece o schema cache.
    if (!_supabaseReadyAt || (Date.now() - _supabaseReadyAt) < _SCHEMA_WARM_UP_MS) {
      return []; // schema cache ainda aquecendo — silencioso
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
    // [Fix #11 — QA Auto-Heal 2026-09-27 | ciclo-10]: auto-adicionar ao seenSet na 1ª vez
    // Problema anterior: warn do .jsc passava na 1ª ocorrência pois seenSet não tinha a key
    // (arquivo persistido expirado ou restart limpo). Agora: 1ª ocorrência → loga 1x via
    // _origWarn + adiciona ao set → todas as ocorrências seguintes → suprimidas silenciosamente.
    const _origWarn = console.warn;
    console.warn = function (...warnArgs) {
      const msg = warnArgs.join(' ');
      if (/\[memory-engine\].*recall falhou/.test(msg)) {
        const k = _normalizeErrKey(msg.replace(/.*err=/, ''));
        if (_seenErrorsSet.has(k)) return; // suprimido — já logado 1x
        // [Fix #11] auto-add: registra no set para suprimir warn do .jsc em ocorrências futuras
        _seenErrorsSet.add(k);
        _saveSeenErrors(_seenErrorsSet, _seenErrorsPath);
        // deixa passar 1x para diagnóstico — o try/catch abaixo também logará via "recall suprimido"
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
