require('dotenv').config({ path: require('path').resolve(__dirname, '..', '.env') });
require('bytenode');

// ── QA Auto-Heal 2026-08-01 v2: process.stderr.write intercept (nível mais baixo) ─
// Problema: dedup via console.error é bypassado quando sub-módulos carregados antes
// do wrap já emitiram logs, ou quando usam process.stderr.write diretamente.
// Fix: interceptar no nível de process.stderr.write — imune a qualquer wrapper anterior.
// Scope: apenas mensagens de flood conhecidas (NOTION_TOKEN). Circuit-breaker e erros
// reais continuam passando normalmente.
const _origStderrWrite = process.stderr.write.bind(process.stderr);
const _FLOOD_PATTERNS = [
  '[server] NOTION_TOKEN não definido',
];
process.stderr.write = function (chunk, encoding, callback) {
  if (typeof chunk === 'string') {
    for (const pattern of _FLOOD_PATTERNS) {
      if (chunk.includes(pattern)) {
        // Silencia completamente — sem contagem, sem leak em restart
        if (typeof encoding === 'function') encoding(); // callback quando encoding é fn
        else if (typeof callback === 'function') callback();
        return true;
      }
    }
  }
  return _origStderrWrite(chunk, encoding, callback);
};
// ── fim stderr intercept ──────────────────────────────────────────────────────

// ── QA Auto-Heal 2026-07-30: dedup de logs repetitivos no stderr ─────────────
// Problema: server.jsc loga "NOTION_TOKEN não definido" a cada requisição/ciclo.
// Fix: interceptar console.error/console.warn e suprimir mensagens idênticas após N ocorrências.
// Abordagem: wrapper no ponto de entrada (server.js) antes de carregar o .jsc compilado.
// Não altera o .jsc — 100% seguro.
const _DEDUP_LIMIT = 1; // max ocorrências permitidas por mensagem única
const _dedupCounts = new Map();
const _originalError = console.error.bind(console);
const _originalWarn  = console.warn.bind(console);

function _dedupLog(originalFn, args) {
  const key = String(args[0] ?? '').slice(0, 40);
  const count = (_dedupCounts.get(key) ?? 0) + 1;
  _dedupCounts.set(key, count);
  if (count <= _DEDUP_LIMIT) {
    originalFn(...args);
  } else if (count === _DEDUP_LIMIT + 1) {
    originalFn(`[server-dedup] Mensagem suprimida após ${_DEDUP_LIMIT}x: "${key.slice(0, 80)}..."`);
  }
  // acima do limite: silêncio total
}

console.error = (...args) => _dedupLog(_originalError, args);
console.warn  = (...args) => _dedupLog(_originalWarn,  args);
// ── fim dedup ─────────────────────────────────────────────────────────────────

// ── QA Auto-Heal Fix #11 — 2026-09-27: auto-migrate assíncrono pós-reconexão [auto-002] ──
// Problema (Fix #10, 2026-09-25): migrate.jsc era chamado sincronamente no boot,
// mas nos 6 restarts registrados o Supabase estava offline → falha silenciosa no try/catch
// → schema v1.7 nunca aplicado → [schema-check] continuava emitindo 26 warnings/4h.
// Root cause: race condition — migrate executa antes do circuit-breaker confirmar conectividade.
//
// Fix #11: estratégia dual:
//   1. Se circuit já CLOSED no boot (Supabase disponível) → migrate imediato via setImmediate
//   2. Se circuit OPEN/HALF_OPEN → aguarda evento 'supabase:circuit:closed' (emitido pelo CB)
// Guard: AUTO_MIGRATE=false desabilita (útil em ambientes onde migration é manual).
// Detectado por: QA Auto-Heal 2026-09-27 | agent_events auto-002
if (process.env.AUTO_MIGRATE !== 'false') {
  function _runMigrate(via) {
    try {
      require('./migrate');
      _originalWarn('[startup] auto-migrate executado (' + via + ')');
    } catch (e) {
      // Não fatal — server sobe mesmo se migrate falhar
      _originalWarn('[startup] auto-migrate falhou (non-fatal) via ' + via + ':', e.message);
    }
  }

  // Verificar estado atual do circuit-breaker (pode estar CLOSED desde o boot)
  try {
    const _cb = require('./supabase-client-circuit-breaker');
    const _state = _cb.__circuitState?.();
    if (_state && _state.state === 'CLOSED') {
      // Supabase disponível agora — executar imediatamente após carregar server.jsc
      setImmediate(() => _runMigrate('boot-imediato'));
    } else {
      // OPEN ou HALF_OPEN — aguardar reconexão proativa (circuit-breaker emite este evento)
      process.once('supabase:circuit:closed', () => _runMigrate('pos-reconexao'));
    }
  } catch (_) {
    // circuit-breaker não disponível — fallback para comportamento anterior (melhor que nada)
    setImmediate(() => _runMigrate('fallback'));
  }
}
// ── fim auto-migrate ──────────────────────────────────────────────────────────

module.exports = require('./server.jsc');
