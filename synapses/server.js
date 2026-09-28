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

// ── QA Auto-Heal Fix #10 — 2026-09-25: auto-migrate no startup [#232] ─────────
// Problema: migrate.js nunca executava automaticamente → schema v1.7 ficava incompleto
// a cada restart, disparando [schema-check] 20x/4h e deixando token_budget_daily,
// team_logs e coordination_events INATIVOS.
// Fix: executar migrate.jsc no boot, ANTES de carregar server.jsc.
// Guard: AUTO_MIGRATE=false desabilita (útil em ambientes onde migration é manual).
if (process.env.AUTO_MIGRATE !== 'false') {
  try {
    require('./migrate');
  } catch (e) {
    // Não fatal — server sobe mesmo se migrate falhar (infra pode estar indisponível)
    console.warn('[startup] auto-migrate falhou (non-fatal):', e.message);
  }
}
// ── fim auto-migrate ──────────────────────────────────────────────────────────

module.exports = require('./server.jsc');
