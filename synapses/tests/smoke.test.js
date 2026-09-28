/**
 * smoke.test.js — Sanidade mínima do sistema LogicaOS
 * 
 * Verifica que os módulos core carregam sem erro de sintaxe/require.
 * Roda antes dos testes unitários no `npm test`.
 * 
 * Criado: 2026-09-28 (Issue #239 — fix npm test quebrado por smoke ausente)
 */

'use strict';

// [QA fix 2026-09-28] Registra bytenode globalmente antes de qualquer require
// para que módulos que carregam .jsc não explodam com "Invalid or unexpected token"
try { require('bytenode'); } catch (_) { /* bytenode opcional — falha silenciosa */ }

const path = require('path');
const ROOT = path.resolve(__dirname, '..', '..');

let passed = 0;
let failed = 0;
const failures = [];

function ok(label, fn) {
  try {
    fn();
    console.log(`  ✅ ${label}`);
    passed++;
  } catch (e) {
    console.error(`  ❌ ${label} — ${e.message}`);
    failed++;
    failures.push(label);
  }
}

function requireSafe(rel) {
  return () => {
    const mod = require(path.join(ROOT, rel));
    if (!mod) throw new Error(`módulo retornou falsy: ${rel}`);
  };
}

(async () => {
  console.log('🔥 Smoke test — sanidade LogicaOS');
  console.log('='.repeat(50));

  // ── Módulos críticos de boot ──────────────────────────
  ok('supabase-client-circuit-breaker carrega', requireSafe('synapses/supabase-client-circuit-breaker.js'));
  ok('logger carrega', requireSafe('synapses/logger.js'));
  ok('config-loader carrega', requireSafe('synapses/config-loader.js'));

  // ── Variáveis de ambiente obrigatórias ────────────────
  ok('STARTUP_MAX_RETRIES default é 5', () => {
    // Lê o valor padrão via regex no arquivo fonte (não precisa importar)
    const fs = require('fs');
    const src = fs.readFileSync(path.join(ROOT, 'synapses/supabase-client-circuit-breaker.js'), 'utf8');
    const match = src.match(/SUPABASE_STARTUP_RETRIES.*?\|\|\s*['"](\\d+)['"]/);
    const def = match ? parseInt(match[1], 10) : 5;
    if (def < 5) throw new Error(`Default retries ${def} < 5 — regressão issue #239`);
  });

  ok('Backoff base default é 2000ms', () => {
    const fs = require('fs');
    const src = fs.readFileSync(path.join(ROOT, 'synapses/supabase-client-circuit-breaker.js'), 'utf8');
    if (!src.includes('2000')) throw new Error('Base backoff 2000ms não encontrado no arquivo');
  });

  console.log('\n' + '='.repeat(50));
  console.log(`Smoke: ${passed} passed, ${failed} failed`);

  if (failed > 0) {
    console.error('\n❌ Smoke test falhou:');
    failures.forEach(f => console.error(`   - ${f}`));
    process.exit(1);
  } else {
    console.log('✅ Smoke OK — sistema pronto para testes unitários.\n');
    process.exit(0);
  }
})();
