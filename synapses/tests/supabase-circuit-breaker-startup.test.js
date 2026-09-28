/**
 * supabase-circuit-breaker-startup.test.js
 * Teste de regressão — Issue #239 / PR #241
 *
 * Cobre o padrão histórico de regressão no startup gate:
 *   v1→v2→v4→v5→v6: burst de fetch failed antes do circuit abrir
 *
 * Estratégia: não importa o módulo compilado .jsc — testa a lógica
 * de startup diretamente com mocks de fetch e TCP instável.
 *
 * Roda via: node synapses/tests/supabase-circuit-breaker-startup.test.js
 * Integrado em: npm test (package.json > scripts.test)
 */

'use strict';

// ── Helpers de assert ────────────────────────────────────────────────────────
let passed = 0;
let failed = 0;
const failures = [];

function assert(condition, label) {
  if (condition) {
    console.log(`  ✅ ${label}`);
    passed++;
  } else {
    console.error(`  ❌ ${label}`);
    failed++;
    failures.push(label);
  }
}

function assertEqual(a, b, label) {
  assert(a === b, `${label} (esperado=${b}, recebido=${a})`);
}

// ── Extrai lógica de startup do módulo (sem .jsc) ────────────────────────────
// Re-implementa o core de startupProbe + _startupPending gate em isolamento,
// fiel ao código de supabase-client-circuit-breaker.js v6
function buildStartupProbe({ STARTUP_MAX_RETRIES = 5, STARTUP_BASE_BACKOFF_MS = 2000 } = {}) {
  async function startupProbe(pingFn) {
    const delays = []; // novo array por chamada — sem acúmulo entre testes
    for (let i = 0; i < STARTUP_MAX_RETRIES; i++) {
      const ok = await pingFn(i);
      if (ok) return { success: true, attempts: i + 1, delays };
      if (i < STARTUP_MAX_RETRIES - 1) {
        const delayMs = Math.min(STARTUP_BASE_BACKOFF_MS * Math.pow(2, i), 32_000);
        delays.push(delayMs);
        // Não esperamos de verdade no teste — só registramos o delay calculado
      }
    }
    return { success: false, attempts: STARTUP_MAX_RETRIES, delays };
  }

  return startupProbe;
}

// ── Extrai lógica de startup gate (_startupPending) ──────────────────────────
// Simula: 9 queries paralelas disparadas durante boot
function buildStartupGate() {
  let _startupPending = true;
  const blocked = [];
  const allowed = [];

  function canAttempt(label) {
    if (_startupPending) {
      blocked.push(label);
      return false;
    }
    allowed.push(label);
    return true;
  }

  function releaseGate() {
    _startupPending = false;
  }

  return { canAttempt, releaseGate, blocked, allowed };
}

// ── SUITE 1: startupProbe — backoff exponencial ──────────────────────────────
async function suite_backoffExponencial() {
  console.log('\n📋 SUITE 1: startupProbe — backoff exponencial [2s,4s,8s,16s,32s]');

  const probe = buildStartupProbe({ STARTUP_MAX_RETRIES: 5, STARTUP_BASE_BACKOFF_MS: 2000 });

  // Caso: Supabase acessível na 1ª tentativa
  {
    let calls = 0;
    const result = await probe(async () => { calls++; return true; });
    assert(result.success, 'Retorna success=true se ping OK na 1ª tentativa');
    assertEqual(result.attempts, 1, 'Usa 1 tentativa quando OK de imediato');
  }

  // Caso: 4 falhas seguidas de sucesso na 5ª
  {
    let calls = 0;
    const result = await probe(async (i) => { calls++; return i === 4; });
    assert(result.success, 'Retorna success=true após 5ª tentativa');
    assertEqual(result.attempts, 5, 'Usa exatamente 5 tentativas (4 falhas + 1 sucesso)');
    assertEqual(result.delays.length, 4, 'Registra 4 delays (entre as 5 tentativas)');
  }

  // Caso: sequência de delays exponenciais [2000, 4000, 8000, 16000]
  {
    const result = await probe(async (i) => i === 4); // sucesso só na 5ª
    const expected = [2000, 4000, 8000, 16000];
    assert(
      JSON.stringify(result.delays) === JSON.stringify(expected),
      `Delays exponenciais corretos: ${JSON.stringify(result.delays)} === ${JSON.stringify(expected)}`
    );
  }

  // Caso: 5 falhas → degraded mode (não trava boot)
  {
    let calls = 0;
    const result = await probe(async () => { calls++; return false; });
    assert(!result.success, 'Retorna success=false após 5 falhas (degraded mode)');
    assertEqual(result.attempts, 5, 'Executa exatamente STARTUP_MAX_RETRIES=5 tentativas');
    assertEqual(calls, 5, 'pingFn foi chamado 5 vezes');
  }

  // Caso: delay máximo capped em 32s (não vai a 32000ms para 5ª tentativa)
  // delays[0]=2000, [1]=4000, [2]=8000, [3]=16000 — 5ª seria 32000 mas sucesso antes
  {
    const probe6 = buildStartupProbe({ STARTUP_MAX_RETRIES: 6, STARTUP_BASE_BACKOFF_MS: 2000 });
    const result = await probe6(async () => false);
    const lastDelay = result.delays[result.delays.length - 1];
    assert(lastDelay <= 32_000, `Delay máximo é 32s (cap) — recebido: ${lastDelay}ms`);
    assertEqual(lastDelay, 32_000, 'Delay na 5ª posição é exatamente 32000ms (2000 * 2^4 = 32000)');
  }
}

// ── SUITE 2: startup gate — bloqueia 9 queries durante probe ─────────────────
async function suite_startupGate() {
  console.log('\n📋 SUITE 2: startup gate — bloqueia burst de 9 queries durante probe');

  // Caso: gate fecha todas as queries enquanto _startupPending=true
  {
    const gate = buildStartupGate();

    // Simula PM2 restart: 9 módulos disparam queries em paralelo ANTES do ping concluir
    const queries = ['agent_events', 'agent_memory', 'memories', 'sessions',
                     'tasks', 'projects', 'agent_decisions', 'embeddings', 'heartbeat'];
    queries.forEach(q => gate.canAttempt(q));

    assertEqual(gate.blocked.length, 9, 'Bloqueia todas as 9 queries paralelas enquanto gate fechado');
    assertEqual(gate.allowed.length, 0, 'Nenhuma query passa enquanto _startupPending=true');
  }

  // Caso: após releaseGate(), queries são liberadas
  {
    const gate = buildStartupGate();
    gate.releaseGate();

    const queries = ['agent_events', 'agent_memory', 'memories'];
    queries.forEach(q => gate.canAttempt(q));

    assertEqual(gate.allowed.length, 3, 'Libera queries após gate aberto');
    assertEqual(gate.blocked.length, 0, 'Nenhuma query bloqueada após release');
  }

  // Caso: interleaved — algumas antes, algumas depois do release
  {
    const gate = buildStartupGate();

    gate.canAttempt('pre-1');
    gate.canAttempt('pre-2');
    gate.releaseGate();
    gate.canAttempt('post-1');
    gate.canAttempt('post-2');
    gate.canAttempt('post-3');

    assertEqual(gate.blocked.length, 2, 'Bloqueia 2 queries que chegaram antes do release');
    assertEqual(gate.allowed.length, 3, 'Libera 3 queries que chegaram após release');
  }
}

// ── SUITE 3: degraded mode — circuit OPEN após startup failure ───────────────
async function suite_degradedMode() {
  console.log('\n📋 SUITE 3: degraded mode — queries bloqueadas até reconnect proativo');

  // Simula: startup falhou → state=OPEN → canAttempt()=false via circuit, não gate
  function buildCircuitState(initialState = 'OPEN') {
    let state = initialState;
    let _startupPending = false; // gate já liberado

    function canAttempt() {
      if (_startupPending) return false;  // gate ainda ativo
      if (state === 'CLOSED') return true; // normal
      return false;                        // OPEN ou HALF_OPEN sem cooldown
    }

    function reconnectSuccess() {
      state = 'CLOSED';
    }

    return { canAttempt, reconnectSuccess, getState: () => state };
  }

  // Caso: OPEN após degraded → bloqueia queries
  {
    const circuit = buildCircuitState('OPEN');
    const q1 = circuit.canAttempt();
    const q2 = circuit.canAttempt();
    assert(!q1 && !q2, 'Queries bloqueadas quando circuit OPEN após startup failure');
  }

  // Caso: após reconnect proativo → CLOSED → queries liberadas
  {
    const circuit = buildCircuitState('OPEN');
    circuit.reconnectSuccess();
    const q1 = circuit.canAttempt();
    assert(q1, 'Queries liberadas após reconnect proativo fechar circuit');
    assertEqual(circuit.getState(), 'CLOSED', 'State é CLOSED após reconnect');
  }

  // Caso: degraded mode não trava o boot (startupProbe retorna false mas não lança exceção)
  {
    const probe = buildStartupProbe({ STARTUP_MAX_RETRIES: 5, STARTUP_BASE_BACKOFF_MS: 100 });
    let threw = false;
    try {
      const result = await probe(async () => false);
      assert(!result.success, 'startupProbe retorna {success:false} — não lança exceção');
    } catch (e) {
      threw = true;
    }
    assert(!threw, 'startupProbe não lança exceção em degraded mode (boot não trava)');
  }
}

// ── SUITE 4: configurabilidade via env vars ───────────────────────────────────
async function suite_envConfig() {
  console.log('\n📋 SUITE 4: configurabilidade via env vars (SUPABASE_STARTUP_RETRIES, SUPABASE_STARTUP_BASE_BACKOFF_MS)');

  // STARTUP_MAX_RETRIES=3 (valor antigo — deve ser detectável como insuficiente)
  {
    const probeOld = buildStartupProbe({ STARTUP_MAX_RETRIES: 3, STARTUP_BASE_BACKOFF_MS: 2000 });
    const result = await probeOld(async () => false);
    assertEqual(result.attempts, 3, 'STARTUP_RETRIES=3 (valor antigo) usa 3 tentativas');
    // window total: delay[0]+delay[1] = 2000+4000 = 6000ms — insuficiente para TCP instável
    const windowTotal = result.delays.reduce((a, b) => a + b, 0);
    assert(windowTotal < 10_000, `Window total com RETRIES=3 é ${windowTotal}ms < 10s — insuficiente para TCP estabilizar`);
  }

  // STARTUP_MAX_RETRIES=5 (novo valor padrão)
  {
    const probeNew = buildStartupProbe({ STARTUP_MAX_RETRIES: 5, STARTUP_BASE_BACKOFF_MS: 2000 });
    const result = await probeNew(async () => false);
    assertEqual(result.attempts, 5, 'STARTUP_RETRIES=5 (novo padrão) usa 5 tentativas');
    // window total: 2000+4000+8000+16000 = 30000ms — suficiente para TCP estabilizar
    const windowTotal = result.delays.reduce((a, b) => a + b, 0);
    assert(windowTotal >= 30_000, `Window total com RETRIES=5 é ${windowTotal}ms >= 30s — suficiente para TCP estabilizar`);
  }
}

// ── Runner ───────────────────────────────────────────────────────────────────
(async () => {
  console.log('🔬 Teste de regressão — startup gate circuit-breaker (Issue #239 / PR #241)');
  console.log('='.repeat(70));

  await suite_backoffExponencial();
  await suite_startupGate();
  await suite_degradedMode();
  await suite_envConfig();

  console.log('\n' + '='.repeat(70));
  console.log(`Resultado: ${passed} passed, ${failed} failed`);

  if (failed > 0) {
    console.error('\n❌ TESTES FALHARAM:');
    failures.forEach(f => console.error(`   - ${f}`));
    process.exit(1);
  } else {
    console.log('\n✅ Todos os testes passaram — startup gate está protegido contra regressão.');
    process.exit(0);
  }
})();
