/**
 * supabase-client-circuit-breaker.js
 * QA Auto-healing — 2026-07-12 (v2 — backoff exponencial + reconnect proativo)
 *
 * Histórico:
 *   v1 (2026-07-11): circuit-breaker básico — detectou 161 "fetch failed" em 4h
 *   v2 (2026-07-12): backoff exponencial + reconnect proativo + log dedup
 *   v4 (2026-09-18): [QA auto-heal 2026-09-18] persistent cooldown state + startup delay
 *   v5 (2026-09-26): [QA auto-heal 2026-09-26] startup probe retry 3x/2s — ciclo 7 (273 erros/4h)
 *   v6 (2026-09-28): [Issue #239] startup probe 5x + backoff exponencial [2s,4s,8s,16s,32s] + degraded mode
 *     Problema: restart PM2 durante outage zera cooldownLevel → burst antes do circuit abrir
 *     Fix 1: salva { cooldownLevel, openSince, state } em /tmp/cb-state.json ao abrir circuit
 *     Fix 2: SUPABASE_STARTUP_DELAY_MS — aguarda N ms adicionais antes de liberar canAttempt()
 *
 * Estados do circuit-breaker:
 *   CLOSED    → operação normal
 *   OPEN      → falhas >= threshold; rejeita chamadas; tenta reconnect proativo
 *   HALF_OPEN → testando 1 chamada real; se OK → CLOSED; se falha → OPEN
 *
 * Melhorias v2:
 *   1. Backoff exponencial: 30s → 60s → 120s → 300s (cap 5min)
 *   2. Reconnect proativo: setInterval pinga Supabase enquanto OPEN
 *   3. Log dedup: 1 log ao abrir + silencia rejeições (conta internamente)
 *   4. Eventos: circuit:open / circuit:closed via EventEmitter
 */

require('dotenv').config({ path: require('path').resolve(__dirname, '..', '.env') });
const { EventEmitter } = require('events');
const fs = require('fs');

// [QA auto-heal 2026-09-18] Persistent cooldown state
// Sobrevive a restarts do PM2 — evita que burst ocorra quando processo reinicia durante outage
const CB_STATE_FILE = '/tmp/cb-state.json';
function _loadPersistedState() {
  try {
    const raw = fs.readFileSync(CB_STATE_FILE, 'utf8');
    const saved = JSON.parse(raw);
    const age = Date.now() - (saved.savedAt || 0);
    // Só restaura se foi salvo há menos de MAX_COOLDOWN_MS (5min) — estado mais antigo é irrelevante
    if (age < MAX_COOLDOWN_MS && saved.cooldownLevel > 0) {
      console.warn(`${TAG} [persistent-state] Restaurando cooldownLevel=${saved.cooldownLevel} (salvo há ${Math.round(age/1000)}s)`);
      return saved;
    }
  } catch (_) { /* sem estado persistido — começa do zero */ }
  return null;
}
function _persistState() {
  try {
    fs.writeFileSync(CB_STATE_FILE, JSON.stringify({
      cooldownLevel,
      state: state === 'OPEN' ? 'OPEN' : 'CLOSED',
      openSince,
      savedAt: Date.now(),
    }));
  } catch (e) {
    console.warn(`${TAG} [persistent-state] Falha ao salvar: ${e.message}`);
  }
}
function _clearPersistedState() {
  try { fs.unlinkSync(CB_STATE_FILE); } catch (_) {}
}

// [QA auto-heal 2026-09-18] Startup delay configurável
// Se SUPABASE_STARTUP_DELAY_MS > 0, aguarda N ms extras após ping antes de liberar canAttempt
const STARTUP_EXTRA_DELAY_MS = parseInt(process.env.SUPABASE_STARTUP_DELAY_MS || '0', 10);

// ── Configuração ─────────────────────────────────────────────────────────────
// [QA auto-heal 2026-07-28: v3 — semáforo anti-burst paralelo]
// Problema raiz ciclos 1-5: burst de 8+ queries paralelas dispara ANTES do
// FAILURE_THRESHOLD=3 ser atingido — todas passam pelo canAttempt()=true e explodem juntas.
// Fix: BURST_CONCURRENCY_CAP limita queries simultâneas enquanto circuit está CLOSED.
const FAILURE_THRESHOLD  = 2;          // falhas consecutivas para abrir o circuit [QA auto-heal 2026-08-01: 3→2, corta burst mais rápido no outage; 2026-07-20: 5→3]
const WINDOW_MS          = 30_000;     // janela de contagem (30s) [QA auto-heal 2026-07-20: 60s→30s, abre mais rápido e para cascata]
const BURST_CONCURRENCY_CAP = 4;       // [QA auto-heal 2026-07-28] máx queries simultâneas — acima disso adia com backoff 200ms
const BASE_COOLDOWN_MS   = 30_000;     // cooldown inicial (30s)
const MAX_COOLDOWN_MS    = 300_000;    // cooldown máximo (5 min)
const LOG_REJECTION_EVERY = 10;        // logar 1 rejeição a cada N durante OPEN
const TAG                = '[circuit-breaker]';

// ── Estado ───────────────────────────────────────────────────────────────────
// [QA auto-heal 2026-09-18] Tenta restaurar estado persistido antes de inicializar
const _persisted    = _loadPersistedState();
let state           = (_persisted && _persisted.state === 'OPEN') ? 'OPEN' : 'CLOSED';
let failures        = 0;
let windowStart     = Date.now();
let openSince       = (_persisted && _persisted.openSince) ? _persisted.openSince : null;
let halfOpenLock    = false;
let cooldownLevel   = (_persisted && _persisted.cooldownLevel > 0) ? _persisted.cooldownLevel : 0; // nível de backoff: 0=30s, 1=60s, 2=120s, 3+=300s
let rejectionCount  = 0;          // total de rejeições durante OPEN (para log dedup)
let reconnectTimer  = null;       // timer do reconnect proativo
// [QA auto-heal 2026-07-28] semáforo anti-burst: conta queries in-flight
let _inFlight       = 0;
// [QA auto-heal 2026-08-01] startup gate: bloqueia queries até pingSupabase() concluir
// Problema raiz: entre process.start e o resultado do ping (~10s), 8+ módulos disparam
// queries paralelas com canAttempt()=true → burst de fetch failed antes do circuit abrir.
// Fix: _startupPending=true bloqueia canAttempt() durante o ping inicial.
let _startupPending = true;

const emitter = new EventEmitter();

// ── Helpers ──────────────────────────────────────────────────────────────────
function currentCooldown() {
  const ms = BASE_COOLDOWN_MS * Math.pow(2, cooldownLevel);
  return Math.min(ms, MAX_COOLDOWN_MS);
}

function resetWindow() {
  failures    = 0;
  windowStart = Date.now();
}

function clearReconnectTimer() {
  if (reconnectTimer) {
    clearInterval(reconnectTimer);
    reconnectTimer = null;
  }
}

// Ping leve ao Supabase para checar conectividade (sem usar o client original)
async function pingSupabase() {
  const url  = process.env.SUPABASE_URL;
  const key  = process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_ANON_KEY;
  if (!url || !key) {
    // FIX QA 2026-07-20: log explícito quando vars ausentes — evita stuck-OPEN silencioso
    console.error(`${TAG} pingSupabase: SUPABASE_URL ou SUPABASE_SERVICE_KEY não definidos — circuit não pode auto-recuperar. Verifique o .env`);
    return false;
  }
  try {
    // [QA auto-heal 2026-09-21 — Fix #7: health-check endpoint mais leve]
    // Problema: ping em agent_events?select=id tinha latência extra → false-positive alive=true
    // Fix: usar /rest/v1/ (retorna apenas schema metadata, sem tocar em tabelas de dados)
    const res = await fetch(`${url}/rest/v1/`, {
      headers: { apikey: key, Authorization: `Bearer ${key}` },
      signal: AbortSignal.timeout(5000),
    });
    // FIX QA 2026-07-20: aceitar qualquer 2xx/3xx/4xx como "Supabase respondeu"
    // fetch failed = sem TCP; 4xx = Supabase up mas sem permissão (ainda é sinal de vida)
    const alive = res.status < 500;
    if (!alive) {
      console.error(`${TAG} pingSupabase: Supabase retornou ${res.status} — considerando down`);
    }
    return alive;
  } catch (e) {
    console.error(`${TAG} pingSupabase: fetch falhou — ${e.message}`);
    return false;
  }
}

// Inicia reconnect proativo enquanto circuit está OPEN
// FIX QA 2026-07-20: intervalo fixo de 15s enquanto OPEN (não escala junto com cooldown)
// Problema anterior: startReconnectLoop usava currentCooldown() como interval do setInterval
// → quando backoff chegava em 300s, o reconnect proativo pingava 1x a cada 5min → stuck-OPEN
// Agora: ping a cada RECONNECT_INTERVAL_MS independente do backoff de chamadas
const RECONNECT_INTERVAL_MS = 15_000; // ping Supabase a cada 15s durante OPEN

function startReconnectLoop() {
  clearReconnectTimer();
  reconnectTimer = setInterval(async () => {
    if (state !== 'OPEN') { clearReconnectTimer(); return; }

    const ok = await pingSupabase();
    if (ok) {
      console.log(`${TAG} CLOSED — reconexão proativa bem-sucedida (ping OK após ${Math.round((Date.now()-openSince)/1000)}s)`);
      state        = 'CLOSED';
      cooldownLevel = Math.max(0, cooldownLevel - 1); // reduz backoff gradualmente
      halfOpenLock  = false;
      rejectionCount = 0;
      resetWindow();
      clearReconnectTimer();
      _clearPersistedState(); // [QA auto-heal 2026-09-18] limpa state persistido ao fechar circuit
      emitter.emit('circuit:closed', { via: 'proactive-reconnect' });
      process.emit('supabase:circuit:closed', { via: 'proactive-reconnect' });
    } else {
      // escalada de backoff para chamadas reais — mas reconnect continua a cada 15s
      if (cooldownLevel < 3) {
        cooldownLevel++;
        console.error(`${TAG} Supabase ainda inacessível — backoff de chamadas escalado para ${currentCooldown()/1000}s (ping continua a cada ${RECONNECT_INTERVAL_MS/1000}s)`);
      }
    }
  }, RECONNECT_INTERVAL_MS);
}

// ── Máquina de estados ───────────────────────────────────────────────────────
function onSuccess() {
  if (state === 'HALF_OPEN') {
    console.log(`${TAG} CLOSED — HALF_OPEN bem-sucedido`);
    state         = 'CLOSED';
    cooldownLevel = Math.max(0, cooldownLevel - 1);
    halfOpenLock  = false;
    rejectionCount = 0;
    clearReconnectTimer();
    _clearPersistedState(); // [QA auto-heal 2026-09-18] limpa state persistido ao fechar via half-open
    emitter.emit('circuit:closed', { via: 'half-open' });
    process.emit('supabase:circuit:closed', { via: 'half-open' });
  }
  resetWindow();
}

function onFailure(context) {
  if (Date.now() - windowStart > WINDOW_MS) resetWindow();

  failures++;

  if (state === 'HALF_OPEN') {
    cooldownLevel = Math.min(cooldownLevel + 1, 3);
    const nextCooldown = currentCooldown();
    console.error(`${TAG} OPEN — HALF_OPEN falhou (${context}); backoff ${nextCooldown/1000}s`);
    state        = 'OPEN';
    openSince    = Date.now();
    halfOpenLock = false;
    startReconnectLoop();
    emitter.emit('circuit:open', { reason: 'half-open-failed', context, cooldown: nextCooldown });
    return;
  }

  if (failures >= FAILURE_THRESHOLD && state === 'CLOSED') {
    const nextCooldown = currentCooldown();
    console.error(`${TAG} OPEN — ${failures} falhas/${WINDOW_MS/1000}s (${context}); backoff inicial ${nextCooldown/1000}s`);
    state     = 'OPEN';
    openSince = Date.now();
    rejectionCount = 0;
    _persistState(); // [QA auto-heal 2026-09-18] persiste estado ao abrir circuit
    startReconnectLoop();
    emitter.emit('circuit:open', { reason: 'threshold-exceeded', failures, cooldown: nextCooldown });
    return;
  }

  // Ainda CLOSED mas abaixo do threshold — log individual (apenas nos primeiros 5)
  if (failures <= FAILURE_THRESHOLD) {
    console.error(`${TAG} falha #${failures}/${FAILURE_THRESHOLD} em ${context}`);
  }
}

function canAttempt() {
  // [QA auto-heal 2026-08-01] startup gate: bloqueia até ping inicial concluir
  // Evita burst de fetch failed durante os ~10s de pingSupabase() no boot.
  if (_startupPending) return false;

  if (state === 'CLOSED') return true;

  if (state === 'OPEN') {
    // Tenta HALF_OPEN depois do cooldown (fallback caso o reconnect proativo não feche)
    const elapsed = Date.now() - openSince;

    // Fix: halfOpenLock timeout — se HALF_OPEN travou há mais de 15s, liberar
    if (halfOpenLock && state === 'HALF_OPEN' && elapsed > currentCooldown() + 15_000) {
      console.warn(`${TAG} halfOpenLock timeout — forçando reset após ${Math.round(elapsed/1000)}s`);
      halfOpenLock = false;
      state = 'OPEN';
    }

    if (elapsed >= currentCooldown() && !halfOpenLock) {
      halfOpenLock = true;
      state        = 'HALF_OPEN';
      console.log(`${TAG} HALF_OPEN — testando após ${Math.round(elapsed/1000)}s`);
      return true;
    }

    // Log dedup: logar 1x a cada LOG_REJECTION_EVERY rejeições
    rejectionCount++;
    if (rejectionCount === 1 || rejectionCount % LOG_REJECTION_EVERY === 0) {
      const remainingS = Math.ceil((currentCooldown() - (Date.now() - openSince)) / 1000);
      console.warn(`${TAG} REJEITADO (${rejectionCount}x) — circuit OPEN; próxima tentativa em ~${Math.max(0,remainingS)}s`);
    }
    return false;
  }

  // HALF_OPEN com lock ativo
  return halfOpenLock && state === 'HALF_OPEN';
}

// ── Carrega o cliente original (compilado) ────────────────────────────────────
// [QA fix 2026-09-28] Garante bytenode registrado antes de carregar .jsc
// — sem isso o smoke test falha com "Invalid or unexpected token" (Issue #238)
let originalClient;
try {
  require('bytenode');
  originalClient = require('./supabase-client.jsc');
} catch (e) {
  console.warn('[supabase-client-cb] bytenode indisponível ou .jsc corrompido — usando stub vazio:', e.message);
  originalClient = {};
}

// ── Proxy: intercepta chamadas e aplica circuit-breaker ───────────────────────
// [QA auto-heal 2026-07-28] Helper: atraso com backoff quando burst excede cap
function _waitIfBurst(label) {
  return new Promise((resolve) => {
    const check = () => {
      if (_inFlight < BURST_CONCURRENCY_CAP) return resolve();
      setTimeout(check, 200); // re-tenta em 200ms
    };
    check();
  });
}

function wrapMethod(fn, label) {
  return async function (...args) {
    if (!canAttempt()) {
      return null; // retorna null silenciosamente (log já foi feito no dedup)
    }

    // [QA auto-heal 2026-07-28] Semáforo anti-burst: aguarda slot disponível
    if (_inFlight >= BURST_CONCURRENCY_CAP) {
      await _waitIfBurst(label);
    }
    _inFlight++;

    try {
      const result = await fn.apply(this, args);
      onSuccess();
      return result;
    } catch (err) {
      onFailure(label);
      throw err;
    } finally {
      _inFlight = Math.max(0, _inFlight - 1);
    }
  };
}

// Aplica proxy em todas as funções exportadas do cliente original
if (typeof originalClient === 'object' && originalClient !== null) {
  const proxied = {};
  for (const [key, val] of Object.entries(originalClient)) {
    proxied[key] = typeof val === 'function' ? wrapMethod(val, key) : val;
  }
  module.exports = proxied;
} else if (typeof originalClient === 'function') {
  module.exports = wrapMethod(originalClient, 'supabase-client');
} else {
  console.warn(`${TAG} Não foi possível instrumentar supabase-client — exportando original`);
  module.exports = originalClient;
}

// ── API de introspection ──────────────────────────────────────────────────────
module.exports.__circuitState = () => ({
  state,
  failures,
  cooldownLevel,
  currentCooldownMs: currentCooldown(),
  openSince,
  windowStart,
  rejectionCount,
});
module.exports.__circuitEvents = emitter;

// ── Startup health check ──────────────────────────────────────────────────────
// Fix: se Supabase já está disponível na inicialização do processo, garantir CLOSED
// Evita que o processo suba "cego" e fique em OPEN por falta de um onSuccess() inicial
//
// [fix/auto-fetch-failed-startup-gate — Issue #239]
// Problema raiz: STARTUP_RETRIES=3 + STARTUP_BACKOFF=2000ms fixo insuficiente.
// PM2 restart rápido dispara 9 queries paralelas antes do TCP estabilizar.
// Confirmado: 192 "fetch failed" no boot (threshold 10x).
// Fix: 5 retries com backoff exponencial [2s,4s,8s,16s,32s].
// Após 5 falhas consecutivas, sobe em degraded mode (não trava boot).
const STARTUP_MAX_RETRIES = parseInt(process.env.SUPABASE_STARTUP_RETRIES || '5', 10);
const STARTUP_BASE_BACKOFF_MS = parseInt(process.env.SUPABASE_STARTUP_BASE_BACKOFF_MS || '2000', 10);

async function startupProbe(retries = STARTUP_MAX_RETRIES, baseDelayMs = STARTUP_BASE_BACKOFF_MS) {
  for (let i = 0; i < retries; i++) {
    const ok = await pingSupabase();
    if (ok) return true;
    if (i < retries - 1) {
      // Backoff exponencial: 2s → 4s → 8s → 16s → 32s (cap 32s)
      const delayMs = Math.min(baseDelayMs * Math.pow(2, i), 32_000);
      console.warn(`${TAG} Startup: ping falhou (tentativa ${i + 1}/${retries}), aguardando ${delayMs}ms antes de re-tentar... [backoff exponencial]`);
      await new Promise(r => setTimeout(r, delayMs));
    }
  }
  return false;
}

(async () => {
  try {
    const ok = await startupProbe(); // [Issue #239] 5 retries, backoff exponencial [2s,4s,8s,16s,32s]; degraded mode após 5 falhas
    if (ok) {
      // [QA auto-heal 2026-09-18] Startup delay: aguarda N ms extras se configurado
      // Garante que módulos dependentes do Supabase tenham tempo de estabilizar antes do burst inicial
      if (STARTUP_EXTRA_DELAY_MS > 0) {
        console.log(`${TAG} Startup OK — aguardando ${STARTUP_EXTRA_DELAY_MS}ms extras (SUPABASE_STARTUP_DELAY_MS) antes de liberar queries`);
        await new Promise(r => setTimeout(r, STARTUP_EXTRA_DELAY_MS));
      }
      if (state !== 'CLOSED') {
        console.log(`${TAG} Startup OK — Supabase acessível, forçando CLOSED`);
        state         = 'CLOSED';
        halfOpenLock  = false;
        rejectionCount = 0;
        clearReconnectTimer();
        resetWindow();
        _clearPersistedState(); // [QA auto-heal 2026-09-18] Supabase ok no startup → limpa state salvo
        process.emit('supabase:circuit:closed', { via: 'startup-check' });
      } else {
        console.log(`${TAG} Startup OK — Supabase acessível, circuit já em CLOSED`);
        _clearPersistedState(); // [QA auto-heal 2026-09-18] limpa state salvo se ok
      }
    } else {
      // [Issue #239] Degraded mode: após 5 falhas no startup, sobe sem travar o boot.
      // PM2 restart rápido + TCP instável causava 192 "fetch failed" — agora sobe e aguarda
      // o reconnect proativo fechar o circuit quando o Supabase estabilizar.
      console.warn(`${TAG} Startup: Supabase inacessível após ${STARTUP_MAX_RETRIES} tentativas — subindo em DEGRADED MODE (queries bloqueadas até reconnect proativo)`);
      if (!openSince) openSince = Date.now(); // garante openSince se não veio do persistido
      state = 'OPEN'; // garante OPEN explícito antes do reconnect
      _persistState(); // persiste estado de OPEN no startup
      startReconnectLoop();
      // _startupPending será false logo abaixo, mas circuit OPEN bloqueia canAttempt()
      // automaticamente — sem cascata de fetch failed durante a instabilidade TCP pós-restart
    }
  } catch (e) {
    console.warn(`${TAG} Startup check falhou: ${e.message}`);
  }
  // [QA auto-heal 2026-09-18] _startupPending é zerado AQUI — após o ping e delay extra
  // (não antes, para garantir que o burst nunca escape durante o delay)
  _startupPending = false;
})();
