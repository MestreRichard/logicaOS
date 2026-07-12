/**
 * supabase-client-circuit-breaker.js
 * QA Auto-healing — 2026-07-12 (v2 — backoff exponencial + reconnect proativo)
 *
 * Histórico:
 *   v1 (2026-07-11): circuit-breaker básico — detectou 161 "fetch failed" em 4h
 *   v2 (2026-07-12): backoff exponencial + reconnect proativo + log dedup
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

// ── Configuração ─────────────────────────────────────────────────────────────
const FAILURE_THRESHOLD  = 5;          // falhas consecutivas para abrir o circuit
const WINDOW_MS          = 60_000;     // janela de contagem (1 min)
const BASE_COOLDOWN_MS   = 30_000;     // cooldown inicial (30s)
const MAX_COOLDOWN_MS    = 300_000;    // cooldown máximo (5 min)
const LOG_REJECTION_EVERY = 10;        // logar 1 rejeição a cada N durante OPEN
const TAG                = '[circuit-breaker]';

// ── Estado ───────────────────────────────────────────────────────────────────
let state           = 'CLOSED';
let failures        = 0;
let windowStart     = Date.now();
let openSince       = null;
let halfOpenLock    = false;
let cooldownLevel   = 0;          // nível de backoff: 0=30s, 1=60s, 2=120s, 3+=300s
let rejectionCount  = 0;          // total de rejeições durante OPEN (para log dedup)
let reconnectTimer  = null;       // timer do reconnect proativo

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
  if (!url || !key) return false;
  try {
    const res = await fetch(`${url}/rest/v1/agent_events?select=id&limit=1`, {
      headers: { apikey: key, Authorization: `Bearer ${key}` },
      signal: AbortSignal.timeout(5000),
    });
    return res.ok || res.status === 406; // 406 = tabela existe mas sem rows (também é OK)
  } catch {
    return false;
  }
}

// Inicia reconnect proativo enquanto circuit está OPEN
function startReconnectLoop() {
  clearReconnectTimer();
  const interval = currentCooldown();
  reconnectTimer = setInterval(async () => {
    if (state !== 'OPEN') { clearReconnectTimer(); return; }

    const ok = await pingSupabase();
    if (ok) {
      console.log(`${TAG} CLOSED — reconexão proativa bem-sucedida (ping OK)`);
      state        = 'CLOSED';
      cooldownLevel = Math.max(0, cooldownLevel - 1); // reduz backoff gradualmente
      halfOpenLock  = false;
      rejectionCount = 0;
      resetWindow();
      clearReconnectTimer();
      emitter.emit('circuit:closed', { via: 'proactive-reconnect' });
    } else {
      // escalada de backoff: aumenta o nível e reinicia o timer com novo intervalo
      if (cooldownLevel < 3) {
        cooldownLevel++;
        console.error(`${TAG} Supabase ainda inacessível — backoff escalado para ${currentCooldown()/1000}s`);
        startReconnectLoop(); // reinicia com novo intervalo
      }
    }
  }, interval);
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
    emitter.emit('circuit:closed', { via: 'half-open' });
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
  if (state === 'CLOSED') return true;

  if (state === 'OPEN') {
    // Tenta HALF_OPEN depois do cooldown (fallback caso o reconnect proativo não feche)
    const elapsed = Date.now() - openSince;
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
const originalClient = require('./supabase-client.jsc');

// ── Proxy: intercepta chamadas e aplica circuit-breaker ───────────────────────
function wrapMethod(fn, label) {
  return async function (...args) {
    if (!canAttempt()) {
      return null; // retorna null silenciosamente (log já foi feito no dedup)
    }

    try {
      const result = await fn.apply(this, args);
      onSuccess();
      return result;
    } catch (err) {
      onFailure(label);
      throw err;
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
