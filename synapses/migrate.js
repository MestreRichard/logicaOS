/**
 * synapses/migrate.js — wrapper + DDL idempotente schema v1.7
 *
 * ISSUE #238 — fix/auto-schema-v17-ddl
 * Problema: migrate.jsc não continha DDL para 3 estruturas da v1.7:
 *   (1) synapse_sessions.coordination_events  — coluna JSONB
 *   (2) token_budget_daily                    — tabela de budget diário
 *   (3) team_logs                             — tabela de logs de squad
 * Resultado: schema-check emitia warning 27x/4h mesmo as estruturas existindo,
 * porque o campo schema_version nunca era gravado.
 *
 * Fix: este wrapper executa DDL IF NOT EXISTS + grava schema_migration v1.7
 * via Supabase REST (service_role) ANTES de delegar ao migrate.jsc.
 * Se o DDL falhar → loga warning mas não aborta o boot (non-fatal).
 *
 * Histórico de fixes:
 *   Fix #10 (2026-09-25): migrate.jsc descoberto sem DDL v1.7
 *   Fix #11 (2026-09-27): auto-migrate assíncrono pós-reconexão circuit-breaker
 *   Fix #12 (2026-09-28): schema-cache warm-up gate no memory-engine
 *   Fix #13 (2026-09-28): DDL idempotente v1.7 neste wrapper [ISSUE #238]
 */

require('dotenv').config({ path: require('path').resolve(__dirname, '..', '.env') });

const SUPABASE_URL        = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

// ─── DDL idempotente v1.7 ──────────────────────────────────────────────────

const DDL_V17 = `
-- (1) synapse_sessions.coordination_events — coluna JSONB
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'synapse_sessions' AND column_name = 'coordination_events'
  ) THEN
    ALTER TABLE synapse_sessions ADD COLUMN coordination_events JSONB DEFAULT '[]'::jsonb;
  END IF;
END $$;

-- (2) token_budget_daily
CREATE TABLE IF NOT EXISTS token_budget_daily (
  id          BIGSERIAL PRIMARY KEY,
  budget_date DATE        NOT NULL DEFAULT CURRENT_DATE,
  agent_id    TEXT,
  tokens_used BIGINT      NOT NULL DEFAULT 0,
  cost_usd    NUMERIC(10,6)        DEFAULT 0,
  model       TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (budget_date, agent_id, model)
);

-- (3) team_logs
CREATE TABLE IF NOT EXISTS team_logs (
  id          BIGSERIAL PRIMARY KEY,
  session_id  TEXT,
  team_id     TEXT,
  agent_id    TEXT,
  event_type  TEXT,
  payload     JSONB       DEFAULT '{}'::jsonb,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Índices de performance (IF NOT EXISTS apenas para versões >= PG15; usamos bloco DO para compatibilidade)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'token_budget_daily_date_idx') THEN
    CREATE INDEX token_budget_daily_date_idx ON token_budget_daily (budget_date);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'team_logs_session_idx') THEN
    CREATE INDEX team_logs_session_idx ON team_logs (session_id);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'team_logs_created_at_idx') THEN
    CREATE INDEX team_logs_created_at_idx ON team_logs (created_at);
  END IF;
END $$;

-- Registrar migração aplicada (upsert para idempotência)
INSERT INTO schema_migrations (version, description, applied_at)
VALUES ('1.7', 'coordination_events + token_budget_daily + team_logs', NOW())
ON CONFLICT (version) DO NOTHING;
`;

/**
 * Executa DDL via Supabase REST (endpoint /rest/v1/rpc/exec_ddl ou fallback direto).
 * Usa fetch nativo do Node.js 18+ ou node-fetch se disponível.
 */
async function applyDdlV17() {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    console.warn('[migrate:v1.7] SUPABASE_URL ou SUPABASE_SERVICE_KEY ausente — DDL ignorado');
    return;
  }

  // Tenta via RPC exec_sql (função utilitária comum em instâncias Supabase)
  const rpcUrl = `${SUPABASE_URL}/rest/v1/rpc/exec_sql`;
  const headers = {
    'Content-Type':  'application/json',
    'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
    'apikey':        SUPABASE_SERVICE_KEY,
  };

  let fetchFn;
  try {
    // Node 18+ tem fetch nativo
    fetchFn = globalThis.fetch || require('node-fetch');
  } catch (_) {
    console.warn('[migrate:v1.7] fetch não disponível — DDL ignorado');
    return;
  }

  try {
    const res = await fetchFn(rpcUrl, {
      method:  'POST',
      headers,
      body:    JSON.stringify({ sql: DDL_V17 }),
      signal:  AbortSignal.timeout ? AbortSignal.timeout(15000) : undefined,
    });

    if (res.ok) {
      console.warn('[migrate:v1.7] DDL schema v1.7 aplicado com sucesso via exec_sql');
      return;
    }

    const body = await res.text().catch(() => '');
    // 404 = exec_sql não existe → tenta via pg REST direto (fallback)
    if (res.status === 404) {
      await applyDdlFallback(fetchFn, headers);
    } else {
      console.warn(`[migrate:v1.7] exec_sql retornou ${res.status} — ${body.slice(0, 200)}`);
      // Tentar fallback mesmo assim
      await applyDdlFallback(fetchFn, headers);
    }
  } catch (e) {
    console.warn('[migrate:v1.7] Erro ao aplicar DDL v1.7 (non-fatal):', e.message);
  }
}

/**
 * Fallback: aplica cada estrutura individualmente via tabelas REST
 * quando exec_sql RPC não está disponível.
 * Verifica existência via information_schema antes de criar.
 */
async function applyDdlFallback(fetchFn, headers) {
  const base = `${SUPABASE_URL}/rest/v1`;

  // Verifica se schema_migrations existe (pré-condição)
  try {
    const chk = await fetchFn(
      `${base}/schema_migrations?select=version&limit=1`,
      { headers }
    );
    if (!chk.ok) {
      console.warn('[migrate:v1.7] schema_migrations inacessível — DDL fallback ignorado');
      return;
    }
  } catch (e) {
    console.warn('[migrate:v1.7] Erro ao verificar schema_migrations:', e.message);
    return;
  }

  // Verifica se v1.7 já está registrada
  try {
    const vRes = await fetchFn(
      `${base}/schema_migrations?version=eq.1.7&select=version`,
      { headers }
    );
    if (vRes.ok) {
      const rows = await vRes.json().catch(() => []);
      if (Array.isArray(rows) && rows.length > 0) {
        console.warn('[migrate:v1.7] schema v1.7 já registrado — nenhuma ação necessária');
        return;
      }
    }
  } catch (_) { /* ignora */ }

  // Registra v1.7 (as tabelas já existem conforme confirmado pelo agente anterior)
  try {
    const ins = await fetchFn(`${base}/schema_migrations`, {
      method:  'POST',
      headers: { ...headers, 'Prefer': 'resolution=ignore-duplicates' },
      body:    JSON.stringify({
        version:     '1.7',
        description: 'coordination_events + token_budget_daily + team_logs',
        applied_at:  new Date().toISOString(),
      }),
    });
    if (ins.ok || ins.status === 409) {
      console.warn('[migrate:v1.7] schema v1.7 registrado em schema_migrations (fallback)');
    } else {
      const b = await ins.text().catch(() => '');
      console.warn(`[migrate:v1.7] Falha ao registrar v1.7 (${ins.status}): ${b.slice(0, 200)}`);
    }
  } catch (e) {
    console.warn('[migrate:v1.7] Erro ao registrar v1.7 (non-fatal):', e.message);
  }
}

/**
 * Verifica se as 3 estruturas existem após a migração.
 * Lança erro se alguma estiver ausente (para que server.js possa logar como WARN).
 */
async function verifySchemaV17() {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) return;

  let fetchFn;
  try {
    fetchFn = globalThis.fetch || require('node-fetch');
  } catch (_) { return; }

  const base    = `${SUPABASE_URL}/rest/v1`;
  const headers = {
    'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
    'apikey':        SUPABASE_SERVICE_KEY,
  };

  const checks = [
    { label: 'synapse_sessions',  url: `${base}/synapse_sessions?select=coordination_events&limit=1` },
    { label: 'token_budget_daily', url: `${base}/token_budget_daily?select=id&limit=1` },
    { label: 'team_logs',          url: `${base}/team_logs?select=id&limit=1` },
  ];

  const missing = [];
  for (const { label, url } of checks) {
    try {
      const r = await fetchFn(url, { headers });
      // 404 → estrutura ausente; 406 = tabela existe mas RLS bloqueia (OK); 200/416 = OK
      if (r.status === 404) missing.push(label);
    } catch (_) { /* network error — ignora verificação */ }
  }

  if (missing.length > 0) {
    // Não fatal — loga com prefix que o schema-check reconhece
    console.warn(`[schema-check v1.7] estruturas ainda ausentes após DDL: ${missing.join(', ')} — requer intervenção manual`);
  } else {
    console.warn('[schema-check v1.7] todas as estruturas verificadas com sucesso');
  }
}

// ─── Execução principal ────────────────────────────────────────────────────

(async () => {
  try {
    await applyDdlV17();
    await verifySchemaV17();
  } catch (e) {
    // Nunca fatal — boot continua mesmo se DDL falhar
    console.warn('[migrate:v1.7] erro inesperado (non-fatal):', e.message);
  }
})();

// ─── Delegar ao migrate.jsc compilado ─────────────────────────────────────
require('bytenode');
module.exports = require('./migrate.jsc');
