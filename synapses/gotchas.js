require('dotenv').config({ path: require('path').resolve(__dirname, '..', '.env') });

// ── Guard: verifica env antes de carregar o .jsc compilado ───────────────────
// Fix original (2026-07-18): gotchas.jsc causava "init.headers is a symbol" e "URL from undefined"
// Issue: https://github.com/MestreRichard/logicaOS/issues/71
//
// FIX QA 2026-07-21 — Remove dependência de gotchas.jsc (bytenode compilado Jul/2 com bug permanente)
// Substituído por implementação inline pura com lazy-env (env lido em runtime, não no require).
// Resultado esperado: ZERO ocorrências de "init.headers is a symbol" e "Failed to parse URL from undefined"
// Closes: #71 (incremento de 83 ocorrências/4h documentado em 2026-07-21)

const { createClient } = require('@supabase/supabase-js');

// Lazy client — lê env em runtime (não no top-level), evitando closure com undefined
let _sbClient = null;
function getClient() {
  if (_sbClient) return _sbClient;
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_ANON_KEY;
  if (!url || url === 'undefined' || !key || key === 'undefined') {
    console.warn('[gotchas] SUPABASE_URL/KEY não definidos — gotchas desabilitado (fallback seguro)');
    return null;
  }
  _sbClient = createClient(url, key);
  return _sbClient;
}

module.exports = {
  getActive: async (agentId, type) => {
    try {
      const sb = getClient();
      if (!sb) return [];
      const { data, error } = await sb
        .from('agent_gotchas')
        .select('pattern,count,last_seen')
        .eq('agent_id', agentId)
        .eq('type', type)
        .eq('active', true)
        .order('count', { ascending: false });
      if (error) { console.warn('[gotchas] getActive error:', error.message); return []; }
      return data || [];
    } catch (e) {
      console.warn('[gotchas] getActive falhou:', e.message);
      return [];
    }
  },

  record: async (agentId, pattern, type) => {
    try {
      const sb = getClient();
      if (!sb) return null;
      const { error } = await sb.from('agent_gotchas').upsert(
        {
          agent_id: agentId,
          pattern,
          type,
          active: true,
          last_seen: new Date().toISOString(),
        },
        { onConflict: 'agent_id,pattern,type' }
      );
      if (error) console.warn('[gotchas] record error:', error.message);
    } catch (e) { /* silencioso — record não é crítico */ }
  },

  increment: async (agentId, pattern, type) => {
    try {
      const sb = getClient();
      if (!sb) return null;
      await sb.rpc('increment_gotcha', {
        p_agent_id: agentId,
        p_pattern:  pattern,
        p_type:     type,
      });
    } catch (e) { /* silencioso — increment não é crítico */ }
  },
};
