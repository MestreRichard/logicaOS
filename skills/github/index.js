// skills/github/index.js
// Skill executável — GitHub API
// Repositórios, PRs, issues, commits
// v2 (2026-09-28): dedup auto-heal issues — issue #230

'use strict';

const TOKEN = process.env.GITHUB_TOKEN;
const BASE  = 'https://api.github.com';
const REPO  = process.env.GITHUB_REPO || '';

function headers() {
  if (!TOKEN) throw new Error('GITHUB_TOKEN não configurado. Adicione ao .env');
  return {
    Authorization: `Bearer ${TOKEN}`,
    Accept:        'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
    'Content-Type': 'application/json',
  };
}

async function request(path, options = {}) {
  const url = path.startsWith('http') ? path : `${BASE}${path}`;
  const res  = await fetch(url, { headers: headers(), ...options });
  if (!res.ok) throw new Error(`GitHub error ${res.status}: ${await res.text()}`);
  return res.json();
}

// ─────────────────────────────────────────────────────────────
// DEDUP — normaliza títulos para comparação de padrão
// ─────────────────────────────────────────────────────────────

// Cache de issues abertas com TTL para evitar N+1 requests por ciclo
let _issueCache = { data: null, ts: 0 };
const CACHE_TTL = 60_000; // 60s — um ciclo do mind não roda mais rápido que isso

/**
 * Normaliza título removendo dados variáveis (contagens, ciclos, timestamps)
 * para que "[auto] supabase-fetch-failed: 165x/4h" e "184x em 4h" sejam o mesmo padrão.
 */
function normalizeTitle(title) {
  return title
    .toLowerCase()
    .replace(/\d+x[\/\s]?\d*h?/g, '')           // remove "165x/4h", "184x em 4h", "61x"
    .replace(/ciclo[\s-]*\d+/gi, '')              // remove "ciclo 7", "ciclo-10"
    .replace(/\d{4}-\d{2}-\d{2}/g, '')            // remove datas "2026-09-26"
    .replace(/#\d+/g, '')                         // remove refs "#230"
    .replace(/\d+\s*(erros?|warnings?|alertas?|logs?|restarts?)/gi, '$1') // "191 erros" → "erros"
    .replace(/\(\s*\)/g, '')                       // remove parênteses vazios residuais
    .replace(/\s{2,}/g, ' ')                       // normaliza espaços
    .replace(/[—–-]{2,}/g, '—')                    // normaliza travessões
    .trim();
}

/**
 * Calcula similaridade entre dois títulos normalizados (Jaccard sobre tokens).
 * Retorna 0-1. Threshold de 0.6 é suficiente para detectar duplicatas.
 */
function titleSimilarity(a, b) {
  const tokA = new Set(a.split(/[\s:,;—\-_\/\[\]\(\)]+/).filter(Boolean));
  const tokB = new Set(b.split(/[\s:,;—\-_\/\[\]\(\)]+/).filter(Boolean));
  if (!tokA.size || !tokB.size) return 0;
  let intersection = 0;
  for (const t of tokA) if (tokB.has(t)) intersection++;
  return intersection / (tokA.size + tokB.size - intersection); // Jaccard
}

const AUTO_LABELS = new Set(['auto-detected', 'auto-fix']);

/**
 * Busca issue duplicata aberta entre as auto-detected/auto-fix.
 * Retorna { number, title, url } se encontrar, null se não.
 */
async function findDuplicateIssue(title, labels = [], repo = REPO) {
  const isAutoIssue = labels.some(l => AUTO_LABELS.has(l));
  if (!isAutoIssue) return null; // Só dedup para issues automáticas

  // Buscar issues abertas (com cache)
  const now = Date.now();
  if (!_issueCache.data || (now - _issueCache.ts) > CACHE_TTL) {
    try {
      // Pega até 100 issues abertas pra cobertura decente
      const data = await request(`/repos/${repo}/issues?state=open&per_page=100`);
      _issueCache = {
        data: data.filter(i => !i.pull_request), // exclui PRs
        ts: now,
      };
    } catch {
      return null; // Se falhar a listagem, não bloqueia criação
    }
  }

  const normNew = normalizeTitle(title);

  for (const issue of _issueCache.data) {
    const issueLabels = issue.labels.map(l => l.name);
    const hasAutoLabel = issueLabels.some(l => AUTO_LABELS.has(l));
    if (!hasAutoLabel) continue;

    const normExisting = normalizeTitle(issue.title);
    const sim = titleSimilarity(normNew, normExisting);

    if (sim >= 0.6) {
      return {
        number: issue.number,
        title:  issue.title,
        url:    issue.html_url,
        similarity: sim,
      };
    }
  }

  return null;
}

// ─────────────────────────────────────────────────────────────
// COMMENTS
// ─────────────────────────────────────────────────────────────

async function addComment(issueNumber, body, repo = REPO) {
  return request(`/repos/${repo}/issues/${issueNumber}/comments`, {
    method: 'POST',
    body:   JSON.stringify({ body }),
  });
}

// ─────────────────────────────────────────────────────────────
// ISSUES
// ─────────────────────────────────────────────────────────────

async function listIssues(repo = REPO, state = 'open') {
  const data = await request(`/repos/${repo}/issues?state=${state}&per_page=30`);
  return data
    .filter(i => !i.pull_request) // exclui PRs
    .map(i => ({
      number: i.number,
      title:  i.title,
      state:  i.state,
      labels: i.labels.map(l => l.name),
      url:    i.html_url,
      created_at: i.created_at,
    }));
}

/**
 * Cria issue com dedup automático para issues auto-detected/auto-fix.
 * Se issue similar já estiver aberta, adiciona comentário em vez de criar nova.
 * Retorna { deduplicated: true, existing_issue } se for duplicata.
 */
async function createIssue(title, body, labels = [], repo = REPO) {
  // ── Dedup check (só para issues automáticas) ──
  const existing = await findDuplicateIssue(title, labels, repo);
  if (existing) {
    const timestamp = new Date().toISOString();
    const commentBody = [
      `### 🔄 Auto-heal dedup — ${timestamp}`,
      '',
      `> Tentativa de criar issue duplicada suprimida (similaridade: ${(existing.similarity * 100).toFixed(0)}%)`,
      '',
      '**Novo body seria:**',
      body,
    ].join('\n');

    await addComment(existing.number, commentBody, repo);

    // Invalida cache pra próximo ciclo pegar estado fresco
    _issueCache.ts = 0;

    return {
      deduplicated: true,
      existing_issue: {
        number: existing.number,
        title:  existing.title,
        url:    existing.url,
      },
      message: `Issue duplicada suprimida. Comentário adicionado em #${existing.number}.`,
    };
  }

  // ── Criação normal ──
  const result = await request(`/repos/${repo}/issues`, {
    method: 'POST',
    body:   JSON.stringify({ title, body, labels }),
  });

  // Invalida cache após criação
  _issueCache.ts = 0;

  return result;
}

// ─────────────────────────────────────────────────────────────
// PULL REQUESTS
// ─────────────────────────────────────────────────────────────

async function listPRs(repo = REPO, state = 'open') {
  const data = await request(`/repos/${repo}/pulls?state=${state}&per_page=20`);
  return data.map(pr => ({
    number: pr.number,
    title:  pr.title,
    state:  pr.state,
    branch: pr.head.ref,
    url:    pr.html_url,
    author: pr.user.login,
    created_at: pr.created_at,
  }));
}

// ─────────────────────────────────────────────────────────────
// COMMITS
// ─────────────────────────────────────────────────────────────

async function listCommits(repo = REPO, branch = 'main', limit = 10) {
  const data = await request(`/repos/${repo}/commits?sha=${branch}&per_page=${limit}`);
  return data.map(c => ({
    sha:     c.sha.slice(0, 7),
    message: c.commit.message.split('\n')[0],
    author:  c.commit.author.name,
    date:    c.commit.author.date,
    url:     c.html_url,
  }));
}

// ─────────────────────────────────────────────────────────────
// REPO INFO
// ─────────────────────────────────────────────────────────────

async function getRepoInfo(repo = REPO) {
  const data = await request(`/repos/${repo}`);
  return {
    name:        data.name,
    description: data.description,
    stars:       data.stargazers_count,
    forks:       data.forks_count,
    open_issues: data.open_issues_count,
    language:    data.language,
    updated_at:  data.updated_at,
    url:         data.html_url,
  };
}

// ─────────────────────────────────────────────────────────────
// TOOL DEFINITIONS para Claude API
// ─────────────────────────────────────────────────────────────

function getClaudeToolDefinitions(prefix = 'github') {
  return [
    {
      name: `${prefix}_list_issues`,
      description: 'Lista issues abertas do repositório GitHub',
      input_schema: {
        type: 'object',
        properties: {
          repo:  { type: 'string', description: 'Repositório no formato owner/repo (opcional)' },
          state: { type: 'string', description: '"open" ou "closed" (default: open)' },
        },
      },
    },
    {
      name: `${prefix}_create_issue`,
      description: 'Cria uma nova issue no repositório GitHub. Auto-dedup: se issue similar com label auto-detected/auto-fix já existe, adiciona comentário em vez de criar nova.',
      input_schema: {
        type: 'object',
        properties: {
          title:  { type: 'string', description: 'Título da issue' },
          body:   { type: 'string', description: 'Descrição detalhada' },
          labels: { type: 'array', items: { type: 'string' }, description: 'Labels (opcional)' },
        },
        required: ['title', 'body'],
      },
    },
    {
      name: `${prefix}_list_commits`,
      description: 'Lista commits recentes do repositório',
      input_schema: {
        type: 'object',
        properties: {
          branch: { type: 'string', description: 'Branch (default: main)' },
          limit:  { type: 'number', description: 'Número de commits (default: 10)' },
        },
      },
    },
  ];
}

module.exports = { listIssues, createIssue, addComment, listPRs, listCommits, getRepoInfo, getClaudeToolDefinitions, normalizeTitle, titleSimilarity, findDuplicateIssue };
