'use strict';

const Fastify = require('fastify');
const cors = require('@fastify/cors');
const { BigQuery } = require('@google-cloud/bigquery');

const PORT = Number(process.env.PORT || 8080);

// BigQuery (US)
const BQ_PROJECT = process.env.BQ_PROJECT || process.env.GOOGLE_CLOUD_PROJECT || 'reise-ssot';
const BQ_DATASET = process.env.BQ_DATASET || 'mart_growth_us';
const BQ_LOCATION = process.env.BQ_LOCATION || 'US';

// Cache
const TTL_INTRADAY_SECONDS = Number(process.env.TTL_INTRADAY_SECONDS || 300); // 5 min
const TTL_CLOSED_SECONDS = Number(process.env.TTL_CLOSED_SECONDS || 3600);    // 60 min
const MAX_RANGE_DAYS = Number(process.env.MAX_RANGE_DAYS || 400);

// Proteção simples contra surpresa
const MAX_BYTES_BILLED = process.env.MAX_BYTES_BILLED
  ? Number(process.env.MAX_BYTES_BILLED)
  : 5 * 1024 * 1024 * 1024; // 5GB por query (bem acima do MVP)

function isValidYmd(s) {
  return typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s);
}

function getTodayYmdInSaoPaulo() {
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Sao_Paulo',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  return fmt.format(new Date());
}

function daysBetweenYmd(start, end) {
  const a = new Date(`${start}T00:00:00Z`);
  const b = new Date(`${end}T00:00:00Z`);
  return Math.floor((b.getTime() - a.getTime()) / (24 * 60 * 60 * 1000));
}

function makeCacheKey(path, query) {
  const parts = Object.keys(query || {})
    .sort()
    .map((k) => `${k}=${query[k]}`);
  return `${path}?${parts.join('&')}`;
}

// Cache simples em memória (por instância)
const cache = new Map();
function cacheGet(key) {
  const item = cache.get(key);
  if (!item) return null;
  if (Date.now() > item.expiresAtMs) {
    cache.delete(key);
    return null;
  }
  return item.value;
}
function cacheSet(key, value, ttlSeconds) {
  cache.set(key, { value, expiresAtMs: Date.now() + ttlSeconds * 1000 });
}

const bigquery = new BigQuery({ projectId: BQ_PROJECT });

/**
 * NOVA REGRA:
 * - kpis.sessoes vem do FUNIL (shopify_funnel_daily_final_v)
 * - channels vem de shopify_channels_daily_complete_v (inclui "Não mapeado")
 */
const SQL_GESTAO = `
WITH date_spine AS (
  SELECT d AS data
  FROM UNNEST(GENERATE_DATE_ARRAY(@start_date, @end_date)) AS d
),

-- Funil diário (base oficial de sessões do dashboard)
funnel AS (
  SELECT
    data,
    CAST(visitantes AS INT64) AS visitantes,
    CAST(sessoes AS INT64) AS sessoes,
    CAST(sessoes_com_carrinho AS INT64) AS sessoes_com_carrinho,
    CAST(sessoes_chegaram_checkout AS INT64) AS sessoes_chegaram_checkout,
    CAST(COALESCE(pedidos_aprovados_validos, 0) AS INT64) AS pedidos_aprovados_validos,
    CAST(COALESCE(taxa_conversao, 0) AS FLOAT64) AS taxa_conversao,
    CAST(COALESCE(taxa_conv_checkout, 0) AS FLOAT64) AS taxa_conv_checkout
  FROM \`${BQ_PROJECT}.${BQ_DATASET}.shopify_funnel_daily_final_v\`
  WHERE data BETWEEN @start_date AND @end_date
),

-- KPI: receita/pedidos SSOT + sessoes do funil (pra bater com jornada)
kpis AS (
  SELECT
    SUM(IFNULL(CAST(r.receita_total AS FLOAT64), 0)) AS vendas,
    SUM(IFNULL(CAST(r.pedidos_aprovados AS INT64), 0)) AS pedidos,
    SUM(IFNULL(CAST(r.pedidos_novos_por_pedido AS INT64), 0)) AS pedidos_novos,
    SUM(IFNULL(CAST(r.pedidos_recorrentes_por_pedido AS INT64), 0)) AS pedidos_recorrentes,
    SUM(IFNULL(CAST(f.sessoes AS INT64), 0)) AS sessoes,

    SAFE_DIVIDE(
      SUM(IFNULL(CAST(r.pedidos_aprovados AS INT64), 0)),
      NULLIF(SUM(IFNULL(CAST(f.sessoes AS INT64), 0)), 0)
    ) AS taxa_conversao,

    SAFE_DIVIDE(
      SUM(IFNULL(CAST(r.receita_total AS FLOAT64), 0)),
      NULLIF(SUM(IFNULL(CAST(r.pedidos_aprovados AS INT64), 0)), 0)
    ) AS aov
  FROM date_spine d
  LEFT JOIN \`${BQ_PROJECT}.${BQ_DATASET}.ssot_revenue_daily\` r USING (data)
  LEFT JOIN funnel f USING (data)
),

channels_period AS (
  SELECT
    canal,
    tipo,
    SUM(CAST(sessoes AS INT64)) AS sessoes,
    SUM(CAST(vendas AS FLOAT64)) AS vendas,
    SUM(CAST(pedidos AS INT64)) AS pedidos,
    SAFE_DIVIDE(SUM(CAST(pedidos AS INT64)), NULLIF(SUM(CAST(sessoes AS INT64)), 0)) AS taxa_conversao,
    SAFE_DIVIDE(SUM(CAST(vendas AS FLOAT64)), NULLIF(SUM(CAST(pedidos AS INT64)), 0)) AS aov,
    SUM(CAST(pedidos_novos_clientes AS INT64)) AS pedidos_novos_clientes,
    SUM(CAST(pedidos_clientes_recorrentes AS INT64)) AS pedidos_clientes_recorrentes
  FROM \`${BQ_PROJECT}.${BQ_DATASET}.shopify_channels_daily_dashboard_v\`
  WHERE data BETWEEN @start_date AND @end_date
  GROUP BY canal, tipo
)

SELECT
  FORMAT_DATE('%F', @start_date) AS start,
  FORMAT_DATE('%F', @end_date) AS end_date,
  'America/Sao_Paulo' AS timezone,

  (SELECT AS STRUCT * FROM kpis) AS kpis,

  (SELECT ARRAY_AGG(STRUCT(
      FORMAT_DATE('%F', data) AS data,
      visitantes,
      sessoes,
      sessoes_com_carrinho,
      sessoes_chegaram_checkout,
      pedidos_aprovados_validos,
      taxa_conversao,
      taxa_conv_checkout
    ) ORDER BY data
  ) FROM funnel) AS funnel_daily,

  (SELECT ARRAY_AGG(STRUCT(
      canal,
      tipo,
      sessoes,
      vendas,
      pedidos,
      taxa_conversao,
      aov,
      pedidos_novos_clientes,
      pedidos_clientes_recorrentes
    ) ORDER BY vendas DESC
  ) FROM channels_period) AS channels
;`;

async function runBqQuery(sql, params) {
  const [job] = await bigquery.createQueryJob({
    query: sql,
    params,
    location: BQ_LOCATION,
    maximumBytesBilled: MAX_BYTES_BILLED,
  });
  const [rows] = await job.getQueryResults();
  return rows;
}

async function main() {
  const app = Fastify({ logger: true, trustProxy: true });

  // CORS
  const allow = (process.env.CORS_ALLOW_ORIGINS || '').trim();
  const allowList = allow ? allow.split(',').map((s) => s.trim()).filter(Boolean) : null;

  await app.register(cors, {
    origin: (origin, cb) => {
      if (!origin) return cb(null, true);
      if (!allowList) return cb(null, true);
      if (allowList.includes(origin)) return cb(null, true);
      return cb(new Error('CORS blocked'), false);
    },
    methods: ['GET', 'OPTIONS'],
  });

  app.get('/v1/health', async () => ({
    ok: true,
    service: 'ssot-api',
    today_sp: getTodayYmdInSaoPaulo(),
    bq_project: BQ_PROJECT,
    bq_dataset: BQ_DATASET,
    bq_location: BQ_LOCATION,
  }));

  app.get('/v1/shopify/gestao', async (req, reply) => {
    const { start, end } = req.query || {};

    if (!isValidYmd(start) || !isValidYmd(end)) {
      reply.code(400);
      return { ok: false, error: 'Use start e end no formato YYYY-MM-DD.' };
    }
    if (start > end) {
      reply.code(400);
      return { ok: false, error: 'start não pode ser maior que end.' };
    }

    const rangeDays = daysBetweenYmd(start, end);
    if (rangeDays > MAX_RANGE_DAYS) {
      reply.code(400);
      return { ok: false, error: `Intervalo muito grande (${rangeDays} dias).` };
    }

    const todaySp = getTodayYmdInSaoPaulo();
    const closed = end < todaySp;
    const ttl = closed ? TTL_CLOSED_SECONDS : TTL_INTRADAY_SECONDS;

    const key = makeCacheKey(req.routerPath, req.query);
    const cached = cacheGet(key);
    if (cached) {
      reply.header('Cache-Control', `public, max-age=${ttl}`);
      return cached;
    }

    const rows = await runBqQuery(SQL_GESTAO, { start_date: start, end_date: end });
    const out = rows && rows[0] ? rows[0] : null;

    cacheSet(key, out, ttl);
    reply.header('Cache-Control', `public, max-age=${ttl}`);
    return out;
  });

  await app.listen({ port: PORT, host: '0.0.0.0' });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});