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

// Proteção simples
const MAX_BYTES_BILLED = process.env.MAX_BYTES_BILLED
  ? Number(process.env.MAX_BYTES_BILLED)
  : 5 * 1024 * 1024 * 1024; // 5GB

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
 * Regras:
 * - Sessões oficiais: FUNIL (shopify_funnel_daily_final_v)
 * - Receita/Pedidos: SSOT (ssot_revenue_daily)
 * - Spend Ads: marketing_spend_campaign_daily_dedup (meta_ads, google_ads)
 * - WhatsApp: whatsapp_cost_daily_dedup
 */
const SQL_GESTAO = `
WITH params AS (
  SELECT
    CAST(@start_date AS DATE) AS start_date,
    CAST(@end_date AS DATE) AS end_date
),

date_spine AS (
  SELECT d AS data
  FROM params, UNNEST(GENERATE_DATE_ARRAY(start_date, end_date)) AS d
),

-- Funil diário (sessões oficiais do painel)
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
  WHERE data BETWEEN (SELECT start_date FROM params) AND (SELECT end_date FROM params)
),

-- SSOT revenue daily
rev AS (
  SELECT
    data,
    CAST(receita_total AS FLOAT64) AS receita_total,
    CAST(pedidos_aprovados AS INT64) AS pedidos_aprovados,
    CAST(clientes_novos AS INT64) AS clientes_novos,
    CAST(pedidos_novos_por_pedido AS INT64) AS pedidos_novos_por_pedido,
    CAST(pedidos_recorrentes_por_pedido AS INT64) AS pedidos_recorrentes_por_pedido
  FROM \`${BQ_PROJECT}.${BQ_DATASET}.ssot_revenue_daily\`
  WHERE data BETWEEN (SELECT start_date FROM params) AND (SELECT end_date FROM params)
),

-- Spend Ads por dia e origem (dedup)
ads_spend_day AS (
  SELECT
    data,
    origem,
    SUM(IFNULL(CAST(investimento AS FLOAT64), 0)) AS custo
  FROM \`${BQ_PROJECT}.${BQ_DATASET}.marketing_spend_campaign_daily_dedup\`
  WHERE data BETWEEN (SELECT start_date FROM params) AND (SELECT end_date FROM params)
  GROUP BY 1,2
),

-- WhatsApp cost por dia (dedup)
wpp_cost_day AS (
  SELECT
    data,
    SUM(IFNULL(CAST(custo AS FLOAT64), 0)) AS custo
  FROM \`${BQ_PROJECT}.${BQ_DATASET}.whatsapp_cost_daily_dedup\`
  WHERE data BETWEEN (SELECT start_date FROM params) AND (SELECT end_date FROM params)
  GROUP BY 1
),

-- Custo diário por canal de investimento
marketing_cost_daily_by_channel AS (
  SELECT
    data,
    CASE
      WHEN origem = 'google_ads' THEN 'Google'
      WHEN origem = 'meta_ads' THEN 'Meta'
      ELSE origem
    END AS canal,
    custo
  FROM ads_spend_day

  UNION ALL

  SELECT
    data,
    'Whatsapp' AS canal,
    custo
  FROM wpp_cost_day
),

-- Total diário (com spine pra preencher dias sem dado)
marketing_cost_daily_total AS (
  SELECT
    d.data,
    IFNULL(SUM(m.custo), 0) AS custo_total
  FROM date_spine d
  LEFT JOIN marketing_cost_daily_by_channel m
    ON m.data = d.data
  GROUP BY 1
),

-- Total do período
marketing_cost_period AS (
  SELECT SUM(custo_total) AS custo_marketing
  FROM marketing_cost_daily_total
),

-- Custo por canal no período
marketing_cost_by_channel AS (
  SELECT canal, SUM(custo) AS custo
  FROM marketing_cost_daily_by_channel
  GROUP BY 1
),

-- KPI (período)
kpis AS (
  SELECT
    SUM(IFNULL(r.receita_total, 0)) AS vendas,
    SUM(IFNULL(r.pedidos_aprovados, 0)) AS pedidos,
    SUM(IFNULL(r.pedidos_novos_por_pedido, 0)) AS pedidos_novos,
    SUM(IFNULL(r.pedidos_recorrentes_por_pedido, 0)) AS pedidos_recorrentes,
    SUM(IFNULL(r.clientes_novos, 0)) AS clientes_novos,

    SUM(IFNULL(f.sessoes, 0)) AS sessoes,

    SAFE_DIVIDE(SUM(IFNULL(r.pedidos_aprovados, 0)), NULLIF(SUM(IFNULL(f.sessoes, 0)), 0)) AS taxa_conversao,
    SAFE_DIVIDE(SUM(IFNULL(r.receita_total, 0)), NULLIF(SUM(IFNULL(r.pedidos_aprovados, 0)), 0)) AS aov,

    (SELECT IFNULL(custo_marketing, 0) FROM marketing_cost_period) AS custo_marketing,

    SAFE_DIVIDE(SUM(IFNULL(r.receita_total, 0)), NULLIF((SELECT custo_marketing FROM marketing_cost_period), 0)) AS roas,

    -- CPS = custo por sessão (mesma ideia do seu merge)
    SAFE_DIVIDE((SELECT custo_marketing FROM marketing_cost_period), NULLIF(SUM(IFNULL(f.sessoes, 0)), 0)) AS cps,

    -- CAC por cliente novo (cliente)
    SAFE_DIVIDE((SELECT custo_marketing FROM marketing_cost_period), NULLIF(SUM(IFNULL(r.clientes_novos, 0)), 0)) AS cac_cliente_novo,

    -- CAC por pedido novo (operacional)
    SAFE_DIVIDE((SELECT custo_marketing FROM marketing_cost_period), NULLIF(SUM(IFNULL(r.pedidos_novos_por_pedido, 0)), 0)) AS cac_pedido_novo
  FROM date_spine d
  LEFT JOIN rev r USING (data)
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
  FROM \`${BQ_PROJECT}.${BQ_DATASET}.shopify_channels_daily_shopify_attrib_with_shopify_sessions_human_complete_v\`
  WHERE data BETWEEN (SELECT start_date FROM params) AND (SELECT end_date FROM params)
  GROUP BY canal, tipo
),

-- Pedidos válidos por hora (BRT)
orders_by_hour_raw AS (
  SELECT
    CAST(hora AS INT64) AS hora,
    SUM(CAST(pedidos_validos AS INT64)) AS pedidos
  FROM \`${BQ_PROJECT}.${BQ_DATASET}.shopify_orders_valid_by_hour_v\`
  WHERE data BETWEEN (SELECT start_date FROM params) AND (SELECT end_date FROM params)
  GROUP BY 1
),
hours_spine AS (
  SELECT h AS hora
  FROM UNNEST(GENERATE_ARRAY(0, 23)) AS h
),
orders_by_hour AS (
  SELECT
    h.hora,
    IFNULL(o.pedidos, 0) AS pedidos
  FROM hours_spine h
  LEFT JOIN orders_by_hour_raw o
    USING (hora)
),

-- Atribuição last click (Shopify journey) - focada em marketing (social/search/email)
marketing_attribution_period AS (
  SELECT
    canal,
    tipo,
    SUM(CAST(pedidos AS INT64)) AS pedidos,
    SUM(CAST(vendas AS FLOAT64)) AS vendas
  FROM \`${BQ_PROJECT}.${BQ_DATASET}.shopify_marketing_attribution_last_click_daily_v\`
  WHERE data BETWEEN (SELECT start_date FROM params) AND (SELECT end_date FROM params)
    AND LOWER(tipo) IN ('social', 'search', 'email')
    AND LOWER(canal) NOT IN ('direct', '(direct)', 'unattributed')
  GROUP BY 1,2
),
marketing_attribution_totals AS (
  SELECT
    SUM(pedidos) AS pedidos_total,
    SUM(vendas) AS vendas_total
  FROM marketing_attribution_period
),
marketing_attribution_top AS (
  SELECT *
  FROM marketing_attribution_period
  ORDER BY pedidos DESC, vendas DESC
  LIMIT 10
),

-- Itens vendidos (pedido válido) - top por quantidade
items_period AS (
  SELECT
    sku,
    ANY_VALUE(item_name) AS item_name,
    SUM(CAST(units AS INT64)) AS units
  FROM \`${BQ_PROJECT}.${BQ_DATASET}.shopify_sales_by_model_color_daily_clean_v\`
  WHERE data BETWEEN (SELECT start_date FROM params) AND (SELECT end_date FROM params)
  GROUP BY 1
)
-- Retorna TODOS os itens do período (o front faz o preview e o "ver todos")

SELECT
  FORMAT_DATE('%F', (SELECT start_date FROM params)) AS start,
  FORMAT_DATE('%F', (SELECT end_date FROM params)) AS end_date,
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
      FORMAT_DATE('%F', data) AS data,
      custo_total
    ) ORDER BY data
  ) FROM marketing_cost_daily_total) AS marketing_cost_daily,

  (SELECT ARRAY_AGG(STRUCT(
      FORMAT_DATE('%F', data) AS data,
      canal,
      custo
    ) ORDER BY data, canal
  ) FROM marketing_cost_daily_by_channel) AS marketing_cost_daily_by_channel,

  (SELECT ARRAY_AGG(STRUCT(
      canal,
      custo
    ) ORDER BY custo DESC
  ) FROM marketing_cost_by_channel) AS marketing_cost_by_channel,

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

  , (SELECT ARRAY_AGG(STRUCT(
      LPAD(CAST(hora AS STRING), 2, '0') AS hour,
      pedidos
    ) ORDER BY hora
  ) FROM orders_by_hour) AS orders_valid_by_hour

  , (SELECT AS STRUCT
      IFNULL(pedidos_total, 0) AS orders,
      IFNULL(vendas_total, 0) AS revenue
    FROM marketing_attribution_totals
  ) AS marketing_attribution_totals

  , (SELECT ARRAY_AGG(STRUCT(
      canal,
      tipo,
      pedidos,
      vendas
    ) ORDER BY pedidos DESC, vendas DESC
  ) FROM marketing_attribution_top) AS marketing_attribution

  , (SELECT ARRAY_AGG(STRUCT(
      sku,
      item_name,
      units
    ) ORDER BY units DESC, sku
  ) FROM items_period) AS top_items_sold
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
    revision: process.env.K_REVISION || 'local',
    service_name: process.env.K_SERVICE || 'local',
  }));

  app.get('/v1/shopify/gestao', async (req, reply) => {
    const { start, end, bust } = req.query || {};

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

    // bust=1 força bypass de cache (útil pra validação)
    if (!bust) {
      const cached = cacheGet(key);
      if (cached) {
        reply.header('Cache-Control', `public, max-age=${ttl}`);
        return cached;
      }
    }

    const rows = await runBqQuery(SQL_GESTAO, { start_date: start, end_date: end });
    const out = rows && rows[0] ? rows[0] : null;

    if (!bust) cacheSet(key, out, ttl);

    reply.header('Cache-Control', `public, max-age=${ttl}`);
    return out;
  });

  await app.listen({ port: PORT, host: '0.0.0.0' });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});