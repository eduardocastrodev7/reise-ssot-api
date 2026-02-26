# Reise SSOT API - Cloud Run (Fastify + BigQuery)

API HTTP usada pelo Dashboard Gerencial da Reise para consultar o **SSOT no BigQuery (US)** com segurança e governança.

## Por que existe esta API?
- Evitar BigQuery no browser (segurança e custo)
- Centralizar regras de negócio e contrato JSON
- Permitir cache e limites
- Garantir que o dashboard consulte **somente o dataset US** (`reise-ssot.mart_growth_us`)

## Stack
- Node.js (Fastify)
- BigQuery Client (@google-cloud/bigquery)
- Deploy em Cloud Run via `gcloud run deploy --source .`

## Requisitos (local)
- Node.js 18+ (recomendado 20)
- NPM
- Google Cloud CLI (`gcloud`) para deploy
- Acesso ao projeto GCP `reise-ssot`

## Variáveis de ambiente
Exemplo (`.env.example`):
```bash
BQ_PROJECT=reise-ssot
BQ_DATASET=mart_growth_us
BQ_LOCATION=US

CORS_ALLOW_ORIGINS=http://localhost:5173

TTL_INTRADAY_SECONDS=300
TTL_CLOSED_SECONDS=3600
MAX_RANGE_DAYS=400
```

> Em produção, essas variáveis são definidas no deploy do Cloud Run.

## Endpoints (MVP)
### Health
- `GET /v1/health`

### Gestão (Shopify-like)
- `GET /v1/shopify/gestao?start=YYYY-MM-DD&end=YYYY-MM-DD`

Retorna um JSON com:
- `kpis` (vendas/pedidos/sessoes/taxa_conversao/aov/novos/recorrentes)
- `funnel_daily` (linha diária do funil)
- `channels` (canais no período)

## Regra de “sessões” (consistência)
Para evitar confusão no dashboard:
- Sessões oficiais do dashboard = **sessões do funil**
- Canais fecham o total com bucket **“Não mapeado”** quando necessário

Isso mantém consistência entre:
- Performance (Sessões)
- Evolução de jornada (Sessões)
- Soma de sessões por canal

## BigQuery / Regiões
- A API consulta **apenas** `reise-ssot.mart_growth_us` (US).
- Não fazer join SP + US na mesma query.
- Se algo nascer em SP, primeiro deve ser publicado/replicado para US.

## Permissões recomendadas (IAM)
Service Account do Cloud Run (ex.: `sa-ssot-api@...`):
- `roles/bigquery.jobUser` no projeto
- `roles/bigquery.dataViewer` no dataset `reise-ssot:mart_growth_us`

## Rodar local (opcional)
```bash
npm install
npm start
```

Por padrão sobe em:
- http://localhost:8080

> Para rodar local com BigQuery, você precisa estar autenticado (ADC) ou rodar em ambiente com permissão.

## Deploy no Cloud Run (produção)
Dentro do projeto:
```bash
gcloud config set project reise-ssot

gcloud run deploy ssot-api \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --service-account sa-ssot-api@reise-ssot.iam.gserviceaccount.com \
  --set-env-vars BQ_PROJECT=reise-ssot,BQ_DATASET=mart_growth_us,BQ_LOCATION=US,CORS_ALLOW_ORIGINS=http://localhost:5173
```

Após deploy, o comando retorna a `Service URL`, usada no front como:
`VITE_SSOT_API_BASE_URL`.

## Segurança
- Não versionar `.env` / `.env.local`
- Não subir chave JSON de service account
- Idealmente restringir CORS ao(s) domínio(s) do front
- Futuro: adicionar autenticação simples (API Key) ou IAP/API Gateway

## Roadmap (próximos endpoints)
- `/v1/shopify/trafego/device`
- `/v1/shopify/trafego/referrer`
- `/v1/shopify/recorrencia`
- Spend/ROAS/CPA por canal (quando spend oficial estiver pronto)
