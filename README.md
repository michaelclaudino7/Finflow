# 📊 FinFlow — Pipeline de Dados do Mercado Financeiro

![Python](https://img.shields.io/badge/Python-3.14-blue?logo=python)
![dbt](https://img.shields.io/badge/dbt-1.7-orange?logo=dbt)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)
![dlt](https://img.shields.io/badge/dlt-1.27-8A2BE2?logo=python)
![Docker](https://img.shields.io/badge/Docker-compose-2496ED?logo=docker)

> Pipeline de dados end-to-end para análise do mercado financeiro brasileiro.
> Ingere cotações de ações e indicadores macroeconômicos (Selic, IPCA, CDI, câmbio) via dlt
> com schema evolution automático, e calcula métricas de risco como Sharpe, Sortino, beta, VaR e drawdown máximo.

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│  INGESTÃO (dlt + yfinance + requests)                           │
│  Yahoo Finance │ BCB API                                        │
│  merge automático │ schema evolution │ lineage de carga         │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  ARMAZENAMENTO (PostgreSQL — schema raw)                        │
│  precos_acoes │ indicadores_macro │ ativos                      │
│  _dlt_loads │ _dlt_version (metadados de carga)                 │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  TRANSFORMAÇÃO (dbt)                                            │
│  staging → intermediate → marts                                 │
│                                                                 │
│  staging:      limpeza, tipagem, padronização                   │
│  intermediate: retornos diários, indicadores pivotados          │
│  marts:        fact_precos, dim_ativos, mart_risco,             │
│                mart_correlacao                                  │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  QUALIDADE (dbt tests)                                          │
│  57 testes automatizados de unicidade, not null e domínio       │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  ANÁLISE (SQL views + Jupyter)                                  │
│  retorno, volatilidade, beta, correlação, VaR                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

```bash
# 1. Clone e entre no projeto
git clone https://github.com/michaelclaudino7/Finflow.git
cd Finflow

# 2. Crie o ambiente virtual e instale as dependências
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Configure as variáveis de ambiente
cp .env.example .env

# 4. Suba o banco de dados
docker compose up -d

# 5. Rode a ingestão completa via dlt
python3 pipeline.py --full

# 6. Rode as transformações dbt
docker run --rm --network host \
  -v $(pwd)/dbt:/dbt \
  -e POSTGRES_HOST=localhost \
  -e POSTGRES_PORT=5432 \
  -e POSTGRES_USER=finflow \
  -e POSTGRES_PASSWORD=finflow123 \
  -e POSTGRES_DB=finflow \
  ghcr.io/dbt-labs/dbt-postgres:1.7.latest \
  run --profiles-dir /dbt --project-dir /dbt
```

Ou use o Makefile:

```bash
make up           # sobe PostgreSQL + Adminer
make ingest       # ingestão incremental via dlt
make ingest-full  # ingestão completa (desde 2019)
make dbt-run      # transforma com dbt
make dbt-test     # testa os 57 modelos dbt
make pipeline     # tudo: ingest + dbt-run + dbt-test
```

### Opções do pipeline dlt

```bash
python3 pipeline.py               # incremental (apenas dados novos)
python3 pipeline.py --full        # carga completa desde DATA_INICIO
python3 pipeline.py --only precos # apenas preços de ações
python3 pipeline.py --only macro  # apenas indicadores macro
python3 pipeline.py --only ativos # apenas metadados dos ativos
```

---

## 📦 Stack

| Camada | Tecnologia | Finalidade |
|---|---|---|
| Ingestão | dlt 1.27 + yfinance + requests | Coleta com schema evolution e lineage |
| Banco de dados | PostgreSQL 15 | Warehouse local |
| Containerização | Docker Compose | Ambiente reproduzível |
| Transformação | dbt-core 1.7 | Modelagem em camadas (staging/marts) |
| Qualidade | dbt tests | 57 testes automatizados |
| Análise | Jupyter + pandas + scipy | Exploração e visualização |

---

## 📐 Modelos dbt

### Staging
| Modelo | Descrição |
|---|---|
| `stg_precos_acoes` | Preços ajustados limpos e padronizados |
| `stg_indicadores_macro` | Selic, IPCA, CDI, câmbio categorizados |
| `stg_ativos` | Metadados de ativos com fallbacks |

### Intermediate
| Modelo | Descrição |
|---|---|
| `int_retornos_diarios` | Retorno simples e logarítmico por ativo |
| `int_indicadores_pivot` | Indicadores macro pivotados por data |

### Marts
| Modelo | Descrição |
|---|---|
| `fact_precos` | Fato principal: preços + retornos + beta + macro |
| `dim_ativos` | Dimensão de ativos com estatísticas históricas |
| `mart_risco` | Sharpe, Sortino, VaR 95/99%, drawdown por ano |
| `mart_correlacao` | Matriz de correlação entre pares de ativos |

---

## 📊 Métricas calculadas

- **Retorno simples e logarítmico** diário, acumulado e YTD
- **Volatilidade anualizada** (rolling 21 dias úteis)
- **Beta** em relação ao IBOVESPA (rolling 252 dias úteis)
- **Sharpe Ratio** anualizado (excesso sobre CDI)
- **Sortino Ratio** (penaliza apenas volatilidade negativa)
- **VaR histórico** 95% e 99%
- **Maximum Drawdown** por ano
- **Win Rate** (% de dias com retorno positivo)
- **Correlação de Pearson** entre todos os pares de ativos
- **Retorno real** (descontado pelo IPCA)

---

## 🔄 Schema Evolution

O projeto utiliza **dlt** para gerenciar automaticamente a evolução do schema das tabelas raw:

- Novas colunas adicionadas nas fontes são detectadas e aplicadas automaticamente no banco
- Cada carga é rastreada na tabela `_dlt_loads` com timestamp, status e pacote de carga
- A tabela `_dlt_version` mantém o histórico de versões do schema
- Estratégia `merge` garante idempotência — rodar o pipeline múltiplas vezes não duplica dados

---

## 🧪 Qualidade de dados

O projeto utiliza dbt tests executados a cada `make dbt-test`:

- Unicidade de chaves primárias
- Valores não nulos em campos obrigatórios
- Validação de domínios (tipos de ativo, grupos macro)
- Checagem de consistência (preços positivos, correlações entre -1 e 1, drawdown negativo)

**57 testes — PASS=57 WARN=0 ERROR=0**

---

## 📁 Estrutura de pastas

```
finflow/
├── ingestion/              # Módulo de ingestão
│   ├── sources/
│   │   ├── yahoo.py        # Fonte Yahoo Finance (dlt resource)
│   │   └── bcb.py          # Fonte BCB + IBGE (dlt resource)
│   └── config.py           # Configurações centrais
├── pipeline.py             # Entrypoint principal do dlt
├── dbt/
│   ├── models/
│   │   ├── staging/        # Limpeza e padronização
│   │   ├── intermediate/   # Cálculos intermediários
│   │   └── marts/          # Tabelas analíticas finais
│   ├── macros/             # Macros reutilizáveis
│   └── dbt_project.yml
├── expectations/           # Great Expectations (suites de validação)
├── notebooks/              # Análise exploratória
├── scripts/
│   └── init.sql            # Inicialização do banco
├── docker-compose.yml
├── Makefile
├── requirements.txt
└── .env.example
```

---

## ⚙️ Configuração

Copie `.env.example` para `.env` e preencha com suas credenciais:

```bash
cp .env.example .env
```

```env
POSTGRES_HOST=
POSTGRES_PORT=
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=

# Ativos monitorados (separados por vírgula)
TICKERS=

# Data inicial da carga histórica
DATA_INICIO=
```

O Adminer (interface web do banco) fica disponível em **http://localhost:8080** após o `docker compose up`.