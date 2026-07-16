# real-time-traffic-analysis-databricks

# 🚦 RoadPulse — Lakehouse de Tráfego e Malha Viária do Reino Unido

Pipeline de dados ponta a ponta em **Arquitetura Medallion**, construído sobre **Databricks + Unity Catalog + Google Cloud Platform**, processando dados de sensores de tráfego e da malha viária do Reino Unido pelas camadas Bronze → Silver → Gold, totalmente orquestrado, parametrizado por ambiente (Dev/UAT/Prod) e integrado com Git para CI/CD.

## 📐 Arquitetura

```
                    ┌─────────────────────── Governança via Unity Catalog ───────────────────┐
                    │                                                                          │
dados de estradas──┐│      ┌─────────┐      ┌─────────┐      ┌─────────┐      ┌─────────┐     │     ┌── Relatórios
                    ├──▶  │ Landing │ ──▶  │ Bronze  │ ──▶  │ Silver  │ ──▶  │  Gold   │  ──▶ │
dados de tráfego──┘│      └─────────┘      └─────────┘      └─────────┘      └─────────┘     │     └── Dashboards
                    │                                                                          │
                    └───────────────────── Google Cloud Storage (GCS) ────────────────────────┘
```

Duas fontes de dados — **malha viária** (praticamente estática) e **contagens de sensores de tráfego** (alta frequência) — chegam como arquivos CSV em uma zona de landing no GCS, e fluem incrementalmente pelas camadas medallion usando **Databricks Autoloader** e **Structured Streaming** rodando em modo batch agendado (`trigger(availableNow=True)`).

## 🧱 Stack Técnica

| Camada | Ferramenta |
|---|---|
| Infraestrutura Cloud | Google Cloud Platform (GCS, IAM) |
| Compute & Governança | Databricks (Unity Catalog, Delta Lake) |
| Ingestão | Databricks Autoloader (`cloudFiles`) |
| Transformação | PySpark Structured Streaming |
| Orquestração | Databricks Workflows (triggers por chegada de arquivo, dependências entre tasks) |
| Versionamento / CI-CD | GitHub + Git Folders do Databricks |
| Ambientes | Dev → UAT → Prod, cada um com projeto GCP, bucket, catalog e cluster isolados |

## 🗂️ Estrutura do Projeto

```
notebooks/
├── commons.ipynb                          # Variáveis e funções reutilizáveis (remoção de duplicados, tratamento de nulos)
├── 01_creating_schemas_dynamically.ipynb  # Cria os schemas bronze/silver/gold nas managed locations do GCS
├── 02_creating_bronze_tables.ipynb        # DDL das tabelas Delta raw_traffic e raw_roads
├── 03_load_to_bronze.ipynb                # Ingestão via Autoloader: Landing → Bronze
├── 04_silver_traffic_transformations.ipynb# Limpeza dos dados de tráfego, adiciona contagens de veículos elétricos/motorizados
├── 05_silver_roads_transformation.ipynb   # Limpeza dos dados de estradas, adiciona colunas de categoria/tipo de via
├── 06_gold_transformations.ipynb          # Constrói as 6 tabelas Gold com agregações de negócio
└── 07_validation_checks.ipynb             # Validação pontual da contagem de registros em todas as camadas
```

## 🥉🥈🥇 Camadas Medallion

**Bronze** — Ingestão crua, com schema aplicado, das tabelas `raw_traffic` (25 colunas) e `raw_roads` (8 colunas) via Autoloader, com rastreamento via `Extract_Time`.

**Silver** — Tabelas limpas, sem duplicados, com nulos tratados e prontas para uso de negócio:
- `silver_traffic`: adiciona `Electric_Vehicles_Count`, `Motor_Vehicles_Count`, `Transformed_Time`
- `silver_roads`: adiciona `Road_Category_Name` e `Road_Type` (Major/Minor) via mapeamento de código para rótulo

**Gold** — Seis tabelas analíticas prontas para relatórios:
| Tabela | Finalidade |
|---|---|
| `gold_traffic` | Dataset completo de tráfego + `Vehicle_Intensity` (veículos por km) |
| `gold_roads` | Dataset completo da malha viária |
| `traffic_summary` | Volume de tráfego por ano |
| `traffic_by_region` | Volume de tráfego por região/autoridade local |
| `road_usage_statistics` | Volume de tráfego por rodovia |
| `ev_adoption_trend` | Adoção de carros/motos elétricos por ano |

## ⚙️ Orquestração

Um único **Databricks Workflow (`ETL_workflow`)** encadeia o pipeline com as dependências corretas entre tasks:

```
load_to_bronze ──┬──▶ silver_traffic ──┐
                  └──▶ silver_roads ────┴──▶ gold_transformations
```

- **Trigger**: chegada de arquivo no path GCS de `raw_traffic` (dados de tráfego chegam com frequência; dados de estradas são quase estáticos e atualizados separadamente/por agenda)
- **Parâmetros**: `environment` (`dev` / `uat` / `prod`) passado no nível do job, consumido por todos os notebooks via `dbutils.widgets`
- **Notificações**: alerta por e-mail em caso de falha de task
- **Compute**: cluster dedicado por ambiente

## 🌎 Estratégia Multi-Ambiente

Cada ambiente (**Dev → UAT → Prod**) é totalmente isolado:
- Projeto GCP e bucket GCS próprios (`landing`, `checkpoints`, `medallion/{bronze,silver,gold}`)
- Storage credentials e 5 external locations próprias por ambiente (`landing_<env>`, `checkpoints_<env>`, `bronze_<env>`, `silver_<env>`, `gold_<env>`)
- Catalog (`<env>_catalog`) e cluster próprios
- Todos os notebooks são 100% agnósticos de ambiente: os caminhos são resolvidos dinamicamente via `DESCRIBE EXTERNAL LOCATION`, e o catalog é selecionado via o widget/parâmetro `env` — o mesmo código roda sem alteração em qualquer ambiente

A promoção entre ambientes segue o fluxo de branches do Git (`dev` → `uat` → `main`) via Pull Requests, com cada Databricks Workflow apontando para sua respectiva branch.

## 🚀 Como Começar

1. Configure os projetos GCP (um por ambiente) e um Unity Catalog Metastore no Databricks
2. Crie os buckets GCS com a estrutura de pastas acima
3. Crie as storage credentials + 5 external locations por ambiente
4. Rode `01_creating_schemas_dynamically` e `02_creating_bronze_tables` uma vez por ambiente, passando `env` como parâmetro
5. Publique os notebooks `03`–`06` como tasks em um Databricks Workflow com trigger de chegada de arquivo
6. Conecte o workspace a este repositório (Git Folder) e aponte o job de cada ambiente para sua branch correspondente

## 📊 Exemplo de Insight

Agregar `road_usage_statistics` revela quais rodovias concentram o maior volume de veículos motorizados — útil para priorizar investimentos em expansão viária ou pontos de recarga para veículos elétricos.

---

*Projeto prático desenvolvido para consolidar padrões de Lakehouse de nível produção: Autoloader, Structured Streaming, governança via Unity Catalog, promoção multi-ambiente e CI/CD integrado ao Git no Databricks.*
