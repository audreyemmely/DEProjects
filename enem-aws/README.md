# 🎓 ENEM Data Pipeline

Pipeline de dados end-to-end usando microdados públicos do ENEM (2023 e 2024) do INEP.

## 🏗️ Arquitetura

```
Dados Públicos (INEP)
        ↓
   S3 Raw (CSV)
        ↓
  Glue Crawler → Glue ETL Job
        ↓
  S3 Bronze (Parquet)
        ↓
     Athena
        ↓
  DBT (Staging → Marts)
        ↓
   Análise / Dashboard

  Airflow (Docker) orquestra tudo
```

**Stack:** Python · AWS S3 · AWS Glue · AWS Athena · DBT · Apache Airflow · Docker

---

## 📁 Estrutura do Repositório

```
enem-pipeline/
├── dags/
│   └── enem_pipeline.py          # DAG principal do Airflow
├── enem_analytics/               # Projeto DBT
│   ├── .dbt/
│   │   └── profiles.yml          # Conexão com Athena (não commitado)
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── stg_enem.sql
│   │   │   └── stg_enem.yml
│   │   └── marts/
│   │       ├── mart_desempenho_por_estado.sql
│   │       ├── mart_desigualdade.sql
│   │       └── mart_mobilidade_educacional.sql
│   └── dbt_project.yml
├── glue/
│   └── glue_raw_to_bronze.py     # Job Glue ETL (Raw → Bronze)
├── queries/                      # Queries analíticas no Athena
│   ├── athena_query1.sql        
|   ├── athena_query2.sql 
|   ├── athena_query3.sql 
|   └── athena_query4.sql
├── scripts/
│   └── extract_enem_to_s3.ipynb     # Ingestão dos dados para o S3
├── Dockerfile
├── docker-compose.yml
├── prints-AWS.pdf                # Documento com prints das telas dos serviços AWS funcionando 
└── README.md
```

---

## ⚙️ Pré-requisitos

- Python 3.11+
- Docker Desktop
- Conta AWS (Free Tier suficiente)
- AWS CLI configurado
- `dbt-athena-community` instalado localmente

---

## 1. Configuração AWS

### IAM — Usuário para execução local

1. AWS Console → IAM → Users → **Create user**
   - Nome: `data-pipeline-user`
   - Políticas: `AmazonS3FullAccess`, `AWSGlueConsoleFullAccess`, `AmazonAthenaFullAccess`
2. Gere as **Access Keys** e salve em `.env` (nunca commite)

### IAM — Role para o Glue

1. IAM → Roles → **Create role**
   - Trusted entity: `AWS Glue`
   - Nome: `AWSGlueServiceRole-enem-pipeline`
   - Políticas: `AmazonS3FullAccess`, `AWSGlueServiceRole`

> **Conceito importante:** IAM User = autenticação humana. IAM Role = permissão service-to-service.

### S3 — Estrutura do bucket

```
s3://SEU-BUCKET/
├── raw/enem/ano=YYYY/         # CSVs originais do INEP
├── bronze/enem/ano=YYYY/      # Parquet após ETL
├── athena-results/            # Resultados temporários do Athena
└── dbt/                       # Tabelas criadas pelo DBT
```

Crie as pastas manualmente ou deixe o script de ingestão criar.

---

## 2. Ingestão dos Dados

`scripts/extract_enem_to_s3.ipynb` — baixa os ZIPs do INEP e faz upload para o S3 sem salvar em disco.

```bash
pip install boto3 requests python-dotenv tqdm
jupyter notebook scripts/extract_enem_to_s3.ipynb
```

Configure o `.env` com base no exemplo abaixo:
```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=seu-bucket
```

### ⚠️ Desafio: Estrutura diferente entre 2023 e 2024

| Ano | Estrutura |
|-----|-----------|
| 2023 | Arquivo único `microdados_enem_2023.csv` (76 colunas) |
| 2024 | Dois arquivos separados: `participantes_2024.csv` + `resultados_2024.csv` |

**Solução:** Join por posição (ambos têm 4.332.944 linhas na mesma ordem), validado localmente com pandas — 0 inconsistências encontradas.

---

## 3. Glue ETL — Raw → Bronze

`glue/glue_raw_to_bronze.py` — job PySpark que:
- Lê CSVs do S3 raw
- No 2024: faz coalesce para 8 partições, adiciona index por posição, faz join e remove o index
- Aplica schema unificado de 75 colunas (tipagem explícita)
- Salva em Parquet particionado por `ano`

**Configuração do job no Glue Console:**
- Glue version: 4.0 (Spark 3.3)
- Worker type: G.1X · Workers: 2
- IAM Role: `AWSGlueServiceRole-enem-pipeline`
- Parâmetro: `--S3_BUCKET` com o nome do bucket

### ⚠️ Erros encontrados

| Erro | Causa | Solução |
|------|-------|---------|
| Schema mismatch | Coluna `ano` ausente no schema | Adicionou `"ano": StringType()` ao `SCHEMA_BRONZE` |
| Encoding error | Arquivos INEP usam ISO-8859-1 | `sep=';', encoding='ISO-8859-1'` |
| Glue Crawler sobrescrevia schema | Configuração padrão do Crawler | Crawler configurado para "Ignore schema changes" |

---

## 4. Athena — Queries Analíticas

Após rodar o Glue Crawler na camada bronze, os dados ficam disponíveis em `enem_db.enem`.



### ⚠️ Desafio: questionário mudou entre 2023 e 2024

| Coluna | Significado em 2023 | Significado em 2024 |
|--------|---------------------|---------------------|
| `Q006` | Renda familiar (A→Q, 17 faixas) | "Você possui renda?" (A=Não, B=Sim) |
| `Q007` | Empregado doméstico | Renda familiar (A→Q, 17 faixas) |
| `Q008` | Banheiro | Empregado doméstico |
| `tp_escola` | Tipo de escola (2=Pública, 3=Privada) | **Não existe** |
| `tp_dependencia_adm_esc` | **Não existe** | Dependência adm. (1=Federal, 2=Estadual, 3=Municipal, 4=Privada) |

O mesmo número de questão tem significados completamente diferentes entre os anos — queries escritas sem verificar o dicionário oficial retornavam valores errados silenciosamente, sem qualquer erro de execução.

### Queries implementadas (`queries/athena_queries.sql`)

1. **Renda × tipo de escola × desempenho** (2024) — usa `Q007` e `tp_dependencia_adm_esc`
2. **Evolução por raça/cor** (2023 e 2024) — colunas consistentes entre os anos, com `pct_escola_publica` normalizado
3. **Mobilidade educacional** — escolaridade da mãe (`Q002`) vs desempenho dos filhos

### ⚠️ Erro: `pct_escola_publica` sempre zero no 2024

**Causa:** `tp_escola` não existe nos arquivos de 2024. O Glue preenchia a coluna com NULL.

**Solução:** CASE normalizado para os dois anos:
```sql
CASE
    WHEN ano = '2023' AND tp_escola = 2                        THEN 1
    WHEN ano = '2024' AND tp_dependencia_adm_esc IN (1, 2, 3) THEN 1
    ELSE 0
END
```

### ⚠️ Erro: duplicatas no GROUP BY

**Causa:** `GROUP BY tp_dependencia_adm_esc` gerava 3 linhas para "Pública" (Federal/Estadual/Municipal).

**Solução:** Athena não suporta alias no GROUP BY — repetir o CASE completo:
```sql
GROUP BY
    CASE WHEN tp_dependencia_adm_esc IN (1,2,3) THEN 'Pública' ... END
```

---

## 5. DBT — Transformações

### Instalação

```bash
pip install dbt-athena-community
dbt init enem_analytics
```

### Configuração (`~/.dbt/profiles.yml`)

```yaml
enem_analytics:
  target: dev
  outputs:
    dev:
      type: athena
      region_name: "{{ env_var('AWS_DEFAULT_REGION') }}"
      s3_staging_dir: "s3://{{ env_var('S3_BUCKET') }}/athena-results/"
      s3_data_dir: "s3://{{ env_var('S3_BUCKET') }}/dbt/"
      schema: enem_db
      database: awsdatacatalog
      aws_access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID') }}"
      aws_secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
      threads: 2
```

### Camadas

```
bronze (source) → stg_enem (view) → mart_desempenho_por_estado (table)
                                   → mart_desigualdade (table)
                                   → mart_mobilidade_educacional (table)
```

**Staging (`stg_enem.sql`):** normaliza as diferenças entre 2023 e 2024 — cria colunas unificadas `escola_publica`, `tipo_escola` e `renda_familiar_cod` usando CTEs intermediários.

**Marts:** tabelas analíticas finais consultáveis diretamente no Athena.

### Executar

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

dbt debug    # testa conexão
dbt run      # executa modelos
dbt test     # valida qualidade
dbt docs generate && dbt docs serve  # documentação em localhost:8080
```

### ⚠️ Erros encontrados

| Erro | Causa | Solução |
|------|-------|---------|
| `TYPE_MISMATCH: integer vs varchar` | `CAST(ano AS INTEGER)` no SELECT mas comparações usavam string no mesmo CTE | Criou CTE intermediário `ano_convertido` para converter antes das comparações |
| `accepted_values` falhava mesmo com inteiros | DBT trata valores como string por padrão | Adicionou `quote: false` no teste |
| `pct_escola_publica` zerado | Mesma causa das queries do Athena | Mesmo CASE normalizado aplicado nos modelos DBT |

---

## 6. Airflow + Docker

### Estrutura

```
enem-pipeline/
├── Dockerfile
├── docker-compose.yml
└── .env
```

### ⚠️ Desafio: conflito de dependências

`dbt-core 1.11.6` exige `protobuf >= 6.0`, mas `opentelemetry-proto 1.22.0` (dependência do Airflow 2.8) exige `protobuf < 5.0` — conflito irresolvível no mesmo ambiente Python.

**Solução:** DBT instalado em virtualenv isolado `/opt/dbt-venv`:

```dockerfile
FROM apache/airflow:2.8.0-python3.11

USER root
RUN python -m venv /opt/dbt-venv && chown -R airflow: /opt/dbt-venv

USER airflow
RUN PIP_USER=false /opt/dbt-venv/bin/pip install --no-cache-dir \
    "dbt-core==1.11.6" \
    "dbt-athena-community==1.10.0" \
    ...

RUN pip install --no-cache-dir \
    "apache-airflow-providers-amazon==8.19.0" \
    "boto3==1.42.59"
```

> `PIP_USER=false` necessário porque a imagem do Airflow define `PIP_USER=true` por padrão, o que conflita com instalações em virtualenv.

### Subindo o ambiente

```bash
# Primeira vez
docker-compose build

docker-compose up airflow-init

# Serviços
docker-compose up -d airflow-webserver airflow-scheduler

# Interface: http://localhost:8080
```

Configure a conexão AWS em **Admin → Connections**:
- Connection Id: `aws_default`
- Connection Type: `Amazon Web Services`
- Extra: `{"region_name": "us-east-1"}`

### DAG: `enem_pipeline`

Operators utilizados:

| Operator | Task | Função |
|----------|------|--------|
| `EmptyOperator` | `inicio_pipeline` | Marca início do grafo |
| `S3KeySensor` | `aguardar_arquivo_bronze_*` | Verifica se dados existem no S3 antes de prosseguir |
| `GlueCrawlerOperator` | `disparar_glue_crawler` | Dispara o crawler nativamente |
| `GlueCrawlerSensor` | `aguardar_glue_crawler` | Aguarda crawler terminar sem polling manual |
| `BashOperator` | `dbt_run` / `dbt_test` | Executa DBT no venv isolado |
| `BranchPythonOperator` | `checar_qualidade_marts` | Fluxo condicional baseado em dados reais |
| `PythonOperator` | `alertar_dados_vazios` | Alerta se marts estiverem vazios |
| `EmptyOperator` | `pipeline_concluido` | Marca fim bem-sucedido |

Fluxo:
```
inicio
  ├── aguardar_arquivo_bronze_2023 ──┐
  └── aguardar_arquivo_bronze_2024 ──┤
                                     ▼
                            disparar_glue_crawler
                                     ▼
                            aguardar_glue_crawler
                                     ▼
                                  dbt_run
                                     ▼
                                 dbt_test
                                     ▼
                          checar_qualidade_marts
                            ┌────────┴────────┐
                     pipeline_concluido  alertar_dados_vazios
```

### ⚠️ Erros encontrados

| Erro | Causa | Solução |
|------|-------|---------|
| `ModuleNotFoundError: airflow.sensors.s3_key` | Módulo movido no Airflow 2.8 | Import corrigido para `airflow.providers.amazon.aws.sensors.s3` |
| Sensor em loop infinito | `bucket_key` sem wildcard não encontrava arquivos Parquet | Adicionado `/*.parquet` no `bucket_key` |
| `dbt: command not found` (exit code 127) | BashOperator usava `dbt` do PATH mas dbt está no venv | Substituído por `/opt/dbt-venv/bin/dbt` |

---

## 📊 Análises Implementadas

1. **Renda × Escola × Desempenho** — impacto combinado de renda familiar e tipo de escola nas notas
2. **Desigualdade racial** — evolução entre 2023 e 2024 por cor/raça com % de escola pública
3. **Mobilidade educacional** — escolaridade da mãe vs desempenho dos filhos, revelando desvantagem acumulada. A escolha pela escolaridade materna tem embasamento na literatura acadêmica: estudos de Bourdieu sobre capital cultural e pesquisas do INEP mostram que Q002 tem correlação mais forte com o desempenho do que Q001, dado o maior tempo de contato materno com o desenvolvimento cognitivo dos filhos.

---

## 🔑 Aprendizados Técnicos

- **Schema evolution:** microdados do ENEM mudaram estrutura entre anos — sempre valide contra o dicionário oficial
- **Conflito de dependências Python:** virtualenv isolado como solução para bibliotecas com requisitos incompatíveis
- **Custo no Athena:** Parquet + particionamento por `ano` + seleção de colunas reduz custo em ~10x vs CSV
- **DBT vs SQL puro:** normalização feita uma vez no staging evita repetição de lógica complexa em todas as queries downstream
- **Sensors vs polling manual:** `GlueCrawlerSensor` com `mode="reschedule"` libera workers do Airflow enquanto aguarda

---

## 📄 Licença

MIT — dados utilizados são públicos, disponibilizados pelo INEP via [portal de microdados](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem).
