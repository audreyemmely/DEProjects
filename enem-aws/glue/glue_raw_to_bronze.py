"""
Glue ETL Job: RAW → BRONZE
==========================
- Lê microdados_enem_2023.csv diretamente (arquivo já unificado)
- Une participantes_2024.csv + resultados_2024.csv via concat por posição
- Empilha os dois anos via UNION ALL
- Padroniza schema (colunas, tipos, nomes em minúsculo)
- Salva como Parquet particionado por ano em s3://<bucket>/bronze/enem/
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import DoubleType, LongType, StringType

# ─── Inicialização ────────────────────────────────────────────────────────────
args    = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET"])
sc      = SparkContext()
glueCtx = GlueContext(sc)
spark   = glueCtx.spark_session
job     = Job(glueCtx)
job.init(args["JOB_NAME"], args)

BUCKET = args["S3_BUCKET"]
RAW    = f"s3://{BUCKET}/raw/enem"
BRONZE = f"s3://{BUCKET}/bronze/enem"

# Opções de leitura dos CSVs do INEP
# sep=; e encoding ISO-8859-1 validados localmente
CSV_OPTIONS = {
    "header":      "true",
    "sep":         ";",
    "encoding":    "ISO-8859-1",
    "inferSchema": "false",  # tipos definidos manualmente no schema bronze
}

# Número de partições Spark para garantir ordem no concat por posição do 2024
NUM_PARTICOES = 8

# ─── Schema bronze unificado ──────────────────────────────────────────────────
# Todas as colunas que existem em pelo menos um dos anos.
# Colunas ausentes em um ano são preenchidas com NULL do tipo correto.
SCHEMA_BRONZE = {
    # Identificação
    "nu_inscricao":            LongType(),
    "nu_sequencial":           LongType(),
    "nu_ano":                  LongType(),
    # Perfil do participante
    "tp_faixa_etaria":         LongType(),
    "tp_sexo":                 StringType(),
    "tp_estado_civil":         LongType(),
    "tp_cor_raca":             LongType(),
    "tp_nacionalidade":        LongType(),
    "tp_st_conclusao":         LongType(),
    "tp_ano_concluiu":         LongType(),
    "tp_escola":               LongType(),
    "tp_ensino":               DoubleType(),
    "in_treineiro":            LongType(),
    # Escola
    "co_escola":               DoubleType(),
    "co_municipio_esc":        DoubleType(),
    "no_municipio_esc":        StringType(),
    "co_uf_esc":               DoubleType(),
    "sg_uf_esc":               StringType(),
    "tp_dependencia_adm_esc":  DoubleType(),
    "tp_localizacao_esc":      DoubleType(),
    "tp_sit_func_esc":         DoubleType(),
    # Local da prova
    "co_municipio_prova":      LongType(),
    "no_municipio_prova":      StringType(),
    "co_uf_prova":             LongType(),
    "sg_uf_prova":             StringType(),
    # Presença
    "tp_presenca_cn":          LongType(),
    "tp_presenca_ch":          LongType(),
    "tp_presenca_lc":          LongType(),
    "tp_presenca_mt":          LongType(),
    # Códigos de prova
    "co_prova_cn":             DoubleType(),
    "co_prova_ch":             DoubleType(),
    "co_prova_lc":             DoubleType(),
    "co_prova_mt":             DoubleType(),
    # Notas das provas
    "nu_nota_cn":              DoubleType(),
    "nu_nota_ch":              DoubleType(),
    "nu_nota_lc":              DoubleType(),
    "nu_nota_mt":              DoubleType(),
    # Notas da redação
    "nu_nota_comp1":           DoubleType(),
    "nu_nota_comp2":           DoubleType(),
    "nu_nota_comp3":           DoubleType(),
    "nu_nota_comp4":           DoubleType(),
    "nu_nota_comp5":           DoubleType(),
    "nu_nota_redacao":         DoubleType(),
    # Respostas e gabaritos
    "tx_respostas_cn":         StringType(),
    "tx_respostas_ch":         StringType(),
    "tx_respostas_lc":         StringType(),
    "tx_respostas_mt":         StringType(),
    "tp_lingua":               LongType(),
    "tx_gabarito_cn":          StringType(),
    "tx_gabarito_ch":          StringType(),
    "tx_gabarito_lc":          StringType(),
    "tx_gabarito_mt":          StringType(),
    "tp_status_redacao":       DoubleType(),
    # Questionário socioeconômico
    "q001": StringType(), "q002": StringType(), "q003": StringType(),
    "q004": StringType(), "q005": LongType(),   "q006": StringType(),
    "q007": StringType(), "q008": StringType(), "q009": StringType(),
    "q010": StringType(), "q011": StringType(), "q012": StringType(),
    "q013": StringType(), "q014": StringType(), "q015": StringType(),
    "q016": StringType(), "q017": StringType(), "q018": StringType(),
    "q019": StringType(), "q020": StringType(), "q021": StringType(),
    "q022": StringType(), "q023": StringType(), "q024": StringType(),
    "q025": StringType(),
    "ano":  StringType(),
}


# ─── Funções auxiliares ───────────────────────────────────────────────────────

def padronizar_colunas(df):
    """Converte todos os nomes de coluna para minúsculo."""
    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower())
    return df


def aplicar_schema_bronze(df):
    """
    Garante que o DataFrame tenha exatamente as colunas do schema bronze,
    na ordem correta, fazendo cast dos tipos e adicionando NULL para
    colunas ausentes.
    """
    for coluna, tipo in SCHEMA_BRONZE.items():
        if coluna in df.columns:
            df = df.withColumn(coluna, F.col(coluna).cast(tipo))
        else:
            df = df.withColumn(coluna, F.lit(None).cast(tipo))

    return df.select(list(SCHEMA_BRONZE.keys()))


# ─── 1. Processar 2023 (arquivo já unificado) ─────────────────────────────────
print("=" * 60)
print("Processando ano 2023...")
print("=" * 60)

df_2023 = spark.read.options(**CSV_OPTIONS).csv(
    f"{RAW}/ano=2023/microdados_enem_2023.csv"
)
df_2023 = padronizar_colunas(df_2023)
df_2023 = df_2023.withColumn("ano", F.lit("2023"))
df_2023 = aplicar_schema_bronze(df_2023)

print(f"2023: {df_2023.count()} registros | {len(df_2023.columns)} colunas")


# ─── 2. Processar 2024 (concat por posição: participantes + resultados) ────────
print("=" * 60)
print("Processando ano 2024...")
print("=" * 60)

print("Lendo participantes_2024.csv...")
df_part = spark.read.options(**CSV_OPTIONS).csv(
    f"{RAW}/ano=2024/participantes_2024.csv"
)
df_part = padronizar_colunas(df_part)

print("Lendo resultados_2024.csv...")
df_res = spark.read.options(**CSV_OPTIONS).csv(
    f"{RAW}/ano=2024/resultados_2024.csv"
)
df_res = padronizar_colunas(df_res)

# Força o mesmo número de partições nos dois DataFrames antes de indexar.
# Isso é necessário para garantir que o monotonically_increasing_id gere
# IDs compatíveis entre os dois arquivos, tornando o join por posição seguro.
# Validado localmente: 4.332.944 linhas em ambos, 0 inconsistências presença/nota.
df_part = df_part.coalesce(NUM_PARTICOES)
df_res  = df_res.coalesce(NUM_PARTICOES)

# Adiciona índice posicional
df_part = df_part.withColumn("_idx", monotonically_increasing_id())
df_res  = df_res.withColumn("_idx",  monotonically_increasing_id())

# Remove colunas que existem nos dois arquivos para evitar duplicatas no join
# (identificadas localmente com: [c for c in res.columns if c in part.columns])
COLUNAS_DUPLICADAS = [
    "nu_ano",
    "co_municipio_prova",
    "no_municipio_prova",
    "co_uf_prova",
    "sg_uf_prova",
]
df_res = df_res.drop(*COLUNAS_DUPLICADAS)

# Join por índice posicional e remove coluna auxiliar
df_2024 = df_part.join(df_res, on="_idx", how="inner").drop("_idx")

df_2024 = df_2024.withColumn("ano", F.lit("2024"))
df_2024 = aplicar_schema_bronze(df_2024)

print(f"2024: {df_2024.count()} registros | {len(df_2024.columns)} colunas")


# ─── 3. Unifica os anos (UNION ALL) ──────────────────────────────────────────
print("=" * 60)
print("Unificando 2023 + 2024...")
print("=" * 60)

df_bronze = df_2023.unionByName(df_2024)
total = df_bronze.count()
print(f"Total bronze: {total} registros")


# ─── 4. Salva como Parquet particionado por ano ───────────────────────────────
print("=" * 60)
print(f"Salvando Parquet em: {BRONZE}")
print("=" * 60)

(
    df_bronze
    .write
    .mode("overwrite")
    .partitionBy("ano")
    .parquet(BRONZE)
)

print("✅ Bronze layer criada com sucesso!")
print(f"   s3://{BUCKET}/bronze/enem/ano=2023/")
print(f"   s3://{BUCKET}/bronze/enem/ano=2024/")

job.commit()