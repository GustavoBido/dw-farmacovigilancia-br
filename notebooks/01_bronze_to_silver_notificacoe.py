# Databricks notebook source
print("Hello Databricks!")

# COMMAND ----------

# Definindo a conta e a key (substitua pelos seus valores reais)
storage_account = "xxxxxxxxx"
account_key = "xxxxxxxxxxxxxxxxx"

# Configurando o acesso com a key
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    account_key
)

# Definindo caminhos para os containers
bronze_base = f"abfss://bronze@{storage_account}.dfs.core.windows.net"
silver_base = f"abfss://silver@{storage_account}.dfs.core.windows.net"
gold_base   = f"abfss://gold@{storage_account}.dfs.core.windows.net"


# COMMAND ----------

display(dbutils.fs.ls(f"{bronze_base}/"))

# COMMAND ----------

display(dbutils.fs.ls(f"{bronze_base}/vigimed/"))

# COMMAND ----------

df_notif = (
    spark.read
    .option("header", True)      
    .option("sep", ";")          # separador é um ;
    .option("encoding", "ISO-8859-1") 
    .csv(caminho_notif)         
)

display(df_notif.limit(10))

df_notif.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

# Função: transforma strings com 8 dígitos (yyyyMMdd) em DATE; caso contrário vira null
def to_date_yyyyMMdd(colname):
    col = F.trim(F.col(colname))
    return F.when(
        (F.length(col) == 8) & col.rlike("^[0-9]{8}$"),
        F.to_date(col, "yyyyMMdd")
    ).otherwise(F.lit(None).cast("date"))

df_notif_silver = (
    df_notif
    .select([F.when(F.trim(F.col(c)).isin("", "NULL", "N/A"), None)
               .otherwise(F.trim(F.col(c))).alias(c)
             for c in df_notif.columns])
    
    # Datas no padrão yyyyMMdd
    .withColumn("DATA_INCLUSAO_SISTEMA", to_date_yyyyMMdd("DATA_INCLUSAO_SISTEMA"))
    .withColumn("DATA_ULTIMA_ATUALIZACAO", to_date_yyyyMMdd("DATA_ULTIMA_ATUALIZACAO"))
    .withColumn("DATA_NOTIFICACAO", to_date_yyyyMMdd("DATA_NOTIFICACAO"))
    .withColumn("DATA_NASCIMENTO", to_date_yyyyMMdd("DATA_NASCIMENTO"))
    # Numéricos básicos
    .withColumn("IDADE_MOMENTO_REACAO", F.col("IDADE_MOMENTO_REACAO").cast("int"))
    .withColumn("PESO_KG", F.regexp_replace("PESO_KG", ",", ".").cast("double"))
    .withColumn("ALTURA_CM", F.regexp_replace("ALTURA_CM", ",", ".").cast("double"))

    .withColumn("extract_date", F.lit("2025-09-12"))
)

# Grava no Silver como Delta (sobrescreve)
(df_notif_silver.write
 .format("delta")
 .mode("overwrite")
 .save(f"{silver_base}/vigimed/notificacoes"))

print("Datas tratadas (yyyyMMdd) e Silver regravado.")

# COMMAND ----------

# Ler de volta o Delta salvo no Silver
df_check = spark.read.format("delta").load(f"{silver_base}/vigimed/notificacoes")

print("Total de linhas:", df_check.count())
display(df_check.limit(10))
