# Databricks notebook source
# Configuração inicial para acessar o ADLS Gen2

storage_account = "xxxxx" 
account_key = "xxxxxxxxxx"

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    account_key
)

# Caminhos base para os containers
bronze_base = f"abfss://bronze@{storage_account}.dfs.core.windows.net"
silver_base = f"abfss://silver@{storage_account}.dfs.core.windows.net"
gold_base   = f"abfss://gold@{storage_account}.dfs.core.windows.net"


# COMMAND ----------

display(dbutils.fs.ls(f"{bronze_base}/"))

# COMMAND ----------

display(dbutils.fs.ls(f"{bronze_base}/vigimed/"))

# COMMAND ----------

from pyspark.sql import functions as F

caminho_reac = f"{bronze_base}/vigimed/VigiMed_Reacoes.csv"

df_reacs_raw = (
    spark.read
    .option("header", True)         
    .option("sep", ";")            
    .option("encoding", "ISO-8859-1")  
    .csv(caminho_reac)
)

# COMMAND ----------

placeholders = {"", "NULL", "N/A", "n/a", "Null", "NaN"}
df_reacs_clean = df_reacs_raw.select(
    [
        F.when(F.trim(F.col(c)).isin(*placeholders), None)
         .otherwise(F.trim(F.col(c))).alias(c)
        for c in df_reacs_raw.columns
    ]
)

# COMMAND ----------

display(df_reacs_clean.limit(10))

# COMMAND ----------

#TRATAMENTO DAS DATAS

def to_date_yyyyMMdd(colname: str):
    col = F.col(colname)
    return F.when(
        (F.length(col) == 8) & col.rlike("^[0-9]{8}$"),
        F.to_date(col, "yyyyMMdd")
    ).otherwise(F.lit(None).cast("date"))

df_reacs_silver = (
    df_reacs_clean
    # Datas de administração
    .withColumn("DATA_INICIO_HORA", to_date_yyyyMMdd("DATA_INICIO_HORA"))
    .withColumn("DATA_FINAL_HORA",    to_date_yyyyMMdd("DATA_FINAL_HORA"))

       # Gravidade: GRAVE -> boolean (Sim/Não)
    .withColumn(
        "GRAVE_BOOL",
        F.when(F.upper(F.col("GRAVE")) == "SIM", F.lit(True))
         .when(F.upper(F.col("GRAVE")) == "NÃO", F.lit(False))
         .when(F.upper(F.col("GRAVE")) == "NAO", F.lit(False))  # sem acento, por via das dúvidas
         .otherwise(F.lit(None).cast("boolean"))
    )


)


# COMMAND ----------

(
    df_reacs_silver.write
    .format("delta")
    .mode("overwrite")              
    .option("overwriteSchema", "true")  
    .save(f"{silver_base}/vigimed/reacoes")
)

print("Reações gravado no SILVER.")


# COMMAND ----------

df_check = spark.read.format("delta").load(f"{silver_base}/vigimed/reacoes")
print("Linhas:", df_check.count())
display(df_check.limit(10))  