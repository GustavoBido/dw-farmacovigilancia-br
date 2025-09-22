# Databricks notebook source
# Configuração inicial para acessar o ADLS Gen2

storage_account = "xxxxxxxxxxxx" 
account_key = "xxxxxxxxxxxxxxx"

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

caminho_meds = f"{bronze_base}/vigimed/VigiMed_Medicamentos.csv"

df_meds_raw = (
    spark.read
    .option("header", True)         
    .option("sep", ";")            
    .option("encoding", "ISO-8859-1")  
    .csv(caminho_meds)
)

# COMMAND ----------

placeholders = {"", "NULL", "N/A", "n/a", "Null", "NaN"}
df_meds_clean = df_meds_raw.select(
    [
        F.when(F.trim(F.col(c)).isin(*placeholders), None)
         .otherwise(F.trim(F.col(c))).alias(c)
        for c in df_meds_raw.columns
    ]
)

# COMMAND ----------

#TRATAMENTO DAS DATAS

def to_date_yyyyMMdd(colname: str):
    col = F.col(colname)
    return F.when(
        (F.length(col) == 8) & col.rlike("^[0-9]{8}$"),
        F.to_date(col, "yyyyMMdd")
    ).otherwise(F.lit(None).cast("date"))

df_meds_silver = (
    df_meds_clean
    # Datas de administração
    .withColumn("INICIO_ADMINISTRACAO", to_date_yyyyMMdd("INICIO_ADMINISTRACAO"))
    .withColumn("FIM_ADMINISTRACAO",    to_date_yyyyMMdd("FIM_ADMINISTRACAO"))

    .withColumn("RELACAO_MEDICAMENTO_EVENTO", F.initcap("RELACAO_MEDICAMENTO_EVENTO"))  
    .withColumn("CODIGO_ATC", F.upper("CODIGO_ATC"))                                     
    .withColumn("VIA_ADMINISTRACAO", F.initcap("VIA_ADMINISTRACAO"))
    .withColumn("FORMA_FARMACEUTICA", F.initcap("FORMA_FARMACEUTICA"))
)


# COMMAND ----------

#Gravando o DELTA no silver
(
    df_meds_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{silver_base}/vigimed/medicamentos")
)

print("Medicamentos gravado no SILVER.")

# COMMAND ----------

# Ler de volta o Delta salvo no Silver
df_check = spark.read.format("delta").load(f"{silver_base}/vigimed/medicamentos")

print("Total de linhas:", df_check.count())
display(df_check.limit(10))