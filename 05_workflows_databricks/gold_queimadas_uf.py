# Databricks notebook source
# MAGIC %md
# MAGIC ## fazer tabela gold

# COMMAND ----------

from pyspark.sql import functions as F

silver_database = 'workspace.silver'
silver_table = 'queimadas'

print(f"lendo tabela {silver_database}.{silver_table} e fazendo agregações ...")

df_gold = (
    spark.table(f"{silver_database}.{silver_table}")
    .groupBy("sigla_uf", "bioma", "mes", "ano")
    .agg(
        F.count("*").alias("total_focos"),
        F.avg("latitude").alias("lat_media"),
        F.avg("longitude").alias("lon_media")
    )
)

display(df_gold.limit(100))


# COMMAND ----------

gold_database = "workspace.gold"
gold_table = "queimadas_uf"

write_mode = "overwrite"
print(f"escrevendo tabela {gold_database}.{gold_table} em modo {write_mode}...")

try: 
    (
        df_gold.write.format("delta") #pode ser parquet, json...
        .option("overwriteSchema", "true")
        .mode(write_mode)
        .saveAsTable(f"{gold_database}.{gold_table}")
    )

    print(f"tabela {gold_database}.{gold_table} escrita com sucesso!")
except Exception as e:
    print(f"falha ao escrever tabela {gold_database}.{gold_table}: {e}")