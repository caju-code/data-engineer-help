# Databricks notebook source
# MAGIC %md
# MAGIC ## criar tabela queimadas silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lendo tabela -> datafrane

# COMMAND ----------

database_name = 'workspace.bronze'
table_name = 'queimadas'

print(f"lendo tabela {database_name}.{table_name}...")

df_raw = spark.read.table(f'{database_name}.{table_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza de dados

# COMMAND ----------

# MAGIC %md
# MAGIC ajustes de null

# COMMAND ----------

from pyspark.sql.functions import col

print("tratando dados...")

df_raw.filter(col("dias_sem_chuva") == -999).count()

# COMMAND ----------

# quantidade de linhas sem dados de dias de chuva por localidade (long, lat)


(
    df_raw.filter(col("dias_sem_chuva") == -999)
    .groupBy("sigla_uf")
    .count()
    .orderBy(col("count"), ascending=False)
)

# COMMAND ----------

# converter pra null

df_cleaned = df_raw.replace(-999, None, "dias_sem_chuva")

df_cleaned.filter(col("dias_sem_chuva").isNull()).count()


# COMMAND ----------

# MAGIC %md
# MAGIC criando PK

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

print("criando pk para tabela...")

df_pk = df_cleaned.withColumn("id", monotonically_increasing_id())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrevendo tabela

# COMMAND ----------

path_table = 'workspace.silver'
table_name = 'queimadas'

print(f"escrevendo tabela {path_table}.{table_name}...")

write_mode = "overwrite"

try: 
    df_pk.write.mode(write_mode).saveAsTable(f'{path_table}.{table_name}')
    print(f"tabela {path_table}.{table_name} escrita com sucesso!")
except Exception as e:
    print(f"falha ao escrever tabela {path_table}.{table_name}: {e}")
