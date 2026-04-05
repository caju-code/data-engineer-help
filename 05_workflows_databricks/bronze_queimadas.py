# Databricks notebook source
# MAGIC %md
# MAGIC # ingestão queimadas
# MAGIC

# COMMAND ----------

print("criando tabela workspace.bronze.queimadas ...")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.bronze.queimadas
# MAGIC   USING delta AS -- tabela final será Delta, habilita replace table
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   read_files(
# MAGIC     '/Volumes/workspace/default/raw_data/queimadas.csv',
# MAGIC     format => 'csv',
# MAGIC     header => true,
# MAGIC     inferSchema => true,
# MAGIC     schemaEvolutionMode => 'none' -- desabilita _rescued_data col (schema inference)
# MAGIC   );

# COMMAND ----------

print("tabela workspace.bronze.queimadas criada!")