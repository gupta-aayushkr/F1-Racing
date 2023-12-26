# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.formula01dl.dfs.core.windows.net","bkG86yhaWPe687sDeT32imxnHq4zj9Kuv0gbgs4vM9OsOtsJaEV2ZrFyomfg2VqmigtkLcPBmCFr+ASt/WvALw=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula01dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula01dl.dfs.core.windows.net"))

# COMMAND ----------

