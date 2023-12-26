# Databricks notebook source
# Set Azure Storage account configuration
spark.conf.set("fs.azure.account.auth.type.formula01dl.dfs.core.windows.net", "SAS")

# Set SAS token provider type
spark.conf.set("fs.azure.sas.token.provider.type.formula01dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

# Set the fixed SAS token directly
spark.conf.set("fs.azure.sas.fixed.token.formula01dl.dfs.core.windows.net", "sp=rl&st=2023-12-13T16:00:17Z&se=2023-12-14T00:00:17Z&spr=https&sv=2022-11-02&sr=c&sig=vroTCSiN4BTMSdQ%2BXroWa1Tynpz4T38WpovHCV8Da0M%3D")


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula01dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula01dl.dfs.core.windows.net"))

# COMMAND ----------

