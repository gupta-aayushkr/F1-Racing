# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake using Service Principal
# MAGIC
# MAGIC ## Steps to follow
# MAGIC
# MAGIC 1. Get `client_id`, `tenant_id`, and `client_secret` from key vault.
# MAGIC 2. Set Spark Config with App/Client Id, Directory/Tenant Id & Secret.
# MAGIC 3. Call file system utility `mount` to mount the storage.
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount).
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key='client-id-adb')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='tenant-id-adb')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='client-secret-adb')

# COMMAND ----------

# client_id = "d3e1e6dd-eca2-47bf-84ce-67017f88c3a3"
# tenant_id = "8f1c9f30-0dbc-42d9-9b25-9777fa5e2e65"
# client_secret = "3gw8Q~eQatsidbNd-jtpEhIqMvKtfF.jXfxP6dqD"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula01dl.dfs.core.windows.net/",
  mount_point = "/mnt/formula01dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula01dl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula01dl/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1dl/demo')