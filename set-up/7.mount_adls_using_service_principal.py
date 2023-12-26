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