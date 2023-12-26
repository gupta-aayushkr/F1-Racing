# Databricks notebook source
# MAGIC %md
# MAGIC Explore the capabilites of dbutils.secret utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-scope', key='client-id-adb')
# dbutils.secrets.list(scope='formula1-scope', key='client-secret-adb')
# dbutils.secrets.list(scope='formula1-scope', key='tenant-id-adb')

# COMMAND ----------

