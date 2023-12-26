# Databricks notebook source
v_result = dbutils.notebook.run("1.ingest_circuits_file",0,{"p_data_source":"Ergast_API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file",0,{"p_data_source":"Ergast_API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_file",0,{"p_data_source":"Ergast_API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4. Ingest drivers.json file",0,{"p_data_source":"Ergast_API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_file",0,{"p_data_source":"Ergast_API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest pit_stops.json file",0,{"p_data_source":"Ergast_API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_file",0,{"p_data_source":"Ergast_API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_file",0,{"p_data_source":"Ergast_API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE f1_processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

# %sql
# -- select *
# -- from f1_processed.results
# -- order by race_id desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table f1_processed.pit_stops;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES FROM f1_processed;

# COMMAND ----------

# 2021-03-21
# 2021-03-28
# 2021-04-18

# COMMAND ----------

