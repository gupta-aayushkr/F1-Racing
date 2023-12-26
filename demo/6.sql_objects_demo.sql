-- Databricks notebook source
create database if not exists demo

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managed Tables

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

desc extended race_results_python

-- COMMAND ----------

select * from race_results_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managed Table (SQL)

-- COMMAND ----------

create table race_results_sql
as
select * from race_results_python
where race_year = 2020;

-- COMMAND ----------

select * from race_results_sql

-- COMMAND ----------

select current_database()

-- COMMAND ----------

desc extended demo.race_results_sql

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

desc extended demo.race_results_ext_py

-- COMMAND ----------

