-- Databricks notebook source
create database if not exists f1_processed
location "/mnt/formula01dl/processed"

-- COMMAND ----------

DESC DATABASE f1_raw;

-- COMMAND ----------

desc extended f1_processed.circuits;

-- COMMAND ----------

