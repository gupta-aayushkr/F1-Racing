# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Ingest races.csv file

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula01dl/raw/

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date" )

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races_df = spark.read.option("header", True).csv(f"{raw_folder_path}/{v_file_date}/races.csv")
display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Converting Struct Type

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

races_df = spark.read.option("header",True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Adding Ingestion TimeStamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_with_timestamp_df = add_ingestion_date(races_with_timestamp_df)

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Select only required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(
    col("raceId").alias("race_id"),
    col("year").alias("race_year"),
    col("round"),
    col("circuitId").alias("circuit_id"),
    col("name"),
    col("ingestion_date"),
    col("race_timestamp")
)


# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write data to datalake as parquet

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable(f"f1_processed.races")

# COMMAND ----------

# %fs
# ls /mnt/formuala01dl/processed/races

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

dbutils.notebook.exit("Success")