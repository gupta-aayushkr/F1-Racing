# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest_qualifying_file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])


# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualifying_id") \
.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
        .withColumnRenamed("constructorId", "constructor_id") \
            .withColumn("data_source", lit(v_data_source)) \
                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

final_df = add_ingestion_date(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_processed.qualifying")

# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = "tgt.qualifying_id = src.qualifying_id AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")