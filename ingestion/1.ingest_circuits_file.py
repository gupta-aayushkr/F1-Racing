# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Ingest Circuits.csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date" )

# COMMAND ----------

v_data_source

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

raw_folder_path
processed_folder_path

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("urt", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read.option("header",True).schema(circuits_schema).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Select only required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    col("circuitId"),
    col("circuitRef"),
    col("name"),
    col("location"),
    col("country"),
    col("lat"),
    col("lng"),
    col("alt")
)


# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Steps 3 - Rename the Column Required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df \
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 4 - Add Ingestion Dates to the datafraem

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable(f"f1_processed.circuits")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

