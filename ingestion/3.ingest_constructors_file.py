# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingesting Constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/formula01dl/raw/

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

constructor_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructors_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Dropping unwanted columns

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructors_dropped_df \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_final_df)

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Writing output into Parquet

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable(f"f1_processed.constructors")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/constructors"))

# COMMAND ----------

dbutils.notebook.exit("Success")