# Databricks notebook source
# MAGIC %md
# MAGIC ### Dynamic_format

# COMMAND ----------

dbutils.widgets.text("table_name","")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation",
            f"abfss://bronzedb@akashdeltalake.dfs.core.windows.net/schema/{table_name}") \
    .load(f"abfss://source@databricksetedl.dfs.core.windows.net/{table_name}")

# COMMAND ----------

df.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation",
            f"abfss://bronzedb@akashdeltalake.dfs.core.windows.net/checkpoints/{table_name}") \
    .trigger(once=True) \
    .start(f"abfss://bronzedb@akashdeltalake.dfs.core.windows.net/{table_name}")

# COMMAND ----------

display(
    spark.read.format("delta")
    .load("abfss://bronzedb@akashdeltalake.dfs.core.windows.net/orders")
)

# COMMAND ----------

