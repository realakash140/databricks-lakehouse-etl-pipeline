# Databricks notebook source
from pyspark.sql.functions import *

df = spark.read.format("delta") \
    .load("abfss://bronzedb@akashdeltalake.dfs.core.windows.net/customers")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumn("domains", split(col("email"), '@')[1])

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

df.groupBy("domains") \
  .agg(count("customer_id").alias("total_customers")) \
  .orderBy(desc("total_customers")) \
  .display()

# COMMAND ----------

from pyspark.sql.functions import *
import time

# COMMAND ----------

df_gmail = df.filter(col("domains") == "gmail.com")
time.sleep(5)

df_yahoo = df.filter(col("domains") == "yahoo.com")
time.sleep(5)

df_hotmail = df.filter(col("domains") == "hotmail.com")
time.sleep(5)

# COMMAND ----------

df = df.withColumn(
    "full_name",
    concat(col("first_name"), lit(" "), col("last_name"))
)

df = df.drop("first_name", "last_name")

display(df)

# COMMAND ----------

df.write \
  .mode("overwrite") \
  .format("delta") \
  .save("abfss://silverdb@akashdeltalake.dfs.core.windows.net/customers")