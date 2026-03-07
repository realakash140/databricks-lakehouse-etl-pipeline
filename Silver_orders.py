# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


# COMMAND ----------

df = spark.read.format("delta") \
    .load("abfss://bronzedb@akashdeltalake.dfs.core.windows.net/orders")

# COMMAND ----------

df = df.withColumnRenamed("_rescued_data", "rescued_data")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.drop("rescued_data")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Read Bronze from ADLS path (NOT table)
df = spark.read.format("delta") \
    .load("abfss://bronzedb@akashdeltalake.dfs.core.windows.net/orders")

# Drop rescued column
df = df.drop("_rescued_data")

# Convert date
df = df.withColumn("order_date", to_timestamp(col("order_date")))

# Add year
df = df.withColumn("year", year(col("order_date")))

# Window spec
window_spec = Window.partitionBy("year").orderBy(desc("total_amount"))

# Ranking columns
df = df.withColumn("rank_flag", dense_rank().over(window_spec))
df = df.withColumn("row_flag", row_number().over(window_spec))

# Save to Silver
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("spotify_cata.silver.orders")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

class windows:
    
    def dense_rank(self, df):
        
        df_dense_rank = df.withColumn(
            "flag",
            dense_rank().over(
                Window.partitionBy("year")
                .orderBy(desc("total_amount"))
            )
        )
        
        return df_dense_rank

# COMMAND ----------

df_new = df

# COMMAND ----------

obj = windows()

# COMMAND ----------

df_result = obj.dense_rank(df_new)

display(df_result)

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://silverdb@akashdeltalake.dfs.core.windows.net/orders")

# COMMAND ----------

