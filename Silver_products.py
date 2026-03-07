# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta") \
    .load("abfss://bronzedb@akashdeltalake.dfs.core.windows.net/products")

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

df = spark.read.format("delta") \
    .load("abfss://silverdb@akashdeltalake.dfs.core.windows.net/products")

# COMMAND ----------

from pyspark.sql.functions import expr

products_silver = df.withColumn(
    "discounted_price",
    expr("spotify_cata.silver.discount_func(price, 0.10)")
)

# COMMAND ----------

display(products_silver)

# COMMAND ----------

products_silver.write \
.format("delta") \
.mode("append") \
.save("abfss://silverdb@akashdeltalake.dfs.core.windows.net/products")

# COMMAND ----------

silver_df = spark.read.format("delta").load(
    "abfss://silverdb@akashdeltalake.dfs.core.windows.net/products"
)

display(silver_df)