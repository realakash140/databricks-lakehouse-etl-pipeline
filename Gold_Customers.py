# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

df_silver = spark.read.table("etl_project.silver.customers")

display(df_silver)

# COMMAND ----------

df_silver = df_silver.dropDuplicates(["customer_id"])

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_silver = df_silver.withColumn("create_date", current_timestamp()) \
                     .withColumn("update_date", current_timestamp())

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, lit

df_silver = df_silver.withColumn(
    "DimCustomerKey",
    monotonically_increasing_id() + lit(1)
)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

table_exists = spark.catalog.tableExists("etl_project.gold.DimCustomers")

print(table_exists)

# COMMAND ----------

from delta.tables import DeltaTable

if not table_exists:

    df_silver.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("etl_project.gold.DimCustomers")

else:

    gold_table = DeltaTable.forName(spark, "etl_project.gold.DimCustomers")

    (
    gold_table.alias("t")
    .merge(
        df_silver.alias("s"),
        "t.customer_id = s.customer_id"
    )
    .whenMatchedUpdate(
        set={
            "email": "s.email",
            "city": "s.city",
            "state": "s.state",
            "domains": "s.domains",
            "full_name": "s.full_name",
            "update_date": "s.update_date"
        }
    )
    .whenNotMatchedInsert(
        values={
            "DimCustomerKey": "s.DimCustomerKey",
            "customer_id": "s.customer_id",
            "email": "s.email",
            "city": "s.city",
            "state": "s.state",
            "domains": "s.domains",
            "full_name": "s.full_name",
            "create_date": "s.create_date",
            "update_date": "s.update_date"
        }
    )
    .execute()
    )

# COMMAND ----------

df_gold = spark.read.table("etl_project.gold.DimCustomers")

display(df_gold)

# COMMAND ----------

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://golddb@akashdeltalake.dfs.core.windows.net/DimCustomers")

# COMMAND ----------

