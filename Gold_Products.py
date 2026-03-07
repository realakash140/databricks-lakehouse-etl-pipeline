# Databricks notebook source
import dlt
from pyspark.sql.functions import *

my_rules = {
    "rule1": "product_id IS NOT NULL",
    "rule2": "product_name IS NOT NULL"
}

@dlt.table
@dlt.expect_all_or_drop(my_rules)
def dimproducts_stage():

    df = spark.readStream.table("etl_project.silver.products")

    return df

# COMMAND ----------

@dlt.view
def dimproducts_view():

    df = spark.readStream.table("LIVE.dimproducts_stage")

    return df

# COMMAND ----------

dlt.create_streaming_table("dimproducts")

# COMMAND ----------

dlt.apply_changes(
  target="dimproducts",
  source="dimproducts_view",
  keys=["product_id"],
  sequence_by="product_id",
  stored_as_scd_type=2
)