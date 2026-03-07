# Databricks notebook source
# MAGIC %md
# MAGIC ### **Fact Orders**

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Reading**

# COMMAND ----------

df = spark.sql("select * from etl_project.silver.orders")
df.display()

# COMMAND ----------

df_dimcus = spark.sql("""
select DimCustomerKey,
       customer_id as dim_customer_id
from etl_project.gold.dimcustomers
""")

df_dimpro = spark.sql("""
select product_id as DimProductKey,
       product_id as dim_product_id
from etl_project.gold.dimproducts
""")

# COMMAND ----------

# MAGIC %md
# MAGIC **Fact DataFrame**

# COMMAND ----------

df_fact = df.join(df_dimcus,
                  df['customer_id'] == df_dimcus['dim_customer_id'],
                  how='left') \
            .join(df_dimpro,
                  df['product_id'] == df_dimpro['dim_product_id'],
                  how='left')

df_fact_new = df_fact.drop('dim_customer_id','dim_product_id', 'customer_id','product_id')

# COMMAND ----------

df_fact_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Upsert on fact table**

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists("etl_project.gold.factorders"):

    dlt_obj = DeltaTable.forName(spark, "etl_project.gold.factorders")

    dlt_obj.alias("trg").merge(
        df_fact_new.alias("src"),
        "trg.order_id = src.order_id AND trg.DimCustomerKey = src.DimCustomerKey AND trg.DimProductKey = src.DimProductKey"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

else:

    df_fact_new.write.format("delta") \
        .option("path", "abfss://golddb@akashdeltalake.dfs.core.windows.net/FactOrders") \
        .saveAsTable("etl_project.gold.factorders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from etl_project.gold.factorders