# Databricks notebook source
# MAGIC %md
# MAGIC ### DLT Pipeline

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ####Streaming Table

# COMMAND ----------

my_rules={
    'rule1':'product_id is not null ',
    'rule2':'product_name is not null'
}

# COMMAND ----------

@dlt.expect(my_rules)

# COMMAND ----------

@dlt.table():
    def DimProducts_stage():
        df=spark.readStream.table("databricks_cata.silver.silver_products")
        return df

# COMMAND ----------

# MAGIC %md
# MAGIC ####streaming view

# COMMAND ----------

@dlt.view():
    
def DimProduct_view():
    df=spark.readStream.table('Live.DimProducts_stage')
    
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ###DimProducts

# COMMAND ----------

dlt.create_streaming_table("DimProducts") 

# COMMAND ----------

dlt.apply_changes(
    target='DimProducts',
    source='DimProduct_view',
    keys=['Product_id'],
    sequence_by='product_id',
    stored_as_scd_type=2
)