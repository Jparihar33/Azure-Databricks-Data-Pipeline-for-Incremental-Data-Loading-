# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.format('parquet')\
            .load('abfss://bronze@databricksete.dfs.core.windows.net/products')
df.display()            

# COMMAND ----------

df.drop('rescued_data')

# COMMAND ----------

df.display()

# COMMAND ----------

df.createorReplaceTempView('Products')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC Create Or Replace Function databricks_cata.bronze.discount_func(p_price double)
# MAGIC returns double
# MAGIC language sql
# MAGIC Return p_price*0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id,databricks_cata.bronze.discount_func(price) as discount_price from products

# COMMAND ----------

df=df.withColumn('discount_price',expr("databricks_cata.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC Create Or Replace Function databricks_cata.bronze.upper_func(p_brand string)
# MAGIC Returns string
# MAGIC Language Python
# MAGIC AS
# MAGIC $$
# MAGIC   return p_brand.upper() 
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id,brand,databricksete_cata.bronze.upper_func(brand) as brand_upper from products

# COMMAND ----------

df.write.format('delta')/
            .mode('append')/
            .option('path','abfss://silver@databricksete.dfs.core.windows.net/products')
            .save()