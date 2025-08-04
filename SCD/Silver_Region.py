# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Reading
# MAGIC

# COMMAND ----------

df=spark.read.format('parquet')\
        .option('path','abfss://bronze@databricksete.dfs.core.windows.net/region')

df.display()        

# COMMAND ----------

df=df.drop('rescued_data')
df.display()

# COMMAND ----------

df.write.format('delta')\
        .mode('append')\
        .save('abfss://silver@databricksete.dfs.core.windows.net/region')

# COMMAND ----------

df=spark.read.format('delta')\
        .load('abfss://silver@databricksete.dfs.core.windows.net/region')

df.display()        