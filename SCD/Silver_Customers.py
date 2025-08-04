# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.format('parquet')\
        .load('abfss://bronze@databricksete.dfs.core.windows.net/customer')
df.display()        

# COMMAND ----------

df=df.drop('rescued_data')

# COMMAND ----------

df=df.withColumn('domains',split(col('email'),'@')[1])
df.display()

# COMMAND ----------

df.groupBy('domains').agg(count('customer_id').alias('Total_customers')).sort('Total_customers',ascending=False).display()

# COMMAND ----------

df_gmail=df.filter(col('domains')=='gmail.com')
df.gmail()

df_yahoo=df.filter(col('domains')=='yahoo.com')
df_yahoo.display()


df_hotmail=df.filter(col('domains')=='hotmail.com')
df_hotmail.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###concatenation

# COMMAND ----------

df=df.withColumn('full_name',concat(col('first_name'),lit(' '),col(last_name)))
df.drop('first_name','last_name')

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Writing

# COMMAND ----------

df.write.format('delta')\
        .mode('append')\
        .save('abfss://silver@databricksete.dfs.core.windows.net/customers')

# COMMAND ----------

df=spark.read.foramt('delta')\
        .load('abfss://silver@databricksete.dfs.core.windows.net/customers')