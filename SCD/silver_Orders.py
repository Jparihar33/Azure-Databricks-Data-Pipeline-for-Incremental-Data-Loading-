# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Reading
# MAGIC

# COMMAND ----------

df=spark.read.format('parquet')\
        .load('abfss://bronze@databricksete.dfs.core.windows.net/orders')

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df=df.withColumnRenamed('_rescued_data','rescue_data')
df.display()

# COMMAND ----------

df.drop('rescued_data')

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn('order_date',to_timestamp(col('order_date')))
df.display()

# COMMAND ----------

df=df.withColumn('year',year(col('order_date')))
df.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df1=df.withColumn('flag',dense_rank()over(window.PartitionBy('year').orderBy(desc('total_amount')))) 
df1.display()

# COMMAND ----------

df1=df1.withColumn('Rank_flag',rank()over(Window.partitionBy('year').orderBy(desc('total_amount'))))
df1.display()

# COMMAND ----------

df1=df1.withColumn('Row_flag',row_number()over(Window.partitionBy('year').orderBy(desc('total_amount'))))
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Classes - Object Oriented Programming

# COMMAND ----------

class Window:
    def __init__(self,df):
        self.df=df

    def rank(self,df):
        df_rank=df.withColumn('rank_flag',rank().over(Window.partitionBy('year').orderBy(desc('total_amount'))))
        return df_rank

    def dense_rank(self,df):
        df_dense_rank=df.withColumn('flag',dense_rank().over(Window.partitionBy('year').orderBy(desc('total_amount'))))
        return df_dense_rank
    
    def row_number(self,df):
        df_row_number=df.withColumn('rowNumber_flag',rank().over(Window.partitionBy('year').orderBy(desc('total_amount'))))
        return df_row_number
    
       



# COMMAND ----------

df_new=df

# COMMAND ----------

df_new.display()

# COMMAND ----------

obj=Window

# COMMAND ----------

df_new=obj.dense_rank(df_new)

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Writing
# MAGIC
# MAGIC

# COMMAND ----------

df.write.format('delta')\
        .mode('append')\
        .save('abfss://silver@databricksete.dfs.core.windows.net/orders')