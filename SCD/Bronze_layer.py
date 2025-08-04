# Databricks notebook source
# MAGIC %md
# MAGIC ###Dynamic Capabilities

# COMMAND ----------

dbutils.widgets.text('file_name','customers')

# COMMAND ----------

dbutils.widgets.get('file_name')

# COMMAND ----------

p_file_name=dbutils.widgets.get('file_name')

# COMMAND ----------

p_file_name

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Reading

# COMMAND ----------

df=spark.read.format('cLoudFiles')\
             .option('cloudFiles.format','parquet')\
             .option('cloudFiles.schemaLocation',f"bfss://bronze@databricksete.dfs.core.windows.net/checkpoint_{p_file_name}")\
               .load(f"abfss://source@databricksete.dfs.core.windows.net/{p_file_name}")   

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Data Writing

# COMMAND ----------

spark.writeStream.format('parquet')\
                 .outputMode('overwrite')\
                 .option('CheckpointLocation',f"abfss://bronze@databricksete.dfs.core.windows.net/checkpoint_{p_file_name}")\
                 .option('path',f'abfss://bronze@databricksete.dfs.core.windows.net/{p_file_name}')
                 .trigger(once=True)\
                 .start() 

# COMMAND ----------

df=spark.read.format('parquet')\
             .option('load',f"abfss://bronze@datalakesete.dfs.core.windows.net/{p_file_name}")

display(df)             