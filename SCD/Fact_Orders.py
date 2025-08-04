# Databricks notebook source
# MAGIC %md
# MAGIC ### Fact Orders

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Data Reading

# COMMAND ----------

df=spark.sql('select * from databricks_cata.silver.orders_silver')
df.display()

# COMMAND ----------

dim_cust=spark.sql('select DimCustomerKey,customer_id as dim_customer_id from databricks_cata.gold.dimcustomers')

dim_prod=spark.sql('select DimProductKey,product_id as dim_product_id from databricks_cata.gold.dimproducts')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fact DataFrame

# COMMAND ----------

df_fact=df.join(dim_cust,dim_cust['customer_id']==df['customer_id']).join(dim_prod,df['product_id']==dim_prod['product_id'])

df_fact_new=df_fact.drop('dim_product_id','dim_customer_id','customer_id','product_id')

df_fact_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert on Fact Table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('databricks_cata.gold.FactOrders'):
    
    dlt_obj=DeltaTab.forName(spark,'databricks_cata.gold.FactOrders')
    dlt_obj.alias('trg').merge(df_fact_new.alias('src'),"trg.order_id=src.order_id AND trg.DimCustomerKey=src.DimCustomerKey,src.DimProductKey=trg.DimProductKey")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()        
else:
    df_fact_new.write.mode('overwrite')\
        .format('delta')\
        .option(path,'abfss://gold@databricksete.dfs.core.windows.net/FactOrders')
        .saveAsTable('databricks_cata.gold.FactOrders')    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.FactOrders