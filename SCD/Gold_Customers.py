# Databricks notebook source
# MAGIC %md
# MAGIC ###Data Reading From Source

# COMMAND ----------

df=spark.sql('select * from databricks_cata.silver.customers')
df.display()

# COMMAND ----------

init_load_flag=int(dbutils.widgets.get('init_load_flag'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter old vs new records

# COMMAND ----------

if init_load_flag=0:

    df_old=spark.sql('''DimCustomerkey,customer_id,create_date,update_date
                     from databricks_cata.gold.DimCustomers''')
    
else:
     df_old=spark.sql('''0 DimCustomerkey, customer_id,0 create_date,0 update_date
                      from databricks_cata.silver.Silver_Customer where 1=0 ''')   

# COMMAND ----------

df_old=df.withColumnRenamed('DimCustomerkey','old_DimDustomerkey')\
          .withColumnRenamed('customer_id','old_customer_id')\
          .withColumnRenamed('create_date','old_create_date')\
          .withColumnRenamed('update_date','old_update_date')       

# COMMAND ----------

df_old.dispaly()

# COMMAND ----------

df_join = df.join(df_old,df[customer_id]==df_old[old_customer_id],how='left')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Separate old vs new Records

# COMMAND ----------

df_new=df_join.filter(df_join['old_DimCustomerkey'].isNUll())

# COMMAND ----------

df_old=df_join.filter(df_join['Old_DimCustomerKey'].isNotNull())

# COMMAND ----------

# Dropping unnecessary columns from old dataframe
df_old=df_old.drop('old_DimcustomerKey','old_customer_id','old_update_date')



#create update date and create date with current timestamp
df_old=df_old.withColumn('update_date',current_timestamp())\
                .wuthColumn('Create date',current_timestamp)

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Removing Duplicates
# MAGIC

# COMMAND ----------

df=df.dropDuplicates('customer_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Surrogate key

# COMMAND ----------

df=df.withColumn('DimCustomerkey',monotonically_increasing_id()+lit(1))
df.limit(15).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding max surrogate key
# MAGIC

# COMMAND ----------

if init_flag_load==1:
    max_surrogate_key=0

else:
    df_maxsur=spark.sql(''select max(DimCustomerKey)as max_surrogate_key from databricks_cata.gold.DimCustomer'')    


    max_surrogate_key=df_maxsur.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new=df_new.withColumn('DimCustomerKey',lit(max_surrogate_Key)+col("DimCustomerKey"))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Union of df_old vs df_new

# COMMAND ----------

df_final=df_old.unionByName(df_new)
df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SCD Type 1**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_cata.gold.DimCustomers"):

    dlt_obj=DeltaTable.forPath(spark,'abfss//gold@databricksete.dfs.core.windows.net/DimCustomers')

    dlt_obj.alias('trg').merge(df_final.alias('src'),trg['DimCustomerKey']=src['DimCustomerKey'])\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
    
else:
    df_final.write.mode('overwrite')\
    .format('delta')\
    .option('path','abfss://gold@databricksete.dfs.core.windows.net/DimCustomers')
    .saveAsTable('databricks_cata.gold.DimCustomers')         

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.Dimcustomers