# Databricks notebook source
dbutils.fs.mount( source = 'wasbs://gold@kc123.blob.core.windows.net', 
                 mount_point= '/mnt/gold/kc12', extra_configs ={'fs.azure.sas.gold.kc123.blob.core.windows.net':'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-01-19T20:15:35Z&st=2025-01-17T12:15:35Z&spr=https&sig=hHshdAsZ8Iw07e91peukoTTAiK27MtrfYPTuuffPf%2B0%3D'})

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremantal_flag = dbutils.widgets.get('incremental_flag')
print(type(incremantal_flag))

# COMMAND ----------

df_scr=spark.sql("select distinct(Model_ID) as Model_ID, model_category from parquet.`/mnt/silver/kc12/processed`")
display(df_scr)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### dim model_sinc 
# MAGIC
# MAGIC

# COMMAND ----------

if spark.catalog.tableExists("gold.dim_model"):    
    
    df_sinc=spark.sql(''' 
            select dim_model_key, Model_ID , model_category
            from parquet.`/mnt/silver/kc12/processed` 
            ''' )

else : 

    df_sinc=spark.sql(''' 
            select 1 as dim_model_key, Model_ID , model_category 
            from parquet.`/mnt/silver/kc12/processed` 
            where 1=0
            ''' )



# COMMAND ----------

df_filter = df_scr.join(df_sinc, df_scr['Model_ID'] == df_sinc['Model_ID'], 'left').select(df_scr['Model_ID'], df_scr['model_category'],df_sinc['dim_model_key'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------



# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_model_key').isNotNull())

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_model_key').isNull()).select(df_scr['Model_ID'], df_scr['model_category'])

# COMMAND ----------

display(df_filter_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ###   create surrogate key
# MAGIC     

# COMMAND ----------

if (incremantal_flag == '0'):
    max_value = 1
else:
    max_value_df = spark.sql("select max(dim_model_key) from gold.dim_model")
    max_value = max_value_df.collect()[0][0]

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_model_key', max_value+monotonically_increasing_id())

# COMMAND ----------

display(df_filter_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ### create final DF df_filter_old + df_filter_new 

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD TYPE 1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

if spark.catalog.tableExists('gold.dim_model'):
     delta_table = DeltaTable.forPath(spark, "/mnt/gold/kc12/dim_model")
     delta_table.alias("trg").merge(df_final.alias("src"), "trg.dim_model_key = src.dim_model_key")\
             .whenMatchedUpdateAll()\
             .whenNotMatchedInsertAll()\
             .execute()



else : #initial run 
    df_final.write.format("delta")\
       .mode("overwrite")\
       .option("path","/mnt/gold/kc12/dim_model")\
       .saveAsTable("gold.dim_model")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dim_model