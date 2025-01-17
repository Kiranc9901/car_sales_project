
from pyspark.sql.functions import *
from pyspark.sql.types import *



dbutils.widgets.text('incremental_flag','0')



incremantal_flag = dbutils.widgets.get('incremental_flag')
print(type(incremantal_flag))



df_scr=spark.sql("select distinct(Model_ID) as Model_ID, model_category from parquet.`/mnt/silver/kc12/processed`")
display(df_scr)



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





df_filter = df_scr.join(df_sinc, df_scr['Model_ID'] == df_sinc['Model_ID'], 'left').select(df_scr['Model_ID'], df_scr['model_category'],df_sinc['dim_model_key'])



df_filter.display()



df_filter_old = df_filter.filter(col('dim_model_key').isNotNull())



df_filter_old.display()



df_filter_new = df_filter.filter(col('dim_model_key').isNull()).select(df_scr['Model_ID'], df_scr['model_category'])



display(df_filter_new)



if (incremantal_flag == '0'):
    max_value = 1
else:
    max_value_df = spark.sql("select max(dim_model_key) from gold.dim_model")
    max_value = max_value_df.collect()[0][0]



df_filter_new = df_filter_new.withColumn('dim_model_key', max_value+monotonically_increasing_id())



display(df_filter_new)





df_final = df_filter_new.union(df_filter_old)



display(df_final)



from delta.tables import DeltaTable



spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

-

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

spark.sql('''select * from gold.dim_model''')
