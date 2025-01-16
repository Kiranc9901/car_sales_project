df=spark.read.format('parquet')\
    .option('inferSchema','true')\
    .load('/mnt/kc12/data_a4e3bac6-55a8-4989-95a3-3a3e16881a76_6558b524-87e6-482d-9ee2-26a8b0552eea.parquet')
display(df)
