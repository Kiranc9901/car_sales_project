df=spark.read.format('parquet')\
    .option('inferSchema','true')\
    .load('/mnt/kc12/data_a4e3bac6-55a8-4989-95a3-3a3e16881a76_6558b524-87e6-482d-9ee2-26a8b0552eea.parquet')
display(df)

+---------+---------+--------+--------+----------+-------+---+-----+----+--------------------+--------------------+
|Branch_ID|Dealer_ID|Model_ID| Revenue|Units_Sold|Date_ID|Day|Month|Year|          BranchName|          DealerName|
+---------+---------+--------+--------+----------+-------+---+-----+----+--------------------+--------------------+
|   BR0001|  DLR0001|  BMW-M1|13363978|         2|DT00001|  1|    1|2017|      AC Cars Motors|      AC Cars Motors|
|   BR0003|  DLR0228|Hon-M218|17376468|         3|DT00001| 10|    5|2017|      AC Cars Motors|       Deccan Motors|
|   BR0004|  DLR0208|Tat-M188| 9664767|         3|DT00002| 12|    1|2017|      AC Cars Motors|     Wiesmann Motors|
|   BR0005|  DLR0188|Hyu-M158| 5525304|         3|DT00002| 16|    9|2017|      AC Cars Motors|       Subaru Motors|
|   BR0006|  DLR0168|Ren-M128|12971088|         3|DT00003| 20|    5|2017|      AC Cars Motors|         Saab Motors|
|   BR0008|  DLR0128| Hon-M68| 7321228|         1|DT00004| 28|    4|2017|      AC Cars Motors|Messerschmitt Motors|
|   BR0009|  DLR0108| Cad-M38|11379294|         2|DT00004| 31|   12|2017|      AC Cars Motors|        Lexus Motors|
|   BR0010|  DLR0088|  Mer-M8|11611234|         2|DT00005|  4|    9|2017|      AC Cars Motors|IFA (including Tr...|
|   BR0011|  DLR0002|  BMW-M2|19979446|         2|DT00005|  2|    1|2017|        Acura Motors|        Acura Motors|
|   BR0011|  DLR0069|Vol-M256|14181510|         3|DT00006|  9|    5|2017|        Acura Motors|          Geo Motors|
|   BR0012|  DLR0249|BMW-M249| 5358057|         1|DT00006|  6|    9|2017|        Acura Motors|        Acura Motors|
|   BR0013|  DLR0229|Hon-M219|16150431|         3|DT00007| 11|    5|2017|        Acura Motors|       Herald Motors|
|   BR0014|  DLR0209|Tat-M189|13389350|         2|DT00007| 13|    1|2017|        Acura Motors|      Zastava Motors|
|   BR0015|  DLR0189|Hyu-M159| 4891618|         2|DT00008| 17|    9|2017|        Acura Motors|      Sunbeam Motors|
|   BR0017|  DLR0149| Lex-M99| 5059144|         2|DT00008| 25|    8|2017|        Acura Motors|        Panoz Motors|
|   BR0018|  DLR0129| Hon-M69|17369466|         2|DT00009| 29|    4|2017|        Acura Motors|          Mia Motors|
|   BR0019|  DLR0109| Cad-M39|26969532|         3|DT00010|  1|    1|2017|        Acura Motors|       Ligier Motors|
|   BR0020|  DLR0089|  Dod-M9| 4816794|         2|DT00011|  5|    9|2017|        Acura Motors|     Infiniti Motors|
|   BR0021|  DLR0070|Vol-M257| 7738896|         1|DT00011| 10|    5|2017|Aixam-Mega (inclu...|      Gilbern Motors|
|   BR0024|  DLR0210|Tat-M190|11038722|         3|DT00012| 14|    1|2017|Aixam-Mega (inclu...|          ZAZ Motors|
+---------+---------+--------+--------+----------+-------+---+-----+----+--------------------+--------------------+
only showing top 20 rows
# Appling the split transformation on Model_ID column and removed "-" and created new column model_category

from pyspark.sql.functions import *
df=df.withColumn('model_category',split(col('Model_Id'),'-')[0])
df.show()

+---------+---------+--------+--------+----------+-------+---+-----+----+--------------------+--------------------+--------------+
|Branch_ID|Dealer_ID|Model_ID| Revenue|Units_Sold|Date_ID|Day|Month|Year|          BranchName|          DealerName|model_category|
+---------+---------+--------+--------+----------+-------+---+-----+----+--------------------+--------------------+--------------+
|   BR0001|  DLR0001|  BMW-M1|13363978|         2|DT00001|  1|    1|2017|      AC Cars Motors|      AC Cars Motors|           BMW|
|   BR0003|  DLR0228|Hon-M218|17376468|         3|DT00001| 10|    5|2017|      AC Cars Motors|       Deccan Motors|           Hon|
|   BR0004|  DLR0208|Tat-M188| 9664767|         3|DT00002| 12|    1|2017|      AC Cars Motors|     Wiesmann Motors|           Tat|
|   BR0005|  DLR0188|Hyu-M158| 5525304|         3|DT00002| 16|    9|2017|      AC Cars Motors|       Subaru Motors|           Hyu|
|   BR0006|  DLR0168|Ren-M128|12971088|         3|DT00003| 20|    5|2017|      AC Cars Motors|         Saab Motors|           Ren|
|   BR0008|  DLR0128| Hon-M68| 7321228|         1|DT00004| 28|    4|2017|      AC Cars Motors|Messerschmitt Motors|           Hon|
|   BR0009|  DLR0108| Cad-M38|11379294|         2|DT00004| 31|   12|2017|      AC Cars Motors|        Lexus Motors|           Cad|
|   BR0010|  DLR0088|  Mer-M8|11611234|         2|DT00005|  4|    9|2017|      AC Cars Motors|IFA (including Tr...|           Mer|
|   BR0011|  DLR0002|  BMW-M2|19979446|         2|DT00005|  2|    1|2017|        Acura Motors|        Acura Motors|           BMW|
|   BR0011|  DLR0069|Vol-M256|14181510|         3|DT00006|  9|    5|2017|        Acura Motors|          Geo Motors|           Vol|
|   BR0012|  DLR0249|BMW-M249| 5358057|         1|DT00006|  6|    9|2017|        Acura Motors|        Acura Motors|           BMW|
|   BR0013|  DLR0229|Hon-M219|16150431|         3|DT00007| 11|    5|2017|        Acura Motors|       Herald Motors|           Hon|
|   BR0014|  DLR0209|Tat-M189|13389350|         2|DT00007| 13|    1|2017|        Acura Motors|      Zastava Motors|           Tat|
|   BR0015|  DLR0189|Hyu-M159| 4891618|         2|DT00008| 17|    9|2017|        Acura Motors|      Sunbeam Motors|           Hyu|
|   BR0017|  DLR0149| Lex-M99| 5059144|         2|DT00008| 25|    8|2017|        Acura Motors|        Panoz Motors|           Lex|
|   BR0018|  DLR0129| Hon-M69|17369466|         2|DT00009| 29|    4|2017|        Acura Motors|          Mia Motors|           Hon|
|   BR0019|  DLR0109| Cad-M39|26969532|         3|DT00010|  1|    1|2017|        Acura Motors|       Ligier Motors|           Cad|
|   BR0020|  DLR0089|  Dod-M9| 4816794|         2|DT00011|  5|    9|2017|        Acura Motors|     Infiniti Motors|           Dod|
|   BR0021|  DLR0070|Vol-M257| 7738896|         1|DT00011| 10|    5|2017|Aixam-Mega (inclu...|      Gilbern Motors|           Vol|
|   BR0024|  DLR0210|Tat-M190|11038722|         3|DT00012| 14|    1|2017|Aixam-Mega (inclu...|          ZAZ Motors|           Tat|
+---------+---------+--------+--------+----------+-------+---+-----+----+--------------------+--------------------+--------------+
only showing top 20 rows


#Created new column RevPerUnit

df=df.withColumn('RevPerUnit',col('Revenue')/col('Units_Sold'))
df.show()
                                                                                     
+---------+---------+--------+--------+----------+-------+---+-----+----+--------------------+--------------------+--------------+----------+
|Branch_ID|Dealer_ID|Model_ID| Revenue|Units_Sold|Date_ID|Day|Month|Year|          BranchName|          DealerName|model_category|RevPerUnit|
+---------+---------+--------+--------+----------+-------+---+-----+----+--------------------+--------------------+--------------+----------+
|   BR0001|  DLR0001|  BMW-M1|13363978|         2|DT00001|  1|    1|2017|      AC Cars Motors|      AC Cars Motors|           BMW| 6681989.0|
|   BR0003|  DLR0228|Hon-M218|17376468|         3|DT00001| 10|    5|2017|      AC Cars Motors|       Deccan Motors|           Hon| 5792156.0|
|   BR0004|  DLR0208|Tat-M188| 9664767|         3|DT00002| 12|    1|2017|      AC Cars Motors|     Wiesmann Motors|           Tat| 3221589.0|
|   BR0005|  DLR0188|Hyu-M158| 5525304|         3|DT00002| 16|    9|2017|      AC Cars Motors|       Subaru Motors|           Hyu| 1841768.0|
|   BR0006|  DLR0168|Ren-M128|12971088|         3|DT00003| 20|    5|2017|      AC Cars Motors|         Saab Motors|           Ren| 4323696.0|
|   BR0008|  DLR0128| Hon-M68| 7321228|         1|DT00004| 28|    4|2017|      AC Cars Motors|Messerschmitt Motors|           Hon| 7321228.0|
|   BR0009|  DLR0108| Cad-M38|11379294|         2|DT00004| 31|   12|2017|      AC Cars Motors|        Lexus Motors|           Cad| 5689647.0|
|   BR0010|  DLR0088|  Mer-M8|11611234|         2|DT00005|  4|    9|2017|      AC Cars Motors|IFA (including Tr...|           Mer| 5805617.0|
|   BR0011|  DLR0002|  BMW-M2|19979446|         2|DT00005|  2|    1|2017|        Acura Motors|        Acura Motors|           BMW| 9989723.0|
|   BR0011|  DLR0069|Vol-M256|14181510|         3|DT00006|  9|    5|2017|        Acura Motors|          Geo Motors|           Vol| 4727170.0|
|   BR0012|  DLR0249|BMW-M249| 5358057|         1|DT00006|  6|    9|2017|        Acura Motors|        Acura Motors|           BMW| 5358057.0|
|   BR0013|  DLR0229|Hon-M219|16150431|         3|DT00007| 11|    5|2017|        Acura Motors|       Herald Motors|           Hon| 5383477.0|
|   BR0014|  DLR0209|Tat-M189|13389350|         2|DT00007| 13|    1|2017|        Acura Motors|      Zastava Motors|           Tat| 6694675.0|
|   BR0015|  DLR0189|Hyu-M159| 4891618|         2|DT00008| 17|    9|2017|        Acura Motors|      Sunbeam Motors|           Hyu| 2445809.0|
|   BR0017|  DLR0149| Lex-M99| 5059144|         2|DT00008| 25|    8|2017|        Acura Motors|        Panoz Motors|           Lex| 2529572.0|
|   BR0018|  DLR0129| Hon-M69|17369466|         2|DT00009| 29|    4|2017|        Acura Motors|          Mia Motors|           Hon| 8684733.0|
|   BR0019|  DLR0109| Cad-M39|26969532|         3|DT00010|  1|    1|2017|        Acura Motors|       Ligier Motors|           Cad| 8989844.0|
|   BR0020|  DLR0089|  Dod-M9| 4816794|         2|DT00011|  5|    9|2017|        Acura Motors|     Infiniti Motors|           Dod| 2408397.0|
|   BR0021|  DLR0070|Vol-M257| 7738896|         1|DT00011| 10|    5|2017|Aixam-Mega (inclu...|      Gilbern Motors|           Vol| 7738896.0|
|   BR0024|  DLR0210|Tat-M190|11038722|         3|DT00012| 14|    1|2017|Aixam-Mega (inclu...|          ZAZ Motors|           Tat| 3679574.0|
+---------+---------+--------+--------+----------+-------+---+-----+----+--------------------+--------------------+--------------+----------+
only showing top 20 rows

#Groupig on Year and branchname and aggregated the total units_sold and sorted on ascending on year and descending on Total_units 
                                                                                     
df1=df.groupBy('Year','BranchName').agg(sum('Units_Sold').alias('Total_Units')).sort('Year','Total_Units',ascending=[1,0])
df1.show()

+----+--------------------+-----------+
|Year|          BranchName|Total_Units|
+----+--------------------+-----------+
|2017|       Alpine Motors|         24|
|2017|      Bristol Motors|         23|
|2017|        Acura Motors|         23|
|2017|          BMW Motors|         23|
|2017| Aston Martin Motors|         23|
|2017|        Ariel Motors|         21|
|2017|     Daihatsu Motors|         21|
|2017|      Gilbern Motors|         21|
|2017|  Asia Motors Motors|         20|
|2017|          DAF Motors|         19|
|2017|       Anadol Motors|         19|
|2017|     DeLorean Motors|         19|
|2017|      AC Cars Motors|         19|
|2017|         Glas Motors|         18|
|2017|   Auto-Union Motors|         18|
|2017|   AMC, Eagle Motors|         18|
|2017|     Caterham Motors|         17|
|2017|       Datsun Motors|         17|
|2017|        Alvis Motors|         17|
|2017|Chevrolet India M...|         16|
+----+--------------------+-----------+
only showing top 20 rows

#Stored the data in silver layer

df.write.format('parquet')\
 .mode('append')\
 .option('path','/mnt/silver/kc12/processed')\
 .save()
