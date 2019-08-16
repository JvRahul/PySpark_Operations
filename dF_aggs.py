aggdf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/tables/retail_data_all-db128.txt").coalesce(5)

aggdf.cache()

aggdf.createOrReplaceTempView("aggtable")
#aggdf.show(10)

#from pyspark.sql.functions import distinct
aggdf.select("InvoiceNo").distinct().count()

from pyspark.sql.functions import countDistinct, approx_count_distinct, col
aggdf.select(countDistinct(col("StockCode"))).show()

from pyspark.sql.functions import first, last, desc
aggdf.orderBy(desc("UnitPrice")).show()

from pyspark.sql.functions import sum, count, avg, expr
aggdf.select(
count("Quantity").alias("total_transactions"),
sum("Quantity").alias("total_purchases"),
avg("Quantity").alias("avg_purchases"),
expr("mean(Quantity)").alias("mean_purchases"))\
.selectExpr(
"total_purchases/total_transactions",
"avg_purchases",
"mean_purchases").show()

from pyspark.sql.functions import collect_set, collect_list
aggdf.agg(collect_set("Description"), collect_list("Description")).show()

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc, round
unitdf = aggdf.select(round("UnitPrice", 2))

from pyspark.sql.functions import desc, expr, round
utestdf = aggdf.orderBy(expr("UnitPrice desc")).show()
aggdf.groupBy("InvoiceNo").count().show()

from pyspark.sql.functions import col, to_date, to_timestamp
df1 = aggdf.withColumn("date", to_date("InvoiceDate", "MM/dd/yyyy H:mm" ))

df1.withColumn("dateTimestamp", to_timestamp("date", 'dd/MM/YY')).show()
