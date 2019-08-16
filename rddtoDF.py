rdd1 = sc.textFile('dbfs:/FileStore/tables/2010_12_01-ec65d.csv')rdd2_split = rdd1.map(lambda l: l.split(","))

#type(rdd1)
rdd1.take(10)

rdd2_header = rdd2_split.first()
rdd2_noheader = rdd2_split.filter(lambda line: line != rdd2_header)

rdd2_tuple = rdd2_noheader.map(lambda p: (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]))

#rdd2_noheader.map(lambda l : l.int(l[0][0], l[0][1], l[0][3], l[0][4], l[0][5], l[0][6]))
rdd2_tuple.take(10)

#provide schema with column headers and data types...
schemaString = "InvoiceNo StockCode Description Quantity InvoiceDate UnitPrice CustomerID Country"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
rdd2_tupledf = spark.createDataFrame(rdd2_tuple, schema)

#rdd2_tupledf.printSchema()
#rdd2_tupledf.show(10)

cols = ['InvoiceNo', 'Quantity', 'CustomerID']
from pyspark.sql.functions import col
for col_name in cols:
    rdd2_tupledf = rdd2_tupledf.withColumn(col_name, col(col_name).cast('int'))

#casted_rdd2tupledf = rdd2_tupledf.withColumn("year")
#df.withColumn("Timestamp", (col("Id").cast("timestamp")))

rdd2_tupledf = rdd2_tupledf.withColumn("Timestamp", (col("InvoiceDate").cast("timestamp")))
rdd2_tupledf = rdd2_tupledf.drop('InvoiceDate').withColumnRenamed('timestamp', 'InvoiceDate')
rdd2_tupledf = rdd2_tupledf.withColumn("flloat", (col('UnitPrice').cast('float'))).drop('UnitPrice').withColumnRenamed('flloat', 'UnitPrice')

rdd2_tupledf.printSchema()
rdd2_tupledf.show()
