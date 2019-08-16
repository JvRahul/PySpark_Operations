from pyspark import *

flightdata2015 = spark.read.option('inferSchema','true').option('header','true').csv('dbfs:/FileStore/tables/flight_data.txt')
flightdata2015.take(3)
flightdata2015.sort('count').explain()

spark.conf.set('spark.sql.shuffle.partitions','5')

flightdata2015.sort('count').take(2)

flightdata2015.createOrReplaceTempView('flightsd2015')

sqlway = spark.sql('select DEST_COUNTRY_NAME, count(1) from flightsd2015 group by DEST_COUNTRY_NAME')

sqlway.explain()

dataframeway = flightdata2015.groupBy('DEST_COUNTRY_NAME').count()

from pyspark.sql.functions import max

flightdata2015.select(max('count')).take(1)

maxsql = spark.sql('select DEST_COUNTRY_NAME, sum(count) as destination_total FROM flightsd2015 group by DEST_COUNTRY_NAME order by sum(count) desc limit 5')
maxsql.show()
