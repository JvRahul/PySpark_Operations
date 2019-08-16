l = range(0,101)
lrdd = sc.parallelize(l)

#spark.createDataFrame(lrdd).collect()
from pyspark.sql import Row
row_rdd = lrdd.map(lambda x: Row(x))
ldf = sqlContext.createDataFrame(row_rdd,['numbers'])

from pyspark.sql.functions import expr
ncolumn = expr(
    """IF((numbers % 2 = 0), 'Buzz', 'Fizz')"""
)
#ncolumn.explain()

lldf = ldf.withColumn('FizzBuzz', ncolumn)
lldf.printSchema()
lldf.show()
