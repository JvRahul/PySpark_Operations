# Udf example

udfExampleDF = spark.range(5).toDF('num')

def power3(double_value):
  return double_value ** 3
  
  
#---- To use it in a dataframe, we register it ----
from pypsark.sql.functions import udf
power3udf = udf(power3)

# Now we can use it in the dataFrame code
from pyspark.sql.functions import col
udfExampleDF.select(power3udf(col('num'))).show()


#---- To use it in sparkSQL, register it ----
sqlContect.udf.register('power3sql', power3)
sqlcontext.sql('select power3sql(2)')
# OR
udfExampleDF.selectExpr('power3sql(num)').show()
