df = spark.read.json('/user/path.json')
df.show()  #it works only on a single line json otherwise it would throw a corrupt_record error

#To handle multiple lines json file,

#use 
df = spark.read.json('/user/path.json', multiLine = True)

#If we got a column (colname) of array type in the dataframe, split it using explode function as below,

from pyspark.sql.functions import explode
df_flat = df.withColumn('colname_flat', explode('colname'))

df_flat.select('col1', 'colname_flat.*').show()
