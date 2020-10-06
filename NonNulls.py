#import require libraries
from pyspark.sql.functions import col, lit
from pyspark.sql import Row

#create a practice df
tbl = sc.parallelize([
        Row(f_n='Al', lid='a12'),             
        Row(f_n='Pr', lid=None),
        Row(f_n=None, lid='b34'),
        Row(f_n='Bq', lid='d44'),
        Row(f_n='Ed', lid=None),
        Row(f_n='Dw', lid='c54'),
        Row(f_n='We', lid='e76'),
        Row(f_n='Rw', lid='514'),
        Row(f_n='Qe', lid='726'),
        Row(f_n=None, lid=None)
    ]).toDF()
tbl.show()

#get only non-null values from 2 columns
tbl.select('lid', 'f_n').filter(col('f_n').isNotNull() & col('lid').isNotNull()).show()

#integer check
tbl.select(
'lid', 'f_n',
  col('lid').cast('int').isNotNull().alias('Value')
).filter(col('Value') == 'true').show()


#register a temp table to do sql ops
tbl.createOrReplaceTempView("temp_tbl")
sql("select * from temp_tbl").show()

#create a df using createDataframe()
dta1 = [
  ('A', 1, 'ab12'),
  ('B', 2, 'cd23'),
  ('C', 3, 'de34'),
  ('D', 4, 'ef45'),
  ('E', 5, 'fg55'),
  ('F', 6, None),
  ('G', 7, 'gh56'),
  ('H', 8, None),
  ('I', 9, 'ij75'),
  ('J', 10, None)
]
tes = spark.createDataFrame(dta1, ['ID', 'Num', 'Alpha_num'])
tes.show()

 
