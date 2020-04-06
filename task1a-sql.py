import sys
from csv import reader
from pyspark import SparkContext
from pyspark.sql import SparkSession


if __name__ == '__main__':
	sc = SparkContext()
	spark = SparkSession(sc)
	df = spark.read.format('csv').options(header='true', inferSchema='true').load(sys.argv[1])
	df1 = spark.read.format('csv').options(header='true', inferSchema='true').load(sys.argv[2])
	joined = df.join(df1, on=['medallion', 'hack_license', 'vendor_id', 'pickup_datetime'], how='inner')
	result = joined.orderBy('medallion','hack_license','pickup_datetime',ascending=True)
	result = result.withColumn("pickup_datetime", result["pickup_datetime"].cast("string"))
	result = result.withColumn("dropoff_datetime", result["dropoff_datetime"].cast("string"))
	result.write.options(header='false').save('task1a-sql.out',format="csv")