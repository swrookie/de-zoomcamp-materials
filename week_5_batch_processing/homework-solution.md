## Week 5 Homework Solution


`
./download_data.sh

from pyspark.sql import SparkSession, types

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

schema = types.StructType([
    types.StructField("dispatching_base_num", types.StringType(), True),
    types.StructField("pickup_datetime", types.TimestampType(), True),
    types.StructField("dropoff_datetime",types.TimestampType(), True),
    types.StructField("PULocationID", types.IntegerType(), True),
    types.StructField("DOLocationID", types.IntegerType(), True),
    types.StructField("SR_Flag", types.StringType(), True),
    types.StructField("Affiliated_base_number", types.StringType(), True),

])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('data/raw/fhvhv/2021/06/fhvhv_tripdata_2021_06.csv.gz')
`

## Question 1

`pyspark`
`spark.version`

Output: '3.3.2'

Answer: `3.3.2`

## Question 2

`df.repartition(12).write.parquet('fhvhv')`
`ls -l | grep part-0000 | gawk '{sum += $5; n++;} END {print sum/n;}'`

Output: 2.59112e+07

Answer: 24MB

## Question 3

`df.filter("to_date(pickup_datetime) == '2021-06-15'").count()`

Output: 452470

Answer: 452,470

## Question 4

`df.selectExpr("max(unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 3600").show()`

Output: 66.8788888888889

Answer: 66.87 Hours

## Question 5

`pyspark`

Output: 

`
Spark context Web UI available at http://ipaddr:4040
`

Answer: 4040

## Question 6

`
df2 = spark.sql("select * from zone_lookup")
df.groupBy("PULocationId") \
    .count() \
    .sort("count", ascending=False) \
    .limit(1) \
    .join(df2, df.PULocationID == df2.LocationID).selectExpr("Zone").show() \
`

Output: 

`
+-------------------+
|               Zone|
+-------------------+
|Crown Heights North|
+-------------------+
`

Answer: Crown Heights North