
from pyspark.sql import SparkSession
from ut_spark import ut_spark

spark =SparkSession.\
        builder. \
        appName("Aggregation"). \
        master("local[3]").\
        getOrCreate()


ut_spark.print_conf(spark)



#df = spark.read.csv("s3a://chu2021-0801-xetra-1234/c14/csv/dog_food.csv")
#df.show(3)

print("reading 1997...")
df = spark.read.csv("s3a://capstone-aotaraw/1997.csv.bz2 ")
df.show(2)


