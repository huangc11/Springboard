from pyspark.sql import SparkSession
from pyspark import SparkConf

try:
           spark = SparkSession. \
               builder. \
               appName("Aggregation"). \
               master("local[4]"). \
               getOrCreate()
except:
     print("fail")

print(spark)