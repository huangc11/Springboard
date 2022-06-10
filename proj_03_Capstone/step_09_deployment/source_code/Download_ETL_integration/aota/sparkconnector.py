from pyspark.sql import SparkSession
from pyspark import SparkConf

class sparkConnector:
   c_spark_sql_shuffle_partitions=8

   def __init__(self):
       pass

   def get_spark(self,p_conf=None):
       '''Create and return a spark ssion'''
       if p_conf==None:
           conf = SparkConf()
       else:
           conf=p_conf

       try:
           spark = SparkSession. \
               builder. \
               appName("Aggregation"). \
               config(conf=conf).\
               getOrCreate()

           spark.conf.set("spark.sql.shuffle.partitions", sparkConnector.c_spark_sql_shuffle_partitions)

           return spark

       except Exception as e:
           print(e)
           return None


if __name__ =="__main__":
    sparkcon = sparkConnector()
    spark = sparkcon.get_spark()
    print(spark)


    '''
    df=spark.read.csv('c:/demo/capstone/raw/plane.csv')
    df.show(3)

    df.write.csv('c:/demo/capstone/raw/plane5.csv')
    '''

    df = spark.read.csv("s3a://chu2021-0801-xetra-1234/c14/csv/dog_food.csv")
    df.show(3)
    df = spark.read.csv("s3a://capstone-aota/raw/plane.csv")
    df.show(3)

    df.write.csv("s3a://capstone-aota/raw/plane3")
    df.show(3)


