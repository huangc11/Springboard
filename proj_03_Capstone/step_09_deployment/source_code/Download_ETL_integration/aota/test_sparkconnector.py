from sparkconnector import sparkConnector

sparkcon = sparkConnector()
spark = sparkcon.get_spark()
print(spark)

conf_out = spark.sparkContext.getConf()
print(conf_out.toDebugString())
df = spark.read.csv("s3a://chu2021-0801-xetra-1234/c14/csv/dog_food.csv")
#df.show(3)


df.write.csv("s3a://capstone-aota/raw/plane34")
c