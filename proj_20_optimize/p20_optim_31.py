
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month

import os
from ut_spr import ut_spr as ut1

def get_abs_path(sub_folder):
    base_path = os.getcwd()
    project_path = ('/').join(base_path.split('/')[0:-3])
    abs_path = os.path.join(project_path, sub_folder)
    return abs_path



spark = SparkSession.builder.appName('Optimize I')\
    .master("local[4]") \
    .enableHiveSupport()\
    .getOrCreate()

spark.conf.set ("spark.sql.shuffle.partitions",4)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

print('----- reading  answersDF  -----')
with ut1.timer():
    #answersDF = spark.read.option('path', answers_input_path).load()
    df_ans = spark.table('answer_tb')


print('----- reading  questionsDF  -----')
with ut1.timer():
    #questionsDF = spark.read.option('path', questions_input_path).load()
   df_que = spark.read.table('question_tb1')


df_que=spark.read.table('question_tb1')
df_ans =spark.table('answer_tb')

df_ansmon = df_ans.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))
resultDF = df_que.join(df_ansmon, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')
resultDF2 = resultDF.orderBy('question_id', 'month')

print('----- resultDF2.show -----')
with ut1.timer():
    resultDF2.show()
    #print(resultDF2.count())
