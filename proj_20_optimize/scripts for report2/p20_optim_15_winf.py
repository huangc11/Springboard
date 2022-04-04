
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month

from pyspark.sql.window import Window
from pyspark.sql.functions import  col, row_number ,count

import os

from ut_spr import ut_spr as ut1


spark = SparkSession.builder.appName('Optimize I').getOrCreate()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
spark.conf.set ("spark.sql.shuffle.partitions",4)

base_path = os.getcwd()

project_path = ('/').join(base_path.split('/')[0:-3]) 

answers_input_path = os.path.join(project_path, 'data/answers')

questions_input_path = os.path.join(project_path, 'data/questions')

answersDF = spark.read.option('path', answers_input_path).load()

questionsDF = spark.read.option('path', questions_input_path).load()




time_start = ut1.curr_time()

from pyspark.sql.window import Window

win = Window.partitionBy('question_id', 'month')

winAnswersDF = answersDF \
    .withColumn('month', month('creation_date')) \
    .withColumnRenamed('creation_date', 'answer_creation_date')


winResultDF = questionsDF \
    .join(winAnswersDF, 'question_id') \
    .withColumn('cnt', count('*').over(win)) \
    .select('question_id', 'creation_date', 'title', 'month', 'cnt') \
    .distinct() \
    .orderBy('question_id', 'month')

# .distinct() \

#winResultDF.explain()
winResultDF.show()

print(ut1.curr_time()-time_start)
