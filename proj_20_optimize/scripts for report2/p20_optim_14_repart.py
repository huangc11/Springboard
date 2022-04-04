
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month

import os

from ut_spr import ut_spr as ut1


spark = SparkSession.builder.appName('Optimize I').getOrCreate()

# --------  set config
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
spark.conf.set ("spark.sql.shuffle.partitions",4)

base_path = os.getcwd()

project_path = ('/').join(base_path.split('/')[0:-3])

answers_input_path = os.path.join(project_path, 'data/answers')

questions_input_path = os.path.join(project_path, 'data/questions')

answersDF = spark.read.option('path', answers_input_path).load()

questionsDF = spark.read.option('path', questions_input_path).load()

'''
Answers aggregation
Here we : get number of answers per question per month
'''





#questionsDF.orderBy('question_id').repartition(4, 'question_id')
#answersDF.orderBy('question_id').repartition(4, 'question_id')

time_start = ut1.curr_time()


answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))
answers_month.orderBy('question_id').repartition(4, 'question_id')


resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month',
                                                                      'cnt')
#resultDF.orderBy('question_id', 'month').explain()
resultDF.orderBy('question_id', 'month').show()


print(ut1.curr_time()-time_start)

