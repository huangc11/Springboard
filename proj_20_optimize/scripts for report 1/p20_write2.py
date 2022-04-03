'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month

import os
from ut_spr import ut_spr as ut1


spark = SparkSession.builder.appName('Optimize I').getOrCreate()

base_path = os.getcwd()

project_path = ('/').join(base_path.split('/')[0:-3])

answers_input_path = os.path.join(project_path, 'data/answers')


#questions_input_path = os.path.join(project_path, 'output/questions-transformed')
questions_input_path = os.path.join(project_path, 'data/questions')

print('----- reading  questionsDF  -----')
with ut1.timer():
    questionsDF = spark.read.option('path', questions_input_path).load()



print('----- reading  answersDF  -----')
with ut1.timer():
    answersDF = spark.read.option('path', answers_input_path).load()


answersDF\
    .coalesce(1)\
    .write \
    .bucketBy(4, 'question_id') \
    .mode('overwrite')\
    .sortBy('question_id') \
    .option("path","data/tb_answer")\
    .saveAsTable('tb_answer', format='parquet')

questionsDF\
    .coalesce(1)\
    .write\
    .bucketBy(4, 'question_id') \
    .mode('overwrite')\
    .sortBy('question_id') \
    .option("path","data/tb_question")\
    .saveAsTable('tb_question', format='parquet')


