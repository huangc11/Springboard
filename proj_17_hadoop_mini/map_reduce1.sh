hadoop jar /usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming.jar -file mapper1.py -mapper mapper1.py -file reducer1.py -reducer reducer1.py -input /user/hduser/data.csv -output output/all_inc
