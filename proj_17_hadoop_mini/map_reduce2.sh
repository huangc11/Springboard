hadoop jar /usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming.jar -file mapper2.py -mapper mapper2.py -file reducer2.py -reducer reducer2.py -input  output/all_inc -output output/make_year_count
