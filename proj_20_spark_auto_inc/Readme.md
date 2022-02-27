In this project, we will read data from a vehicle accident/repair data file and output the counts of  accidents group by combination of verhicle make and year. 

### Implementation



- We use Spark RDD  to solve this problem. Programming language is python. 


- A single python script, 'autoinc_spark.py', is provided.  It will

  1. Read data from 'data_postsale.csv' and crate the first raw RDDs
  
  2. Apply transformations on the raw RDD by using  following functions and create a series of new RDD 
  
| Transformation| Function | Description |
| ----------- | ----------- |-----------|
|map()| extract_vin_key_value()|Extract information from raw data and create key/value pair, (vin, {make,year,inc_type})  |
|groupByKey(),flatMap()|populate_make: output()|Filter out accident records with make and year populated |
|map()|extract_make_key_value()| Convert each make and year combination to a (key, value) pair, (make +year, 1)|
|reduceByKey| lambda x, y: x + y|Aggregate on the key and count the number of records in total per key|
   
  3. Finally, it will save the result, which is stored in the final RDD, to folder 'output/spark_output'

  


### Run

####  Prerequisites

1. You should have an Hadoop cluster up and running. If not, You can follow the instructions from the following  video to set up Hortonworks Hadoop Sandbox to run the code. 

https://www.youtube.com/watch?v=735yx2Eak48

This Video includes:
- Install Virtual Box 
- Install Cloudera HDP

2. After Hortonworks Hadoop Sandbox  installed and up running, create linux account 'hduser'

   - login as 'root' (vising localhost:4200 using user 'root')
   - run:
    useradd -m -r hduser


#### Preparation

1. Upload data file to HDFS
  1) Visit  'localhost:8080' and login as 'admin'
  2) Navigate to 'file view' and craete ollowing new folder if not existing
     user/hduser
  3) Upload source data ile 'data_postsale.csv'
     
2. Login to HDP sandbox as 'hduser', navigate to /home/hduser, create following python script by edit 'vi':
 autoinc_spark.py
 You may also upload the script to HDFS: /user/hduser and copy from there (using 'hadoop fs -get' command) 

 
####  Running the map-reduce processes

- Login to HDP sandbox as 'hduser', navigate to /home/hduser, run following command to execute the 1st map-reduce process:

 /usr/hdp/current/spark-client/bin/spark-submit autoinc_spark.py

 
- Run following command to check result 

hadoop -fs -cat /user/hduser/output/spark_output/part-00000


#### Output
We ran the process and the screen shots are here:
https://github.com/huangc11/Springboard/tree/main/proj_20_spark_auto_inc/output