In this project, we will read from a vehicle accident/repair data file and output the counts of  accidents group by combination of verhicle make and year. 

### Implementation


#####  Overview

We will use map-reduce pragramming model to solving this problem. Programming language is python. 


##### map-reduce process

There will be two map-reduce process

1. **Map-reduce 1**:  to filter out accidents with vehicle make and year populated. It will read source data file and output  key pair as (make+year,1)

2. **Map-reduce 2**:  to output the count number of accident occurrences for vehicle make and year. 
It will read the output of map-reduce 1 and output result, (make+year, count)

##### python code
There are 4 python scripts, which will server as mappers, reducers of the two map-reduce process

1. **mapper1.py**:  reads the input data,  extract vin_number  and   output (key, value) pair as key--vin_number, value --incident type, make, and year.
2. **reducer1.py**:   reads the mapper output, pick accident records, populate year and make and write them to output destination
3. **mapper2.py**: reads the previous reducer output and output (key, value) pair as (combination of make and year, 1)
4. **reducer2.py**: reads the mapper output . Within the group of make and year, sum all the 1s to produce the total count as the output.


##### Running the map-reduce 

We will use HadoopStreaming for helping us passing data between our Map and Reduce code via STDIN (standard input) and STDOUT (standard output).
We will simply use Python’s sys.stdin to read input data and print our own output to sys.stdout. HadoopStreaming will take care of everything else!


### Run

####  Prerequisites

1. You should have an Hadoop cluster up and running. If not, You can follow the instructions from the following  video to set up Hortonworks Hadoop Sandbox to run the code. 

https://www.youtube.com/watch?v=735yx2Eak48

This Video includes:
- Install Virtual Box 
- Install Cloudera HDP

2. After Hortonworks Hadoop Sandbox  installed and up, create linux account 'hduser'

   - login as 'root' (vising localhost:4200)
   - run:
    useradd -m -r hduser


#### Preparation

1. Upload data file to HDFS
  - visit  'localhost:8080' and login as 'admin'
  - Navigate to 'file view' and craete ollowing new folder
     user/hduser
  - upload souce data ile 'data.csv'
     
2.  For running  map reduce locally
Login to sandbox as 'hduser', create folder /home/hduser/mapreduce_autoinc and naviate into it
1) Create following python scripts by copying and paste (note: uploading the scripts that were created at Windows may not be working):
autoinc_mapper1.py
autoinc_reducer1.py, 
autoinc_mapper2.py,
autoinc_reducer2.py

2) Upload  file 'data_carinc.csv' to this folder


3. At /home/hduser,  create following shell scripts:
  - **map_reduce1.sh**, which includes following content (it will read  hdfs file: /user/hduser/data.csv and write to intermidiate files on hdfs under /user/hduser/output/all_inc)
  
   hadoop jar /usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming.jar -file mapper1.py -mapper mapper1.py -file reducer1.py -reducer reducer1.py -input /user/hduser/data.csv -output output/all_inc
   
  - **map_reduce2.sh**, which includes following content (it will read from hdfs at /user/hduser/output/all_inc and write  output to hdfs at /user/hduser/output/make_year_count)
  
  hadoop jar /usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming.jar -file mapper2.py -mapper mapper2.py -file reducer2.py -reducer reducer2.py -input  output/all_inc -output output/make_year_count


#### Test the code locally

Login to sandbox as 'hduser', navigate to folder /home/hduser/mapreduce_autoinc.Type

cat data.csv|./mapper1.py|sort|./reducer1.py|./mapper2.py|sort|./reducer2.py
 
####  Running the map-reduce processes

- Run following command to execute the 1st map-reduce process:

/.map_reduce1.sh



- Run following command to execute the 2nd  map-reduce process:

/.map_reduce2.sh


- Run following command to check result 

hadoop -fs -cat output/make_year_count/part-00000


#### Output
We run the process and the screen shots are here:
https://github.com/huangc11/Springboard/tree/main/proj_17_hadoop_mini/ouptut
