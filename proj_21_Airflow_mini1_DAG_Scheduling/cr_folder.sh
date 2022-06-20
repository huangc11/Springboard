#!/usr/bin/bash


dt="$(date +"%Y%m%d")"

echo  "/tmp/data/$dt"
echo $"/tmp/data/$dt"

rmdir "/tmp/data/$dt"
#rmdir   "/opt/airflow/logs/$dt"
#mkdir "/opt/airflow/logs/$dt"

mkdir "/tmp/data/$dt"


