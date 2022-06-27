
from pathlib import Path
from logparser import logParser


aflog_win= "c:/ubtemp/airflowlog"
aflog_ubt = '/home/chu/airflow_docker/logs/dag_id=dag2_marketvol' \
#dag_id=dagxx_marketvol'

logdir =aflog_ubt
logdir ="c:/ubtemp/airflowlog/logs"

file_list = Path(logdir).rglob('*.log')

flist=list(file_list)
print(list)

print('Here are all the errors:')
i=0
for fp in flist:
   p = logParser(fp)
   (count, errors)  =p.parse()
   if count!=0:
       i=i+count
       print('---------------log name: {}-----------------------'.format(fp))
       for e in errors:
           print(e)


print('Total number of errors: {}'.format(i))