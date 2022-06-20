from datetime import date

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import yfinance as yf


default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2022,6,18),
    'retries':2,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id='dag_marketvol',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval='0 18 * * *',
)



# Define the task
t0=BashOperator(
        task_id='make_dir_1',
        bash_command="bash /opt/airflow/dags/cr_folder.sh ",
        dag=dag)





import datetime as dt
def download_stock(symbol):

    dt_str= dt.date.today().strftime('%Y%m%d')
    c_fileprex = "/tmp/data/"+dt_str+'/'
    start_date = dt.date.today() - dt.timedelta(days=1)
    end_date = start_date + dt.timedelta(days=1)
    
    df = yf.download(symbol, start=start_date, end=end_date, interval='1m')

    #c_fileprex="/opt/airflow/logs/"
    filepath = "{}{}_{}.csv".format(c_fileprex, symbol,dt_str)
    print(filepath)
    df.to_csv(filepath, header=False)





t1 = PythonOperator(
   task_id='download_data_aapl',
   python_callable=download_stock,
   op_kwargs={"symbol":"AAPL"},
   dag=dag)  



t2 = PythonOperator(
   task_id='download_data_tsla',
   python_callable=download_stock,
   op_kwargs={"symbol":"TSLA"},
   dag=dag)


t3=BashOperator(
        task_id='mv_file1',
        bash_command="bash /opt/airflow/dags/mv_file1.sh ",
        dag=dag)


t4=BashOperator(
        task_id='mv_file2',
        bash_command="bash /opt/airflow/dags/mv_file2.sh ",
        dag=dag)



import pandas as pd
import pandas
import datetime as dt

def read_stock(file_folder, symbol):
    dt_str=dt.date.today().strftime('%Y%m%d')
    #file_path = file_folder+'/'AAPL_'+dt_str+'.csv'
    file_path = '{}/{}_{}.csv'.format(file_folder, symbol, dt_str)
    print( 'File to be read:' + file_path )
    try:
        df1 = pandas.read_csv(file_path, header=None, names=['date_time', 'open', 'high',  'low', 'close', 'adj_close', 'volume'])
        df1['date']=df1['date_time'].str.slice(0, 10)
        print('Read data file succeeded')
        
        
        return df1
    except Exception as e:
        print('Read data file failed')
        #create an empty dataframe
        df = pd.DataFrame()
        return df
    

def get_stock_avg(file_folder):
    df1=read_stock(file_folder,'AAPL')
    df2=read_stock(file_folder,'TSLA')

    
    if not(df1.empty):
        print('---------avrage of AAPL open -------------')
        print(df1.groupby('date')['open'].mean())
        print('---------count of AAPL-------------')
        print(df1.groupby('date')['open'].count())
        
            

    if not(df2.empty):
        print('---------avrage of TSLA open -------------')
        print(df2.groupby('date')['open'].mean())
        print('---------count of TSLA-------------')       
        print(df2.groupby('date')['open'].count())
 


t5 = PythonOperator(
   task_id='query_count_pd',
   python_callable=get_stock_avg,
   op_kwargs={"file_folder":"/tmp/query"},
   dag=dag)  


t0>>t1>>t3
t0>>t2>>t4


t3>>t5
t4>>t5
