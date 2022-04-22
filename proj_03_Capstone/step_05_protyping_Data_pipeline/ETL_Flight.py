

from pyspark.sql import functions as f

from ut_base import ut_base as ut_base
from ut_spark import ut_spark
from ut_store import ut_store
from ut_log import ut_log
#from ut_clean import ut_clean
from ut_pipeline import ut_pipeline as ut_pipl

class ETL_Flight:
    c_raw_file_name ='2008.csv.bz2'
    #c_raw_file_name = 'flights.csv'
    c_raw_file_format ='csv'
    c_cln_file_name = ut_store.cln_file_flight
    c_cln_file_format ='parquet'
    c_proc_name ='ETL-Flight'

    ft_fields = ut_store.flight_schema_fields




    def __init__(self):
        pass


    @classmethod
    def read_raw(cls, log_id):
        ut_log.log_info("Read data from raw file(s) started...")

        ft_schema = ut_spark.list_to_schema(cls.ft_fields)
        rf_path= ut_store.folder_raw + cls.c_raw_file_name
        print(rf_path)
        df =  ut_spark.df_read_from_csv2(rf_path, ft_schema)
        if df==None:
            ut_log.log_error('{}: read raw file "{}" failed.'.format(cls.c_proc_name, rf_path))
            exit(-1)
        else:
            ut_pipl.log_proc_metric(log_id, cls.c_proc_name, df, 'read', rf_path)
            return df





    @staticmethod
    def gen_user_key(year, month, day, flightnum):
        sep = '_'
        return str(year) + sep + str(month).rjust(2, '0') + sep + str(day).rjust(2, '0') + sep + flightnum

    @classmethod
    def transform(cls,df1):
        ''' transformation   '''
        # 1. remove cancelled records
        ### 2. remove column 'tax_in', 'tax_out'
        
        df2=df1.filter(f.col("Cancelled")!=1).filter(f.col("Diverted")!=1)
        df2=ut_spark.add_mono_incr_id(df2, "row_id1")


        return df2


    @classmethod
    def exc_etl(cls):
        ut_log.log_proc_start(cls.c_proc_name)

        # Extract from raw files
        log_id = '{}_{}'.format(cls.c_proc_name, ut_base.curr_time_str())
        df1 = cls.read_raw(log_id)
        print(df1.count())

        print('hello-----------')

        #clean and transformation
        df2 = cls.transform(df1)


        #Write clean data to  file
        res = ut_pipl.save_cleaned(cls.c_proc_name, log_id, df2, cls.c_cln_file_name, cls.c_cln_file_format)
        ut_log.log_proc_end(cls.c_proc_name)


if __name__ =='__main__':
   ETL_Flight.exc_etl()

 #  log_id = '{}_{}'.format(ETL_Flight.c_proc_name, ut_base.curr_time_str())
  # df1 = ETL_Flight.read_raw(log_id)
   #df2 = df1.filter(f.col("Cancelled") != 1)








