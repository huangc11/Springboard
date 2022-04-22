

from pyspark.sql import functions as f

from ut_base import ut_base as ut_base
from ut_spark import ut_spark
from ut_store import ut_store
from ut_log import ut_log
from ut_pipeline import ut_pipeline as ut_pipl

class ETL_Carrier:

    c_raw_file_name =ut_store.raw_file_carrier
    c_raw_file_format ='csv'
    c_cln_file_name = ut_store.cln_file_carrier
    c_cln_file_format ='parquet'
    c_proc_name ='ETL-Carrier'

    def __init__(self):
        pass

    @staticmethod
    def transform(df):
        return df

    @classmethod
    def exc_etl(cls):


        ut_log.log_proc_start(cls.c_proc_name)

        # Extract from raw files
        log_id = '{}_{}'.format(cls.c_proc_name, ut_base.curr_time_str())
        df1 = ut_pipl.read_raw(cls.c_proc_name, log_id, cls.c_raw_file_name)


        #clean and transformation
        df2 = cls.transform(df1)

        # Write clean file
        print(cls.c_cln_file_format)
        df2.show()
        print(cls.c_cln_file_name)
        res = ut_pipl.save_cleaned (cls.c_proc_name, log_id,df2, cls.c_cln_file_name, cls.c_cln_file_format)
        ut_log.log_proc_end(cls.c_proc_name)


if __name__ =='__main__':
    ETL_Carrier.exc_etl()






