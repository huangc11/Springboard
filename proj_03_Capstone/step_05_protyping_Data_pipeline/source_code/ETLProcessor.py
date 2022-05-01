

from pyspark.sql import functions as f

from ut_base import ut_base as ut_base
from ut_spark import ut_spark
from ut_store import ut_store
from ut_log import ut_log

class CustomError(Exception):
    pass

class ETLProcessor:
    '''A class of grouped methods to run ETL (extract, transform/clean, load) process on
    a given dataset:
      1. Will read raw csv files from 'raw' folder
      2. Will write parquet files to 'processed' folder'''

    c_raw_file_format ='csv'
    c_cln_file_format ='parquet'
    c_fail = -1
    c_succ = 1



    def __init__(self, entity_name:str, raw_file_name:str,  cln_file_name:str):
        self.entity_name = entity_name
        self.proc_name = 'ETL-' + self.entity_name
        self.log_id = ETLProcessor.gen_log_id(self)

        self.raw_file_name = raw_file_name
        self.raw_file_path = ut_store.folder_raw + self.raw_file_name
        self.raw_file_format = ETLProcessor.c_raw_file_format

        self.file_schema = self.define_schema()
        print('schema:{}'.format(self.file_schema))

        self.cln_file_name = cln_file_name
        self.cln_file_path = ut_store.folder_cln + self.cln_file_name
        self.cln_file_format = ETLProcessor.c_cln_file_format


    def gen_log_id(self):
        ''' Create a uniue id for logging, based on process name and current timestamp'''
        return  '{}_{}'.format(self.proc_name, ut_base.curr_time_str())

    def define_schema(self):
        return None

    def Extract(self):
        "To extract data from raw files "

        ut_log.log_info("Read data from raw files started...")


        df =  ut_spark.df_read_from_csv(self.raw_file_path, self.file_schema)

        if df==None:
            ut_log.log_error('{}: read raw file "{}" failed.'.format(self.proc_name, self.raw_file_name))
            return df
        else:
            df.show(3)
            ut_log.log_proc_metric(self.log_id, self.proc_name, df, 'read',self.raw_file_path,self.raw_file_format)
            return df

    def Transform(self, df1):
       return df1


    def write_to_file(self,df):
        '''Write the dataframe data to destination '''
        try:
            print('write to file, generic...')
            ut_spark.df_write_to_file(df,  self.cln_file_path, self.cln_file_format)
        except Exception as e:
            ut_log.log_exeption(e)

    def Save(self, df):

        try:

            ut_log.log_info("Write cleaned data to file started...")

            res = self.write_to_file(df)

            if res==ut_spark.c_fail:
                ut_log.log_error('Write cleaned data to file for {} failed. log_id:{}.'.format(self.proc_name, self.log_id))
                return  ETLProcessor.c_fail
            else:
                ut_log.log_info('Write cleaned data succeeded.')
                ut_log.log_proc_metric(self.log_id, self.proc_name, df, 'write',self.cln_file_path,self.cln_file_format)
                return ETLProcessor.c_succ

        except Exception as e:
            ut_log.log_exeption(e)
            ut_log.log_info('Write cleaned data failed -- exception raised.')
            return ETLProcessor.c_fail



    def ETL_Run(self):

        ut_log.log_proc_start(self.proc_name)

        # Read(Extract) data from raw file
        df1 = self.Extract()

        if df1==None:
            ut_log.log_proc_abort(self.proc_name, 'read from raw file failure.')
            return ETLProcessor.c_fail




        #Data cleansing and transformation
        try:

             df2 = self.Transform(df1)
             if df2 == None:
               raise CustomError

        except Exception as e:
            ut_log.log_exeption(e)
            ut_log.log_proc_abort(self.proc_name, 'Transformion failure.')
            return  ETLProcessor.c_fail

        #Save(Load) processed data to "proessed"
        try:
            df2.show(4)
            res = self.Save(df2)

            if res==ETLProcessor.c_fail:
                raise CustomError

        except Exception as e:
            ut_log.log_exeption(e)
            ut_log.log_proc_abort(self.proc_name, ' saving to processed layer failure.')
            return  ETLProcessor.c_fail

        ut_log.log_proc_end(self.proc_name)
        return ETLProcessor.c_succ











