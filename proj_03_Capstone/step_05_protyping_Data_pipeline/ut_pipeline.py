
from pyspark.sql import functions as f
from ut_base import ut_base as ut_base
from ut_spark import ut_spark
from ut_store import ut_store
from ut_log import ut_log



class ut_pipeline:

    c_fail = -1
    c_succ = 1

    def __init__(self):
        pass

    @staticmethod
    def log_proc_metric(log_id, proc_name, df, rw_type, file_path, file_format=None):
        cnt_col = len(df.columns)
        cnt_row = df.count()
        dt_str = ut_base.curr_time().strftime(ut_store.metric_time_fmt)
        metric_line = "{},{},{},{},{},{},{}".format(proc_name, dt_str, rw_type, file_path, cnt_col, cnt_row,log_id)
        ut_log.log_info(metric_line)
        ut_log.log_metric(metric_line)

    @staticmethod
    def read_raw(proc_name, log_id, raw_file_name):
        ut_log.log_info("Read data from raw files started...")
        rf_path= ut_store.folder_raw + raw_file_name
        df =  ut_spark.df_read_from_csv1(rf_path)

        if df==None:
            ut_log.log_error('{}: read raw file "{}" failed.'.format(proc_name, raw_file_name))
            return None
        else:
            ut_pipeline.log_proc_metric(log_id, proc_name, df, 'read', rf_path)
            return df

    @staticmethod
    def save_cleaned(proc_name, log_id, df, cln_file_name, cln_file_format):

        ut_log.log_info("Write cleaned data to file started...")
        cln_file_path = ut_store.folder_cln + cln_file_name
        print(cln_file_format)
        res = ut_spark.df_write_to_file(df, cln_file_path, cln_file_format)

        if res==ut_spark.c_fail:
            ut_log.log_error('Write cleaned data to file for {} failed. log_id:{}.'.format(proc_name, log_id))
            res_sav= ut_pipeline.c_fail
        else:
            ut_pipeline.log_proc_metric(log_id, proc_name, df, 'write',cln_file_path, cln_file_format)
            res_sav=ut_pipeline.c_succ

        return res_sav





if __name__=='__main__':
    ut_pipeline.log_proc_metric('333', 'etl2',None, 'write', 'c:\temp')