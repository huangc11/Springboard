from pyspark.sql import functions as f

from pyspark.sql.functions  import col

from ut_base import ut_base as ut_base
from ut_spark import ut_spark
from ut_store import ut_store
from ut_log import ut_log
from ut_pipeline import ut_pipeline as ut_pipl


class intgr_stg_table:
    '''A module to create dimension tables'''

    c_file_stg_flight ='stg_flight_detail'
    #'C:\demo\capstone\integrated\stg_flight_detail'
    c_filep_stg_flight = ut_store.folder_intgr +  c_file_stg_flight

    __df_stg_flight=None

    def __init__(self):
        pass


    @classmethod
    def gen_date_key(cls, year, month, day):
        sep = ''
        return str(year) + sep + str(month).rjust(2, '0') + sep + str(day).rjust(2, '0')

    @classmethod
    def gen_orig_dest_key(cls, origin,dest):
        sep = '_'
        return origin + sep + dest

    @classmethod
    def cr_stg_flight(cls):

        ut_log.log_proc_start('create-stg_flight')

        udf_gen_date_key = f.udf(cls.gen_date_key, returnType=f.StringType())
        udf_gen_orig_dest_key = f.udf(cls.gen_orig_dest_key, returnType=f.StringType())

        df_fl = ut_spark.df_read_from_parquet(ut_store.folder_cln + ut_store.cln_file_flight)
        log_id = ut_base.curr_time_fmt()
        ut_pipl.log_proc_metric(log_id, 'Intgr-Create-stg_flight', df_fl, 'read',
                                ut_store.folder_cln + ut_store.cln_file_flight)

        #Adding date_key for buildinng dim_date later
        df_fl1 = df_fl.withColumn("date_key", udf_gen_date_key("Year", "Month", "DayofMonth"))

        # Adding orig_dest_key for buildinng dim_orign_dest
        df_fl2 = df_fl1.withColumn("orig_dest_key", udf_gen_orig_dest_key("Dest", "Origin"))

        #df_fl2.select("date_key", "orig_dest_key").showw()

        df_fl3 = ut_spark.add_mono_incr_id(df_fl2, "seq_id")

        wf_path= cls.c_filep_stg_flight
        ut_spark.df_write_to_file(df_fl3,  wf_path )
        log_id = ut_base.curr_time_fmt()
        ut_pipl.log_proc_metric(log_id, 'Intgr-Create-stg_flight', df_fl3, 'write',  wf_path)


        ut_log.log_proc_end('create-stg_flight')

    @classmethod
    def get_filename_stg_flight(cls):
        return cls.c_filep_stg_flight

    @classmethod
    def get_fp_stg_flight(cls):
        return cls.c_filep_stg_flight

    @classmethod
    def get_df(cls):
        if cls.__df_stg_flight ==None:
            df = ut_spark.df_read_from_parquet(cls.c_filep_stg_flight)
            if df==None:
                ut_log.log_warning("Get stg_flight failed-- stg_flight may not have been  created.")

            return df
        else:
            cls.__df_stg_flight


if __name__ == "__main__":
    intgr_stg_table.cr_stg_flight()