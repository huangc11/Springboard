
from ut_log import ut_log
from ut_base import ut_base
from ut_spark import ut_spark
from ut_store import ut_store
#from intgrFactTbGeneration import intgrFactTbGeneration
from intgrfacttbprocessor import intgrFactTbProcessor

from pyspark.sql import functions as f
from pyspark.sql.functions import col

class CustomError(Exception):
    pass


def df_to_local(df,  file_path):

    df.write.format("parquet").mode("overwrite").option("path", file_path).save()
    spark = ut_spark.get_sparksession()
    return spark.read.parquet(file_path)

class integrationManager:
    '''The main process to create fact/dimension tables.'''

    def __init__(self):
        pass


    @staticmethod
    def create_dim_carrier():
        '''To create the carrier dimension table '''
        target_fp= ut_store.folder_intgr + ut_store.file_dim_carrier

        ut_log.log_proc_start('create-dim_carrier')

        df_carr = ut_spark.df_read_from_parquet(ut_store.folder_cln + ut_store.cln_file_carrier)
        df_carr2 = df_carr.withColumn("carrier_id", f.col("Code"))

        res = ut_spark.df_write_to_file(df_carr2, target_fp , format="parquet")


        ut_log.log_proc_metric(ut_log.get_log_id(), 'Intgr-dim_carrier', df_carr2, 'write', target_fp  )

    @classmethod
    def create_dim_plane(cls):
        '''To create the carrier dimension plane '''

        target_fp =ut_store.folder_intgr + ut_store.file_dim_plane

        ut_log.log_proc_start('create-dim_plane')

        # Read cleaned/processed dataset and add a new column
        df_pl = ut_spark.df_read_from_parquet(ut_store.folder_cln +ut_store.cln_file_plane) \
                .withColumn("plane_id", f.col("tailnum"))

        res = ut_spark.df_write_to_file(df_pl, target_fp, format="parquet")

        #log the writing metirc
        ut_log.log_proc_metric(ut_log.get_log_id(), 'Intgr-Create-dim_date', df_pl, 'write', target_fp)



    @classmethod
    def cr_fact_table(cls):
        year_list = ['1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006']

        for year in year_list:
            iprocessor = intgrFactTbProcessor(year)
            iprocessor.run()

    @classmethod
    def create_dim_date(cls):
        ''' Create dim_date '''
        spark = ut_spark.get_sparksession()

        stg_fp = ut_store.folder_intgr + "stg_dim_date"
        df_stg_dim_dt = spark.read.parquet(stg_fp)
        ut_log.log_proc_metric(ut_log.get_log_id(), "create dim_date", df_stg_dim_dt, 'read', stg_fp)

        df_2 = df_stg_dim_dt.select("date_id", "Year", "Month", "DayofMonth", "DayOfWeek").distinct()

        w_fp = ut_store.folder_intgr + "dim_date"
        df_2.write.format("parquet").mode("overwrite").option("path", w_fp).save()

        ut_log.log_proc_metric(ut_log.get_log_id(), "create dim_date", df_2, 'write', w_fp)

    @classmethod
    def create_dim_origin_dest(cls):

        ''' create table dim_origin_dest '''

        spark = ut_spark.get_sparksession()
        # read from stage table
        stg_fp = ut_store.folder_intgr + "stg_dim_origin_dest"
        df_stg_dim_od = spark.read.parquet(stg_fp)
        ut_log.log_proc_metric(ut_log.get_log_id(), "create dim_origin_dest", df_stg_dim_od, 'read', stg_fp)

        # write to loacal drive and read back -- for performance
        df_stg_dim_od = df_to_local(df_stg_dim_od, 'tmp_df_stg_dim_dt')

        ut_spark.show_partition_count(df_stg_dim_od)
        ## dim_date
        df_2 = df_stg_dim_od. \
            select(col("orig_dest_id")
                   , col("origin")
                   , col("dest")
                   , col("distance")
                   , col("orig_airport")
                   , col("orig_city")
                   , col("orig_state")
                   , col("orig_country")
                   , col("dest_airport")
                   , col("dest_city")
                   , col("dest_state")
                   , col("dest_country")).distinct()

        # write to dim_origin_test
        with ut_base.timer():
            w_fp = ut_store.folder_intgr + "dim_origin_dest"
            df_2.write.format("parquet").mode("overwrite").option("path", w_fp).save()

        ut_log.log_proc_metric(ut_log.get_log_id(), "create dim_origin_dest", df_2, 'write', w_fp)

    @classmethod
    def run(cls):
        ut_spark.initialise()

        ut_log.log_module_start("Intgeration Process")

        try:

            #create dim_carrier
            cls.create_dim_carrier()

            #create dim_plane
            cls.create_dim_plane()

            #create fact_flight and dim_date, dim_orig_dest
            cls.cr_fact_table()

            cls.create_dim_origin_dest()


            cls.create_dim_date()

        except Exception as e:
            ut_log.log_exeption(e)
            ut_log.log_module_abort("Intgeration Process")
            return


        ut_log.log_module_end("Integration Process")


if __name__ =='__main__':

     integrationManager.run()

