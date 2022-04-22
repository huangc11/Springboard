

from pyspark.sql import functions as f

from pyspark.sql.functions  import col

from ut_base import ut_base as ut_base
from ut_spark import ut_spark
from ut_store import ut_store
from ut_log import ut_log
from ut_pipeline import ut_pipeline as ut_pipl
from intgr_stg_table import intgr_stg_table

class intgr_dim_table:
    '''A module to create dimension tables'''

    def __init__(self):
        pass


    @staticmethod
    def create_dim_carrier():
        ut_log.log_proc_start('create-dim_carrier')

        df_carr = ut_spark.df_read_from_parquet(ut_store.folder_cln + ut_store.cln_file_carrier)

        df_carr2 = df_carr.withColumn("carrier_id", f.col("Code"))

        #dup_cnt = ut_clean.df_dup_by_col(df_carr2, "carrier_id").count()
        #assert dup_cnt == 0

        file_path = ut_store.folder_intgr + ut_store.file_dim_plane
        res = ut_spark.df_write_to_file(df_carr2, file_path, format="parquet")

        log_id = ut_base.curr_time_fmt()
        ut_pipl.log_proc_metric(log_id, 'Intgr-Create-dim_date', df_carr2, 'write',
                                file_path)

        #ut_log.log_proc_end('create-dim_carrier')

    @classmethod
    def create_dim_plane(cls):
        def verify_unique_key(df_pl2):
            try:
               pass
               # dup_cnt = ut_clean.df_dup_by_col(df_pl2, "plane_id").count()
               # assert dup_cnt == 0
            except AssertionError:
                ut_log.log_error('Create dim_plane-- plane_id not unique. Process failed')
                ut_log.log_proc_fail('create-dim_plane')
                exit(-1)

        ut_log.log_proc_start('create-dim_plane')

        # Read cleaned/processed dataset
        df_pl = ut_spark.df_read_from_parquet(ut_store.folder_cln +ut_store.cln_file_plane)

        # Add plane_id column
        df_pl2 = df_pl.withColumn("plane_id", f.col("tailnum"))

        # verify plane_id is unique
        verify_unique_key(df_pl2)

        res = ut_spark.df_write_to_file(df_pl2, ut_store.folder_intgr + ut_store.file_dim_plane, format="parquet")

        log_id = ut_base.curr_time_fmt()
        ut_pipl.log_proc_metric(log_id, 'Intgr-Create-dim_date', df_pl2, 'write', ut_store.folder_intgr + ut_store.file_dim_plane)



    @classmethod
    def create_dim_date(cls):

        ut_log.log_proc_start('create-dim_date')

        df_fl_base = ut_spark.df_read_from_parquet(intgr_stg_table.get_fp_stg_flight())
        df_date = df_fl_base.select("date_key", "Year", "Month", "DayofMonth", "DayOfWeek").distinct()

        # rename columndate_key to date_id
        df_date2 = df_date.withColumn("date_id",f.col("date_key"))
        df_date2.printSchema()


        file_path = ut_store.folder_intgr + "dim_date"
        res = ut_spark.df_write_to_file(df_date2, file_path, format="parquet")

        log_id = ut_base.curr_time_fmt()
        ut_pipl.log_proc_metric(log_id, 'Intgr-Create-dim_date', df_date2, 'write',file_path)


        #ut_log.log_proc_end('create-dim_date')

        return df_date2

    @classmethod
    def cr_dim_origin_dest(cls):

        fp_dim_od =ut_store.folder_intgr + "dim_origin_dest"

        ut_log.log_proc_start('create-dim_origin_dest')

        df_fl = ut_spark.df_read_from_parquet(intgr_stg_table.get_fp_stg_flight())

        df_od1 = df_fl.select("orig_dest_key", "origin", "dest", "distance").distinct()
        df_ap = ut_spark.df_read_from_parquet(ut_store.folder_cln + 'airport')

        df_od2 = df_od1.join(df_ap, df_od1["Origin"] == df_ap["iata"]). \
            select(col("orig_dest_key") \
                   , col("origin") \
                   , col("dest") \
                   , col("distance") \
                   , col("airport").alias("orig_airport") \
                   , col("city").alias("orig_city") \
                   , col("state").alias("orig_state") \
                   , col("country").alias("orig_country") \
                   )

        df_od3 = df_od2.join(df_ap, df_od1["Dest"] == df_ap["iata"]). \
            select(col("orig_dest_key").alias("orig_dest_id") \
                   , col("origin") \
                   , col("dest") \
                   , col("distance") \
                   , col("orig_airport") \
                   , col("orig_city") \
                   , col("orig_state") \
                   , col("orig_country") \
                   , col("airport").alias("dest_airport") \
                   , col("city").alias("dest_city") \
                   , col("state").alias("dest_state") \
                   , col("country").alias("dest_country") \
                   )

        res = ut_spark.df_write_to_file(df_od3, fp_dim_od, format="parquet")

        log_id = ut_base.curr_time_fmt()
        ut_pipl.log_proc_metric(log_id, 'Intgr-Create-dim_origin_dest', df_od3, 'write', fp_dim_od)


        #ut_log.log_proc_end('create-dim_origin_dest')



    @classmethod
    def cr_all(cls):

            ut_spark.initialise()

            ut_log.log_proc_start("Ingration--Create Dimension tables")
            cls.create_dim_plane()
            cls.cr_dim_origin_dest()
            cls.create_dim_date()
            cls.create_dim_carrier()

            ut_log.log_proc_end("Ingration--Create Dimension tables")


if __name__ == "__main__":
    intgr_dim_table.cr_all()

