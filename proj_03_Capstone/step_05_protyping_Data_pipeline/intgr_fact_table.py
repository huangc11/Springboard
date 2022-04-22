

from pyspark.sql import functions as f

from pyspark.sql.functions import col

from ut_base import ut_base as ut_base
from ut_spark import ut_spark
from ut_store import ut_store
from ut_log import ut_log
from ut_pipeline import ut_pipeline as ut_pipl

class intgr_fact_table:
    '''A module to create fact table '''

    def __init__(self):
        pass

    @staticmethod
    def cr_fact_flight():

        ut_log.log_proc_start('create-fact_flight')
        #folder_intgr =ut_store.folder_intgr
        folder_intgr = ut_store.folder_intgr

        df_fl = ut_spark.df_read_from_parquet(folder_intgr+'stg_flight_detail')

        dfd_dt = ut_spark.df_read_from_parquet(folder_intgr+'dim_date')
        dfd_ca = ut_spark.df_read_from_parquet(folder_intgr+'dim_carrier')
        dfd_pl = ut_spark.df_read_from_parquet(folder_intgr+'dim_plane')
        dfd_od = ut_spark.df_read_from_parquet(folder_intgr+'dim_origin_dest')

        df_fl.printSchema()
        dfd_dt.show(3)
        print(df_fl.count())
        df_fl2 = df_fl.join(dfd_dt, df_fl["date_key"] == dfd_dt["date_key"]) \
            .drop(dfd_dt["date_key"]) \
            .drop(dfd_dt["year"]) \
            .drop(dfd_dt["month"]) \
            .select(col("year")\
                    ,col("month")\
                    ,col("seq_id") \
                    , col("date_key").alias("date_id") \
                    , col("orig_dest_key").alias("orig_dest_id") \
                    , col("FlightNum") \
                    , col("uniqueCarrier").alias("carrier_id") \
                    , col("tailNum").alias("plane_id")
                    , col("DepTime") \
                    , col("CRSDepTime").alias("scheduled_deptime") \
                    , col("ArrTime") \
                    , col("CRSArrTime").alias("scheduled_arrtime") \
                    , col("ArrDelay") \
                    , col("DepDelay")\
                    , col("CarrierDelay") \
                    , col("WeatherDelay") \
                    , col("NASDelay") \
                    , col("SecurityDelay") \
                    , col("LateAircraftDelay")
                    )

        df_fl2.show()
        print(df_fl2.count())
        res = ut_spark.df_write_to_file(df_fl2, folder_intgr+'fact_flight', format="parquet")

        log_id = ut_base.curr_time_fmt()
        ut_pipl.log_proc_metric(log_id, 'Intgr-Create-fact_flight', df_fl2, 'write', folder_intgr+'fact_flight')

        ut_log.log_proc_end('create-fact_flight')


if __name__ =="__main__":
    intgr_fact_table.cr_fact_flight()
