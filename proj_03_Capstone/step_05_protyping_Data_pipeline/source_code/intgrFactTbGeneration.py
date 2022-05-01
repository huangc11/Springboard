from pyspark.sql import functions as f

from pyspark.sql.functions  import col

from ut_base import ut_base as ut_base
from ut_spark import ut_spark
from ut_store import ut_store
from ut_log import ut_log

class CustomError(Exception):
    pass

class intgrFactTbGeneration:
    '''A module to create the only staging table - stg_flight_detail'''
    c_fail = -1
    c_succ = 1

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
    def create_fact_and_dim(cls):

        c_proc_name= 'fact-table-generation'

        ut_log.log_proc_start(c_proc_name)

        #read and process
        try:

            udf_gen_date_key = f.udf(cls.gen_date_key, returnType=f.StringType())
            udf_gen_orig_dest_key = f.udf(cls.gen_orig_dest_key, returnType=f.StringType())

            df_fl = ut_spark.df_read_from_parquet(ut_store.folder_cln + ut_store.cln_file_flight)

            log_id = ut_base.curr_time_fmt()
            ut_log.log_proc_metric(log_id, c_proc_name, df_fl, 'read',
                                    ut_store.folder_cln + ut_store.cln_file_flight)

            #Adding date_key for buildinng dim_date later
            df_fl1 = df_fl.withColumn("date_id", udf_gen_date_key("Year", "Month", "DayofMonth"))

            # Adding orig_dest_key for buildinng dim_orign_dest
            df_fl2 = df_fl1.withColumn("orig_dest_key", udf_gen_orig_dest_key("Dest", "Origin"))


            df_fl_base = ut_spark.add_mono_incr_id(df_fl2, "seq_id")
            df_fl_base.groupBy("year").count().show()

            ''' **************************************************
              Create dimension table -- dim_date
            *****************************************************'''
            df_date = df_fl_base.select("date_id", "Year", "Month", "DayofMonth", "DayOfWeek").distinct()
            res = ut_spark.df_write_to_file(df_date, ut_store.folder_intgr + "dim_date", format="parquet")
            if res ==ut_spark.c_fail:
                raise CustomError
            ut_log.log_info("dim_date created.")

            ''' **************************************************
              Create dimension table -- dim_orig_dest
            *****************************************************'''


            df_od1 = df_fl_base.select("orig_dest_key", "origin", "dest", "distance").distinct()
            df_ap = ut_spark.df_read_from_parquet(ut_store.folder_cln + 'airport')

            df_od2 = df_od1.join(df_ap, df_od1["Origin"] == df_ap["iata"]). \
                select(col("orig_dest_key")
                       , col("origin")
                       , col("dest")
                       , col("distance")
                       , col("airport").alias("orig_airport")
                       , col("city").alias("orig_city")
                       , col("state").alias("orig_state")
                       , col("country").alias("orig_country")
                       )

            df_od3 = df_od2.join(df_ap, df_od1["Dest"] == df_ap["iata"]). \
                select(col("orig_dest_key").alias("orig_dest_id")
                       , col("origin")
                       , col("dest")
                       , col("distance")
                       , col("orig_airport")
                       , col("orig_city")
                       , col("orig_state")
                       , col("orig_country")
                       , col("airport").alias("dest_airport")
                       , col("city").alias("dest_city")
                       , col("state").alias("dest_state")
                       , col("country").alias("dest_country")
                       )

            res = ut_spark.df_write_to_file(df_od3, ut_store.folder_intgr + "dim_origin_dest", format="parquet")
            if res ==ut_spark.c_fail:
                raise CustomError
            ut_log.log_info("dim_orig_dest created.")

            ''' **************************************************
              Create fact table -- fact_flight
            *****************************************************'''
            df_fact_flt =df_fl_base.select\
                    (col("year")
                    , col("month")
                    , col("seq_id")
                    , col("date_id")
                    , col("orig_dest_key").alias("orig_dest_id")
                    , col("flightNum")
                    , col("uniqueCarrier").alias("carrier_id")
                    , col("tailNum").alias("plane_id")
                    , col("DepTime")
                    , col("CRSDepTime").alias("scheduled_deptime")
                    , col("ArrTime")
                    , col("CRSArrTime").alias("scheduled_arrtime")
                    , col("ArrDelay")
                    , col("DepDelay")
                    , col("CarrierDelay")
                    , col("WeatherDelay")
                    , col("NASDelay")
                    , col("SecurityDelay")
                    , col("LateAircraftDelay")
                    )

            ut_log.log_info('df_fact_flt created.')
            df_fact_flt.groupBy("year").count().show()
            df_fact_flt.printSchema()

            # partition the fact dataset by "seq_id" (using seq_id to average the size of each partition)
            df_fact_flt_repart=df_fact_flt.repartition(12, "seq_id")
            ut_spark.show_partition_count(df_fact_flt_repart)

            '''-------------------------------------------------------
            Write fact table to storing location ("integeratio" layer)
           --------------------------------------------------------'''

            ut_log.log_info('Started writing fact_flight to files...at '+ ut_log.curr_time_str())

            #get file path
            wf_path = ut_store.folder_intgr + ut_store.file_fact_flight

            #Process by year: collect year information to df2
            df2 = df_fact_flt_repart.groupBy("year").count().orderBy("year")

            dataCollect = df2.collect()
            for row in dataCollect:
                year = row['year']

                df3 = df_fact_flt_repart.filter(f.col("year") == year)
                print('year={}:count={}'.format(year, df3.count()))
                df3.write.format("parquet") \
                    .mode("append") \
                    .partitionBy("year") \
                    .option("path", wf_path) \
                    .save()

            ut_log.log_proc_metric(log_id, c_proc_name, df_fl, 'write',
                                   wf_path)
            ut_log.log_info("fact_flight created.")

        except Exception as e:
            ut_log.log_exeption((e))
            ut_log.log_proc_abort(c_proc_name)
            return cls.c_fail


        ut_log.log_proc_end(c_proc_name)
        return cls.c_succ


if __name__ == "__main__":
    intgrFactTbGeneration.create_fact_and_dim()