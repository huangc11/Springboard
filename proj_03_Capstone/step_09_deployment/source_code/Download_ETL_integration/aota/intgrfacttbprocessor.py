
from ut_log import ut_log
from ut_base import ut_base
from ut_spark import ut_spark
from ut_store import ut_store


from pyspark.sql import functions as f
from pyspark.sql.functions  import col


#@classmethod
def gen_date_key(year, month, day):
    sep = ''
    return str(year) + sep + str(month).rjust(2, '0') + sep + str(day).rjust(2, '0')


#@classmethod
def gen_orig_dest_key(origin, dest):
    sep = '_'
    return origin + sep + dest

def df_write_parquet_orw(df, path):
    df.write.format("parquet").mode("overwrite").option("path", path).save()

def df_to_local(df,  file_path):

    df.write.format("parquet").mode("overwrite").option("path", file_path).save()
    spark = ut_spark.get_sparksession()
    return spark.read.parquet(file_path)


class intgrFactTbProcessor:
    '''A module to create the only staging table - stg_flight_detail'''
    c_fail = -1
    c_succ = 1
    c_ff_num_partition=8
    c_proc_name = 'Integraiton-Process'

    def __init__(self, year):
        self.year =year
        self.proc_name = intgrFactTbProcessor.c_proc_name + '-' + self.year
        self.tmp_df_fl_base_tb= ut_store.folder_tmp+ "tmp_df_fl_base_" + self.year


    def  read_data(self):

        try:
            spark = ut_spark.get_sparksession()

            c_proc_name= 'fact-table-generation'
            udf_gen_date_key = f.udf(gen_date_key, returnType=f.StringType())
            udf_gen_orig_dest_key = f.udf(gen_orig_dest_key, returnType=f.StringType())

            ut_log.log_info("Reaing data from processed tier for year {}...".format(self.year))
            with ut_base.timer():
                rfp = ut_store.folder_cln + ut_store.cln_file_flight + '/year=' + self.year
                print(rfp)
                df_fl = spark.read.parquet(rfp)

            df_count=df_fl.count()
            print("df_count: {}".format(df_count))
            if df_count==0:
                return None

            print(df_count)
            ut_log.log_info("Saving to local drive...")
            with ut_base.timer():
                df_fl=df_to_local(df_fl, ut_store.folder_tmp+"temp_df_fl")

            ut_log.log_info("Saved..")

            df_fl = df_fl.withColumn("year", f.lit(self.year))

            # Adding date_key for buildinng dim_date later
            df_fl1 = df_fl.withColumn("date_id", udf_gen_date_key("Year", "Month", "DayofMonth"))

            # Adding orig_dest_key for buildinng dim_orign_dest
            df_fl2 = df_fl1.withColumn("orig_dest_key", udf_gen_orig_dest_key("Dest", "Origin"))

            df_fl_base = ut_spark.add_mono_incr_id(df_fl2, "seq_id")
            df_fl_base = df_to_local(df_fl_base, ut_store.folder_tmp+ "tmp_df_fl_base_" + self.year)

            return  df_fl_base

        except  Exception as e:
            print(e)
            ut_log.log_exeption(e)
            return(None)


    def  process_stg_dim_table(self, df_fl_base):

        '''
             Create stg_dim_date, which contains dim_date for this subset
        :return:
        '''

        ''' **************************************************
          Append data  dim_date
        *'****************************************************'''

        spark = ut_spark.get_sparksession()

        #df_fl_base= spark.read.parquet(ut_store.folder_tmp+ "tmp_df_fl_base_1998" )
        #df_fl_base= spark.read.parquet(self.tmp_df_fl_base_tb)
        #df_fl_base = self.df_fl_base





        df_date = df_fl_base.select("date_id", "Year", "Month", "DayofMonth", "DayOfWeek").distinct()

        # append to
        stg_fp=ut_store.folder_intgr + "stg_dim_date"
        # = ut_spark.df_write_to_file(df_date, stg_fp, format="parquet")

        df_date.write.format("parquet").mode("append") .option("path", stg_fp).save()

        ut_log.log_info("stg_dim_date generation completed.")



        ''' **************************************************
          Append data  dim_orig_dest
        *'****************************************************'''

        df_od1= df_fl_base.select("orig_dest_key", "origin", "dest", "distance").distinct()
        df_ap = ut_spark.df_read_from_parquet(ut_store.folder_cln + 'airport')
        df_ap.cache()

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
        df_od2.count()
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

        stg_fp=ut_store.folder_intgr + "stg_dim_origin_dest"

        df_od3.write.format("parquet").mode("append").option("path", stg_fp).save()

        ut_log.log_info("stg_dim_origin_dest generation completed.")

    def generate_fact_flight(self, df_fl_base):

        spark = ut_spark.get_sparksession()

        df_fl_base = spark.read.parquet(self.tmp_df_fl_base_tb)
        df_fact_flt = df_fl_base.select \
            (col("year")
             , col("month")
             , col("seq_id")
             , col("date_id")
             , col("orig_dest_key").alias("orig_dest_id")
             , col("flightNum")
             , col("uniqueCarrier").alias("carrier_id")
             , col("tailNum").alias("plane_id")
             , col("DepTime"), col("CRSDepTime").alias("scheduled_deptime")
             , col("ArrTime")
             , col("CRSArrTime").alias("scheduled_arrtime")
             , col("ArrDelay")
             )

        # partition the fact dataset by "seq_id" (using seq_id to average the size of each partition)
        df_fact_flt_repart=df_fact_flt.repartition(8, "seq_id")

        '''-------------------------------------------------------
        Write fact table to storing location ("integeratio" layer)
        --------------------------------------------------------'''

        ut_log.log_info('Start writing fact_flight to files...at '+ ut_log.curr_time_str())

        #get file path
        wf_path = ut_store.folder_intgr + ut_store.file_fact_flight

        print(df_fact_flt_repart.count())


        df_fact_flt_repart.write.format("parquet") \
                .mode("append") \
                .partitionBy("year") \
                .option("path", wf_path) \
                .save()



        ut_log.log_proc_metric(ut_log.get_log_id(), self.proc_name, df_fact_flt_repart, 'write', wf_path)
        ut_log.log_info("fact_flight created.")



    def run(self):


        ut_log.log_proc_start(self.proc_name)

        self.df_fl_base =self.read_data()

        if self.df_fl_base!=None:

            self.df_fl_base.cache()

            self.process_stg_dim_table(self.df_fl_base)

            self.generate_fact_flight(self.df_fl_base)

            self.df_fl_base.unpersist()
        else: 
            ut_log.log_info('No data to process or reading errors.')

        ut_log.log_proc_end(self.proc_name)


if __name__ == '__main__':

    year_list = ['1997','1998', '1999', '2000', '2001', '2002', '2003','2004', '2005','2006']

            # [ '1998','2001','2002']
                  #'

    for year in year_list:
        iprocessor =intgrFactTbProcessor(year)
        iprocessor.run()