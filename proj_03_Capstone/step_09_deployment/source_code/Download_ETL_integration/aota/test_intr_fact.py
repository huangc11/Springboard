
from ut_log import ut_log
from ut_base import ut_base
from ut_spark import ut_spark
from ut_store import ut_store
from intgrFactTbGeneration import intgrFactTbGeneration

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



def  read_data(p_year=1998):

    spark = ut_spark.get_sparksession()

    c_proc_name= 'fact-table-generation'
    udf_gen_date_key = f.udf(gen_date_key, returnType=f.StringType())
    udf_gen_orig_dest_key = f.udf(gen_orig_dest_key, returnType=f.StringType())

    ut_log.log_info("Reaing data from processed tier for year {}...".format(p_year))
    with ut_base.timer():
        rfp = ut_store.folder_cln + ut_store.cln_file_flight + '/year=' + str(p_year)
        print(rfp)
        df_fl = spark.read.parquet(rfp)

    ut_log.log_info("Saving to local drive...")
    with ut_base.timer():
        df_fl=df_to_local(df_fl, ut_store.folder_tmp+"temp_df_fl")

    ut_log.log_info("Saved..")

    df_fl = df_fl.withColumn("year", f.lit(p_year))

    # Adding date_key for buildinng dim_date later
    df_fl1 = df_fl.withColumn("date_id", udf_gen_date_key("Year", "Month", "DayofMonth"))

    # Adding orig_dest_key for buildinng dim_orign_dest
    df_fl2 = df_fl1.withColumn("orig_dest_key", udf_gen_orig_dest_key("Dest", "Origin"))

    df_fl_base = ut_spark.add_mono_incr_id(df_fl2, "seq_id")
    df_fl_base = df_to_local(df_fl_base, ut_store.folder_tmp+ "tmp_df_fl_base_" + str(p_year))


def  prc_stg_dim_table():

    '''
         Create stg_dim_date, which contains dim_date for this subset
    :return:
    '''

    ''' **************************************************
      Append data  dim_date
    *'****************************************************'''

    spark = ut_spark.get_sparksession()

    df_fl_base= spark.read.parquet(ut_store.folder_tmp+ "tmp_df_fl_base_1998" )


    df_date = df_fl_base.select("date_id", "Year", "Month", "DayofMonth", "DayOfWeek").distinct()

    # append to
    stg_fp=ut_store.folder_intgr + "stg_dim_date"
    # = ut_spark.df_write_to_file(df_date, stg_fp, format="parquet")

    df_date.write.format("parquet").mode("append") .option("path", stg_fp).save()

    ut_log.log_info("stg_dim_data generation completed.")


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

    stg_fp=ut_store.foldoreer_tmp + "stg_dim_origin_dest"

    df_od3.write.format("parquet").mode("append").option("path", stg_fp).save()



c_proc_name = 'fact-table-generation'

ut_log.log_proc_start(c_proc_name)

read_data()
prc_stg_dim_table()

spark = ut_spark.get_sparksession()

df_fl_base = spark.read.parquet(ut_store.folder_tmp + "tmp_df_fl_base_1998")
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
     , col("DepDelay")
     )

# partition the fact dataset by "seq_id" (using seq_id to average the size of each partition)
df_fact_flt_repart=df_fact_flt.repartition(8, "seq_id")

'''-------------------------------------------------------
Write fact table to storing location ("integeratio" layer)
--------------------------------------------------------'''

ut_log.log_info('Started writing fact_flight to files...at '+ ut_log.curr_time_str())

#get file path
wf_path = ut_store.folder_intgr + ut_store.file_fact_flight


df_fact_flt_repart.write.format("parquet") \
        .mode("append") \
        .partitionBy("year") \
        .option("path", wf_path) \
        .save()



ut_log.log_proc_metric(ut_log.get_log_id(), c_proc_name, df_fact_flt_repart, 'write', wf_path)
ut_log.log_info("fact_flight created.")

ut_log.log_proc_end(c_proc_name)