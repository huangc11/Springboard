from pyspark.sql import functions as f

from pyspark.sql.functions  import col


#from ut_base import ut_base as ut_base

from ut_spark import ut_spark
from ut_store import ut_store
from ut_log import ut_log
from ut_base import ut_base

class CustomError(Exception):
    pass

spark = ut_spark.get_sparksession()


def flight_parquet_to_csv(year='1997'):
    r_fp = 'c:/demo/capstone/integrated/fact_flight/year=' + year
    ut_log.log_info(ut_log.curr_time_fmt() + ' Read from ' + r_fp + '...')
    df = spark.read.parquet(r_fp)

    df = df.withColumn("year", f.lit(year))

    print(ut_log.curr_time_fmt() + ' Started writing ...')

    df.write \
        .format("csv") \
        .mode("append") \
        .option("path", "c:/demo/capstone/dwsource/fact_flight") \
        .save()

    print(ut_log.curr_time_fmt() + ' Parquest->csv conversion completed for year= ' + year + '.')


year_list = ['1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006']

for year in year_list:
    try:
        flight_parquet_to_csv(year)
    except Exception as e:
        print('failed for year= '+ year +'.')
