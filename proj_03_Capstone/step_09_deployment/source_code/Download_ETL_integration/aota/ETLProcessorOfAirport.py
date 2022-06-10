from pyspark.sql import functions as f

from ut_base import ut_base as ut_base
from ut_spark import ut_spark
from ut_store import ut_store
from ut_log import ut_log
from ETLProcessor import ETLProcessor

class ETLProcessorOfAirport(ETLProcessor):

    def Transform(self, df1):

        try:
            dup_cnt= ut_spark.df_dup_by_col(df1,"iata").count()
            assert dup_cnt==0

            df2 = df1.withColumn("airport_id", f.col("iata"))
            df2.show(3)
        except AssertionError:
            ut_log.log_exeption('Airport unique constraint on "iata" violated')
            return None
        except Exception as e:
            ut_log.log_exeption(type(e).__name__)
            ut_log.log_exeption(e)
            ut_log.log_error('Cleansing/transformation on airport failed due to ', type(e).__name__)
            return None


        return df2

if __name__ == '__main__':
    print(ut_store.raw_file_airport, ut_store.cln_file_airport)
    etl_airport = ETLProcessorOfAirport('airport', ut_store.raw_file_airport, ut_store.cln_file_airport)
    etl_airport.ETL_Run()