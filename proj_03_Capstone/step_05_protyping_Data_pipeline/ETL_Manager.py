
'''

from pyspark.sql import functions as f

from ut_base import ut_base as ut_base
from ut_spark import ut_spark
from ut_store import ut_store

from ut_pipeline import ut_pipeline as ut_pipl

'''
from ut_log import ut_log
from ut_spark import ut_spark

from ETL_Flight import ETL_Flight
from ETL_Plane  import ETL_Plane
from ETL_Carrier import ETL_Carrier
from ETL_Airport import ETL_Airport

class ETL_Manger:

    def __init__(self):
        pass

    @staticmethod
    def run():
        ut_spark.initialise()

        ut_log.log_module_start("ETL Process")
        #ut_log.log_proc_start("ETL-Manager")

        ETL_Plane.exc_etl()
        ETL_Carrier.exc_etl()
        ETL_Airport.exc_etl()
        ETL_Flight.exc_etl()
        ut_log.log_module_end("ETL Process")




if __name__ =='__main__':

    ETL_Manger.run()
    '''ut_spark.initialise()

    ut_log.log_proc_start("ETL-Manager")


    ETL_Plane.exc_etl()
    ETL_Carrier.exc_etl()
    ETL_Airport.exc_etl()
    ETL_Flight.exc_etl()
    ut_log.log_proc_end("ETL-Manager")
    #ETL_Flight.exc_etl(）'''