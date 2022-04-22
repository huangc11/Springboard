
'''

from pyspark.sql import functions as f

from ut_base import ut_base as ut_base
from ut_spark import ut_spark
from ut_store import ut_store

from ut_pipeline import ut_pipeline as ut_pipl

'''
from ut_log import ut_log
from ut_spark import ut_spark

from intgr_stg_table import intgr_stg_table
from intgr_dim_table import intgr_dim_table
from intgr_fact_table import intgr_fact_table

class intgr_Manager:

    def __init__(self):
        pass

    @staticmethod
    def run():
        ut_spark.initialise()

        ut_log.log_module_start("Intgeration Process")

        intgr_stg_table.cr_stg_flight()

        intgr_dim_table.cr_all()

        intgr_fact_table.cr_fact_flight()


        ut_log.log_module_end("Integration Process")




if __name__ =='__main__':

    intgr_Manager.run()
