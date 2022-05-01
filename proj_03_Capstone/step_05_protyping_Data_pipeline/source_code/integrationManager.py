
from ut_log import ut_log
from ut_base import ut_base
from ut_spark import ut_spark
from ut_store import ut_store
from intgrFactTbGeneration import intgrFactTbGeneration

from pyspark.sql import functions as f

class CustomError(Exception):
    pass

class integrationManager:
    '''The main process to create fact/dimension tables.'''

    def __init__(self):
        pass


    @staticmethod
    def create_dim_carrier():
        '''To create the carrier dimension table '''
        ut_log.log_proc_start('create-dim_carrier')

        df_carr = ut_spark.df_read_from_parquet(ut_store.folder_cln + ut_store.cln_file_carrier)
        df_carr2 = df_carr.withColumn("carrier_id", f.col("Code"))

        res = ut_spark.df_write_to_file(df_carr2, ut_store.folder_intgr + ut_store.file_dim_plane, format="parquet")


        ut_log.log_proc_metric(ut_log.get_log_id(), 'Intgr-Create-dim_date', df_carr2, 'write',
                                ut_store.folder_intgr + ut_store.file_dim_plane)

    @classmethod
    def create_dim_plane(cls):
        '''To create the carrier dimension plane '''
        ut_log.log_proc_start('create-dim_plane')

        # Read cleaned/processed dataset and add a new column
        df_pl = ut_spark.df_read_from_parquet(ut_store.folder_cln +ut_store.cln_file_plane) \
                .withColumn("plane_id", f.col("tailnum"))

        res = ut_spark.df_write_to_file(df_pl, ut_store.folder_intgr + ut_store.file_dim_plane, format="parquet")

        #log the writing metirc
        ut_log.log_proc_metric(ut_log.get_log_id(), 'Intgr-Create-dim_date', df_pl, 'write', ut_store.folder_intgr + ut_store.file_dim_plane)


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
            intgrFactTbGeneration.create_fact_and_dim()

        except Exception as e:
            ut_log.log_exeption(e)
            ut_log.log_module_abort("Intgeration Process")
            return


        ut_log.log_module_end("Integration Process")


if __name__ =='__main__':

     integrationManager.run()
