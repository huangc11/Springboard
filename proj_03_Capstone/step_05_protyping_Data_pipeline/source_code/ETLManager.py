
from ut_log import ut_log
from ut_spark import ut_spark
from ut_store import ut_store

from ETLProcessorOfAirport  import ETLProcessorOfAirport
from ETLProcessorOfPlane  import ETLProcessorOfPlane
from ETLProcessorOfCarrier import ETLProcessorOfCarrier
from ETLProcessorOfFlight  import ETLProcessorOfFlight

class ETLManger:
    '''The main process to run ETL on all dataset and other  datasets.'''

    @staticmethod
    def process_flight(list_of_year):

        for year in list_of_year:
            print(year)
            fd_1 = ETLProcessorOfFlight(year)
            fd_1.ETL_Run()

    @classmethod
    def run(cls):

        c_module_name = "ETL Process"

        ut_log.log_module_start(c_module_name)

        try:

            ut_spark.initialise()

            # Process 'airport'
            etl_airport = ETLProcessorOfAirport('airport', ut_store.raw_file_airport, ut_store.cln_file_airport)
            etl_airport.ETL_Run()

            # Process 'plane'
            etl_plane = ETLProcessorOfPlane('plane', ut_store.raw_file_plane, ut_store.cln_file_plane)
            etl_plane.ETL_Run()

            # process 'carrier'
            etl_carrier = ETLProcessorOfCarrier('carrier', ut_store.raw_file_carrier, ut_store.cln_file_carrier)
            etl_carrier.ETL_Run()


            #process 'flight'
            year_list =['1997','1998','1999','2000','2001','2002','2003','2004','2005','2006']
            cls.process_flight(year_list)

        except Exception as e:
            ut_log.log_module_abort (c_module_name)
            return

        ut_log.log_module_end(c_module_name)


if __name__ =='__main__':
    ETLManger.run()




