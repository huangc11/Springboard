from pyspark.sql import functions as f

from ut_base import ut_base as ut_base
from ut_spark import ut_spark
from ut_store import ut_store
from ut_log import ut_log
from ETLProcessor import ETLProcessor



class ETLProcessorOfFlight(ETLProcessor):
    '''A class to ETL on one single set'''

    meta_flight = {
        '1987': {'filename': '1987.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/IXITH2', 'filesize': 12652442, 'year': 1987},
        '1988': {'filename': '1988.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/TUYWU3', 'filesize': 49499025, 'year': 1988},
        '1989': {'filename': '1989.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/T7EP3M', 'filesize': 49202298, 'year': 1989},
        '1990': {'filename': '1990.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/QJKL3I', 'filesize': 52041322, 'year': 1990},
        '1991': {'filename': '1991.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/EJ4WJO', 'filesize': 49877448, 'year': 1991},
        '1992': {'filename': '1992.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/PLPDQO', 'filesize': 50040946, 'year': 1992},
        '1993': {'filename': '1993.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/IOU9DX', 'filesize': 50111774, 'year': 1993},
        '1994': {'filename': '1994.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/BH5P0X', 'filesize': 51123887, 'year': 1994},
        '1995': {'filename': '1995.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/ZLTTDC', 'filesize': 74881752, 'year': 1995},
        '1996': {'filename': '1996.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/3KDWWL', 'filesize': 75887707, 'year': 1996},
        '1997': {'filename': '1997.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/RUGDRW', 'filesize': 76705687, 'year': 1997},
        '1998': {'filename': '1998.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/H07RX8', 'filesize': 76683506, 'year': 1998},
        '1999': {'filename': '1999.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/IP6BL3', 'filesize': 79449438, 'year': 1999},
        '2000': {'filename': '2000.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/YGU3TD', 'filesize': 82537924, 'year': 2000},
        '2001': {'filename': '2001.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/CI5CEM', 'filesize': 83478700, 'year': 2001},
        '2002': {'filename': '2002.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/OWJXH3', 'filesize': 75907218, 'year': 2002},
        '2003': {'filename': '2003.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/KM2QOA', 'filesize': 95326801, 'year': 2003},
        '2004': {'filename': '2004.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/CCAZGT', 'filesize': 110825331, 'year': 2004},
        '2005': {'filename': '2005.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/JTFT25', 'filesize': 112450321, 'year': 2005},
        '2006': {'filename': '2006.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/EPIFFT', 'filesize': 115019195, 'year': 2006},
        '2007': {'filename': '2007.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/2BHLWK', 'filesize': 121249243, 'year': 2007},
        '2008': {'filename': '2008.csv.bz2', 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/EIR0RA', 'filesize': 39277452, 'year': 2008}
    }




    def __init__(self, year):

        raw_file_name = ETLProcessorOfFlight.meta_flight[year]['filename']
        cln_file_name = 'flight'
        ETLProcessor.__init__(self, 'flight',raw_file_name ,  cln_file_name)
        self.year = year


    def Transform(cls, df1):
        """ transformation   """
        # 1. remove cancelled records
        # 2. remove column 'tax_in', 'tax_out'

        df2 = df1.filter(f.col("Cancelled") != 1).filter(f.col("Diverted") != 1)
        df2 = ut_spark.add_mono_incr_id(df2, "row_id1")
        df3=df2.drop("ActualElapsedTime", "CRSElapsedTime", "TaxiIn", "TaxiOut",
                     "Cancelled", "CancellationCode", "Diverted", "CarrierDelay",
                     "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
        return df3

    def define_schema(self):
        """To create the schema for read. from a given field list stored in ut_store"""
        # schema files
        flight_fields = [
            ('Year', 'int'),
            ('Month', 'int'),
            ('DayofMonth', 'int'),
            ('DayOfWeek', 'int'),
            ('DepTime', 'str'),
            ('CRSDepTime', 'str'),
            ('ArrTime', 'str'),
            ('CRSArrTime', 'str'),
            ('UniqueCarrier', 'str'),
            ('FlightNum', 'str'),
            ('TailNum', 'str'),
            ('ActualElapsedTime', 'int'),
            ('CRSElapsedTime', 'int'),
            ('AirTime', 'int'),
            ('ArrDelay', 'int'),
            ('DepDelay', 'int'),
            ('Origin', 'str'),
            ('Dest', 'str'),
            ('Distance', 'int'),
            ('TaxiIn', 'int'),
            ('TaxiOut', 'int'),
            ('Cancelled', 'imt'),
            ('CancellationCode', 'str'),
            ('Diverted', 'str'),
            ('CarrierDelay', 'int'),
            ('WeatherDelay', 'int'),
            ('NASDelay', 'int'),
            ('SecurityDelay', 'int'),
            ('LateAircraftDelay', 'int')
        ]

        return  ut_spark.list_to_schema(flight_fields)


    def write_to_file(self, df):
        '''Write the dataframe data to destination '''

        print("df:get_partition_num:")
        print(ut_spark.get_partition_num(df))
        ut_spark.show_partition_count(df)

        print('flight-write started...')
        wr_path = ut_store.folder_cln + 'flight'
        df.write \
            .format("parquet") \
            .mode("append") \
            .partitionBy("year") \
            .option("path", wr_path) \
            .save()

if __name__ =="__main__":
    fd_1 = ETLProcessorOfFlight('1997')
    fd_1.ETL_Run()

