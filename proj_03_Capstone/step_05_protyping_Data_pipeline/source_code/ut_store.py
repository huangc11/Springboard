
class ut_store:
    '''place holder for  parameters'''

    sparksql_shuffle_partition_num = 4

    folder_raw='C:\\demo\\capstone\\raw\\'
    folder_cln='C:\\demo\\capstone\\processed\\'
    folder_intgr = 'C:\\demo\\capstone\\integrated\\'
    folder_log =  'C:\\demo\\capstone\\logs\\'

    #Raw files name
    raw_file_plane = 'plane.csv'
    raw_file_flight ='2008.csv.bz2'
    raw_file_carrier = 'carrier.csv'
    raw_file_airport = 'airport.csv'

    #Processed/Clean file name
    cln_file_plane ='plane'
    cln_file_flight = 'flight'
    cln_file_carrier = 'carrier'
    cln_file_airport = 'airport'

    # dimension table data files
    file_dim_plane ='dim_plane'
    file_dim_date ='dim_date'
    file_dim_carrier='dim_carrier'
    file_dim_origin_dest = 'dim_origin_dest'

    # fact table data file
    file_fact_flight ='fact_flight'


    #schema files
    c_flight_fields = [
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




