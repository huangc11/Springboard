
class ut_store:
    # create schema

    folder_raw='C:\\demo\\capstone\\raw\\'
    folder_cln='C:\\demo\\capstone\\processed\\'
    folder_intgr = 'C:\\demo\\capstone\\integrated\\'
    folder_log =  'C:\\demo\\capstone\\logs\\'
    #folder_logpipeline='C:\\demo\\capstone\\logs\\logpipeline_detail\\'

    #file_core_name_flight ='flight'
    #file_core_name_plane = 'plane'
    ##ile_core_name_carrier= 'carrier'
    #file_core_name_airport = 'airport'

    #Raw files name
    raw_file_plane = 'plane.csv'
    raw_file_flight ='flight.csv'
    raw_file_carrier = 'carrier.csv'
    raw_file_airport = 'airport.csv'

    #Processed/Clean file names
    cln_file_plane ='plane'
    cln_file_flight = 'flight'
    cln_file_carrier = 'carrier'
    cln_file_airport = 'airport'

    # dimension table files
    file_dim_plane ='dim_plane'
    file_dim_date ='dim_date'
    file_dim_carrier='dim_carrier'
    file_dim_origin_dest = 'dim_origin_dest'

    file_prefix_pipelinelog = 'pipeline_metric_'
    file_postfix_pipelinelog ='.txt'

    metric_time_fmt ="%m/%d/%Y-%H:%M:%S"
    sparksql_shuffle_partition_num=4



    '''*****************************************
      Flight Schema fields list
    *************************************'''


    flight_schema_fields = [
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