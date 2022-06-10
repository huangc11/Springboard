class ut_store:
    '''place holder for'''
    sparksql_shuffle_partition_num = 4

    # df = spark.read.csv("s3a://capstone-aota/raw/sales_info.csv")apstone/raw/'

    folder_base = 's3a://capstone-aota'

    folder_dwnld = '/data/aota/raw/'
    folder_raw = folder_base + 'raw/'
    folder_cln = folder_base + '/processed/'
    folder_intgr = folder_base + '/integrated/'
    folder_log = 'C:/demo/capstone/logs/'
    folder_tmp = 'C:/demo/capstone/tmp/'

    # Raw files name
    raw_file_plane = 'plane.csv'
    raw_file_carrier = 'carrier.csv'
    raw_file_airport = 'airport.csv'

    # Processed/Clean file name
    cln_file_plane = 'plane'
    cln_file_flight = 'flight'
    cln_file_carrier = 'carrier'
    cln_file_airport = 'airport'

    # dimension table data files
    file_dim_plane = 'dim_plane'
    file_dim_date = 'dim_date'
    file_dim_carrier = 'dim_carrier'
    file_dim_origin_dest = 'dim_origin_dest'

    # fact table data file
    file_fact_flight = 'fact_flight'

