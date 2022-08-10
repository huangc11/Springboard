class ut_store:
    '''place holder for'''
    sparksql_shuffle_partition_num =8

    folder_dwnld = 'C:/demo/capstone/raw/'
    folder_raw = 'C:/demo/capstone/raw/'
    folder_cln = 'C:/demo/capstone/processed/'
    folder_intgr = 'C:/demo/capstone/integrated/'
    folder_log = 'C:/demo/capstone/logs/'
    folder_tmp = '/C:/demo/capstone/tmp/'
    folder_intgr2='/C:/demo/capstone/dwsource/'

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

