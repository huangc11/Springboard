
from pyspark.sql import SparkSession

from file_parser import fileParser as parser
from file_ingestor import fileIngestor

from datetime import datetime

from ut_log import ut_log

def get_file_path(file_type, year,month, day):
    c_fold_source = 'C:\\demo\\capstone2\\data\\source'

    assert file_type in ('json', 'csv')

    calc_date = datetime(year,month, day)
    date_str = calc_date.strftime("%Y-%m-%d")

    postfix = 'NYSE'  if file_type=='csv'  else 'NASDAQ'
    file_path = '{}\\{}\\{}\\{}'.format(c_fold_source, file_type, date_str,postfix)

    return file_path

if __name__ == "__main__":

    spark = SparkSession.builder.master('local').appName('app').getOrCreate()


    '''*************************** 
       Ingest json file of a certain date
     ***************************'''

    f_src_path= get_file_path('json', 2020, 8, 6)
    ut_log.log_info('Ingest file {} ...'.format(f_src_path))

    json_ingestor = fileIngestor(parser.parse_json, f_src_path)
    json_ingestor.ingest_file(spark)

    '''*************************** 
      Ingest csv file of a certain date
     ***************************'''

    f_src_path = get_file_path('csv', 2020, 8, 6)
    ut_log.log_info('Ingest file {} ...'.format(f_src_path))

    csv_ingestor = fileIngestor(parser.parse_csv, f_src_path)
    csv_ingestor.ingest_file(spark)
    

