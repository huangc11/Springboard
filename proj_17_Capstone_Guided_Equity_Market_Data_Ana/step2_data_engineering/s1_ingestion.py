
from pyspark.sql import SparkSession
from dataparser import dataParser as parser


class fileIngestor:
    path_of_accepted_file = 'C:\\demo\\captone2\\data\\accepted'

    def __init__(self,func_parser, src_path, tgt_path ):
        self.f_src_path =src_path
        self.f_tgt_path = fileIngestor.path_of_accepted_file
        self.func_parser= func_parser

    def ingest_file(self, spark):

        print(self.f_src_path)
        print(self.f_tgt_path)

        try:
            # read
            raw = spark.sparkContext.textFile(self.f_src_path)
            raw.collect()

            # parse the RDD and convert to dataframe
            #parsed = raw.map(lambda line: parser.parse_json(line))
            parsed = raw.map(lambda line: self.func_parser(line))
            df1 = spark.createDataFrame(parsed, schema=parser.comm_evt_schema)
            df1.show(3)
            print(df1.count())

            #write
            df1.write.partitionBy("partition").mode("append").parquet(self.f_tgt_path)
            print('Ingest data file completed.')

        except Exception as e:
            print('Ingest data file failed, due to ' + type(e).__name__)


if __name__ == "__main__":

    spark = SparkSession.builder.master('local').appName('app').getOrCreate()

    #process json
    f_src_path = 'C:\\demo\\captone2\\data\\source\\' +'json\\2020-08-05\\NASDAQ'

    
    json_ingestor = fileIngestor(parser.parse_json(), f_src_path)
    json_ingestor.ingest_file(spark)

    #process csv
    f_src_path= 'C:\\demo\\captone2\\data\\source\\'+ 'csv\\2020-08-05\\NYSE'
    csv_ingestor = fileIngestor(parser.parse_csv, f_src_path)
    csv_ingestor.ingest_file(spark)
    

