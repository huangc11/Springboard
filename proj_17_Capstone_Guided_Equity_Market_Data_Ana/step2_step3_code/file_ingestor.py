

from pyspark.sql import SparkSession
from file_parser import fileParser as parser
from ut_store import ut_store


class fileIngestor:
        path_of_accepted_file = ut_store.c_accepted_folder

        def __init__(self, func_parser, src_path, tgt_path=None):
            self.f_src_path = src_path
            self.f_tgt_path = fileIngestor.path_of_accepted_file
            self.func_parser = func_parser

        def ingest_file(self, spark):

            print(self.f_src_path)
            print(self.f_tgt_path)

            try:
                # read
                raw = spark.sparkContext.textFile(self.f_src_path)
                raw.collect()

                # parse the RDD and convert to dataframe
                # parsed = raw.map(lambda line: parser.parse_json(line))
                parsed = raw.map(lambda line: self.func_parser(line))
                df1 = spark.createDataFrame(parsed, schema=parser.comm_evt_schema)
                df1.show(3)
                print(df1.count())

                # write
                print('writing to file {}'.format(self.f_tgt_path))
                df1.write.partitionBy("partition").mode("append").parquet(self.f_tgt_path)
                #res=df1.write.mode("append").parquet(self.f_tgt_path)
                #print(res)
                print('Ingest data file completed.')

            except Exception as e:
                print('Ingest data file failed, due to ' + type(e).__name__)


