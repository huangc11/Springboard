

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, rank, row_number
from pyspark.sql.window import Window

class EODProcessor:
    c_source_folder = 'C:\\demo\\captone2\\data\\accepted\\'
    c_target_folder = 'C:\\demo\\captone2\\data\\trade\\'

    def __init__(self, p_partition,  p_trade_date):

        self.partition = p_partition
        self.source_folder =EODProcessor.c_source_folder
        self.target_folder =EODProcessor.c_target_folder
        self.trade_date = p_trade_date

    def process_partition(self):
        spark = SparkSession.builder.master('local').appName('app').getOrCreate()

        #Read from 'accepted' folder
        trade_common = spark.read.parquet(self.source_folder + 'partition=' + self.partition)
        trade_common.show(3)
        print(trade_common.count())


        #process
        trade = trade_common.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "trade_pr")
        trade.orderBy("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm").show(20)


        win = Window.partitionBy("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb").orderBy("arrival_tm")
        win_trade = trade.withColumn('rank', row_number().over(win)).orderBy('arrival_tm')
        trade_corrected = win_trade.filter(col("rank") == 1)
        print(trade_corrected.count())

        # save

        trade_corrected.write.mode('overwrite').parquet(self.target_folder + "trade_dt={}".format(self.trade_date))

        return 1

if __name__ == "__main__":

    p_trade_date = "2020-08-05"


    #Process Trade
    p_partition = 'T'
    trade_processor= EODProcessor('T',  p_trade_date)
    trade_processor.process_partition()

    #process Quota
    p_partition = 'Q'
    quota_processor= EODProcessor('Q', p_trade_date)
    quota_processor.process_partition()

