

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, rank, row_number
from pyspark.sql.window import Window
from EOD_Processor import EODProcessor

if __name__ == "__main__":

    p_trade_date = "2020-08-06"


    #Process Trade data

    p_partition = 'T'
    trade_processor= EODProcessor('T',  p_trade_date)
    trade_processor.process_partition()

    #process Quota data
    p_partition = 'Q'
    quota_processor= EODProcessor('Q', p_trade_date)
    quota_processor.process_partition()

