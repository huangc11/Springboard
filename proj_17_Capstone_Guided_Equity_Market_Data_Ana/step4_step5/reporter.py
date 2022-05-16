'''

class  Reporter:

    def __init__(self, spark, my_config):
        self.spark = spark
        self.my_config = my_config

    def report(self):
        pass

'''

c_base_folder = 'C:/demo/capstone2/data/'
from utspark import utSpark
from ut_store import ut_store

from pyspark.sql import functions as f
from pyspark.sql.window import Window


from pyspark.sql import SparkSession



class Reporter():

    def __init__(self,p_trade_dt):

        self.trade_dt = p_trade_dt


    def report(self, spark):

        '''Create a staging table for the30-min Moving Average'''
        # Read trade



        print("create_moving_avg started ...")
        df= spark.read.parquet(c_base_folder + 'trade' )

            #('C:\\demo\\capstone2\\data\\trade')

        df.createOrReplaceTempView("trades")

        #Create Trade Staging table -- tmp_trade

        df1 = spark.sql("select trade_dt, symbol,exchange, event_tm, event_seq_nb, "
                        "trade_pr from trades where trade_dt='{}'".format(self.trade_dt))
        df1.createOrReplaceTempView("tmp_trade")


        mov_avg_df=spark.sql("""
                select trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr, 
                avg(trade_pr) 
                  over(partition by symbol order by event_tm  RANGE BETWEEN INTERVAL '30' MINUTE PRECEDING and current ROW ) 
                  as mov_avg_pr  
                from tmp_trade
                order by symbol""")

        mov_avg_df.createOrReplaceTempView("tmp_trade_moving_avg")
        mov_avg_df.coalesce(1).write.format('parquet').mode("overwrite").saveAsTable("temp_trade_moving_avg")
        print("create_moving_avg compleed ...")


        '''
        Create Staging table for the prior day's last trade
        Get previous date last trade and calculate'''

        import datetime as dt

        prev_date = dt.datetime.strptime(self.trade_dt, '%Y-%m-%d') - dt.timedelta(days=1)
        prev_date_str = dt.datetime.strftime(prev_date, '%Y-%m-%d')

        df = spark.sql(
            "select symbol,exchange, event_tm, event_seq_nb, trade_pr "
            "from trades where trade_dt='{}'".format(prev_date_str))

        df.createOrReplaceTempView('tmp_last_trade')

        #Calculate last trade price
        last_pr_df=spark.sql("""
                select distinct symbol, exchange, last_pr from(
                    select symbol, exchange, event_tm, event_seq_nb, trade_pr,  
                           last(trade_pr) over(partition by exchange, symbol order by event_tm
                             range between UNBOUNDED PRECEDING and  UNBOUNDED FOLLOWING ) as last_pr  
                    from tmp_last_trade 
                    order by exchange, symbol)
                """)

        last_pr_df.coalesce(1).write.format('parquet').mode("overwrite").saveAsTable('temp_last_trade')

        last_pr_df.show()

        print("create_privday_last_trade completed ...")


        '''
        Populate The Latest Trade and Latest Moving Average Trade Price To The Quote Records
        '''


        # Read processed quote data of current date
        df_q = spark.read.parquet(c_base_folder+ 'quote/trade_dt=' + self.trade_dt)
        df_q.createOrReplaceTempView('quote')

        # create denormalized view of union quotes and  tmp_trade_moving_avg
        df_quote_union = spark.sql('''
            select  trade_dt, 
              "T" as rec_type, 
              symbol, 
              exchange, 
              event_tm, 
              event_seq_nb, 
              trade_pr, 
              null as bid_pr, 
              null as bid_size, 
              null as ask_pr, 
              null as ask_size, 
              mov_avg_pr from tmp_trade_moving_avg 
            union 
            select 
              trade_dt, 
              rec_type, 
              symbol, 
              exchange, 
              event_tm, 
              event_seq_nb, 
              trade_pr,  
              bid_pr, 
              bid_size, 
              ask_pr,  
              ask_size, 
              null from 
              quote''')

        df_quote_union.createOrReplaceTempView("quote_union")

        # popluate the latest trade_pr and mov_avg_pr
        quote_union_update = spark.sql("""
            select *,
            last_value(trade_pr, true) OVER (PARTITION BY symbol, exchange ORDER BY event_tm ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_trade_pr,
            last_value(mov_avg_pr, true) OVER (PARTITION BY symbol, exchange ORDER BY event_tm ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_mov_avg_pr
            from quote_union
        """)
        quote_union_update.createOrReplaceTempView("quote_union_update")

        #filter out the quote reords
        quote_update = spark.sql("""
            select trade_dt, symbol, event_tm, event_seq_nb, exchange,
                    bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr
            from quote_union_update
            where rec_type = 'Q'
        """)
        quote_update.createOrReplaceTempView("quote_update")

        # show the results
        print("quote_update count:")
        print(quote_update .count())
        quote_update.toPandas().head(3)

        # to get thep rior day close price
        quote_final = spark.sql("""
            select t1.trade_dt, t1.symbol, event_tm, event_seq_nb, t1.exchange,
                bid_pr, bid_size, ask_pr, ask_size, 
                last_trade_pr, 
                last_mov_avg_pr,
                bid_pr - t2.last_pr as bid_pr_mv, 
                ask_pr -t2.last_pr as ask_pr_mv,
                t2.last_pr
            from  quote_update t1
            left join  temp_last_trade t2 on 
                    t1.symbol= t2.symbol and  t1.exchange= t2.exchange
            """)


        from ut_store import ut_store
        wfp =c_base_folder + "quote-trade-analytical"
        print((wfp))

        quote_final.write.partitionBy("trade_dt").mode("append").parquet(wfp)
        quote_final.show(3)
        print("final update writen to dest")


if __name__ == "__main__":
    '''
   # spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
   '''

    spark= utSpark.creat_spark()

    reporter =Reporter('2020-08-06')
    reporter.report(spark)
