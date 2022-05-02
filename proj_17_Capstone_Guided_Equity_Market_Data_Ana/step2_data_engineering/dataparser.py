

from pyspark.sql import types as ty
from pyspark.sql.types import StructType, StructField

import datetime as dt
import json

class fileParser:
    c_columns = ["trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "trade_pr",
                 "trade_size", "bid_pr", "bid_size", "ask_prdt.datetime.strptime(str, '%Y-%m-%d %H:%M:%S')", "ask_size", "partition", "line"]

    comm_evt_schema = StructType([ \
        StructField("trade_dt", ty.DateType(), True), \
        StructField("rec_type", ty.StringType(), True), \
        StructField("symbol", ty.StringType(), True), \
        StructField("exchange", ty.StringType(), True), \
        StructField("event_tm", ty.TimestampType(), True), \
        StructField("event_seq_nb", ty.IntegerType(), True), \
        StructField("arrival_tm", ty.TimestampType(), True), \
        StructField("trade_pr", ty.FloatType(), True), \
        StructField("trade_size", ty.IntegerType(), True), \
        StructField("bid_pr", ty.FloatType(), True), \
        StructField("bid_size", ty.IntegerType(), True), \
        StructField("ask_pr", ty.FloatType(), True), \
        StructField("ask_size", ty.IntegerType(), True), \
        StructField("partition", ty.StringType(), True), \
        StructField("line", ty.StringType(), True)
    ])

    @classmethod
    def f_to_date(cls, str=None):
        """ convert date formatted string to date"""

        if str==None:
            return None
        else:
            return dt.datetime.strptime(str, '%Y-%m-%d')

    @classmethod
    def f_to_datetime(cls, str=None):
        """ convert  formatted string to datetime"""
        if str==None:
            return None
        else:
            return dt.datetime.strptime(str, '%Y-%m-%d %H:%M:%S')


    @classmethod
    def format_event(cls,rec):
        """conver str fields to desired data types"""
        #print("input event 4 format:")
        #print(rec)
        event = (
                cls.f_to_date(rec[0])
                , rec[1]
                , rec[2]
                , rec[3]
                , cls.f_to_datetime(rec[4][:19])
                , int(rec[5])
                , cls.f_to_datetime(rec[6][:19])
                , float(rec[7])
                , int(rec[8])
                , float(rec[9])
                , int(rec[10])
                , float(rec[11])
                , int(rec[12])
                , rec[13]
                , rec[14]
            )

        return event



    @classmethod
    def parse_csv(cls, line):
        record_type_pos = 2
        rec= line.split(",")
        c_bad_event =(None, None, None, None, None, None, None,None, None, None, None, None, None, "B", line)

        try:

            if rec[record_type_pos] == "T":
                event1 =(rec[0], rec[2], rec[3], rec[6], rec[4], rec[5], rec[1][:19], rec[7] , rec[8], 0.0, 0, 0.0, 0, "T", "")
            #cls.tradecsv_to_event(record)
                event2= cls.format_event(event1)
                return event2

            elif rec[record_type_pos] == "Q":
                event1 =  (rec[0], rec[2] , rec[3], rec[6], rec[4], rec[5] , rec[1], 0.0, 0, rec[7], rec[8], rec[9], rec[10], "Q","")
                    #cls.quotecsv_to_event(record)
                event2 = cls.format_event(event1)
                return event2
            else:
                 return c_bad_event

        except Exception as e:
            print('parse_csv Exeption: ' + type(e).__name__)
            print(e)
            return c_bad_event

    @classmethod
    def parse_json(cls, line):
        c_bad_event = (None, None, None, None, None, None, None, None, None, None, None, None, None, "B", line)
        try:
            rec = json.loads(line)
            record_type = rec['event_type']

            if record_type == "T":
                if rec["size"] != 0:
                    event = (rec["trade_dt"], rec["event_type"], rec["symbol"], rec["exchange"], rec["event_tm"][:19],
                             rec["event_seq_nb"],
                             rec["file_tm"][:19], rec["price"], rec["size"], 0.0, 0, 0.0, 0, 'T', '')
                    event_fmt = cls.format_event(event)
                else:
                    event_fmt = c_bad_event

                return event_fmt

            elif record_type == "Q":
                if rec["bid_size"] != 0 or rec["ask_size"] != 0:  # [some key fields empty]
                    event = (rec["trade_dt"], rec["event_type"], rec["symbol"], rec["exchange"], rec["event_tm"][:19],
                             rec["event_seq_nb"],
                             rec["file_tm"][:19], 0.0, 0, rec["bid_pr"], rec["bid_size"], rec["ask_pr"],
                             rec["ask_size"], 'Q', '')
                    event_fmt = cls.format_event(event)
                else:
                    event_fmt = c_bad_event
                return event_fmt
            else:
                return c_bad_event

        except Exception as e:
            print(e)
            return c_bad_event

