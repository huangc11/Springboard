
import csv
import datetime as dt
#from datetime import datetime
#from  main_pipeline import format_row


def format_row(row):
    def f_to_date(str='2020-08-01'):
        """ convert date formatted string to date"""
        return dt.datetime.strptime(str, '%Y-%m-%d')

    def f_to_date_int(str='2020-08-01'):
        """ convert date string to integer"""
        return int(f_to_date(str).strftime("%Y%m%d"))

    new_row = (
    int(row[0]), int(f_to_date_int(row[1])), int(row[2]), row[3], f_to_date(row[4]), row[5], row[6], int(row[7]),
    float(row[8]), int(row[9]))
    return new_row

def test_format_row():
    row =['1', '2020-08-01', '100', 'The North American International Auto Show', '2020-09-01', 'Exhibition', 'Michigan', '123', '35.00', '3']
    tgt_row =[1, 20200801, 100, 'The North American International Auto Show', dt.datetime(2020, 9, 1, 0, 0), 'Exhibition', 'Michigan', 123, 35.0, 3]
    fmt_row= format_row(row)

    print(fmt_row)
    print(tgt_row)

    for i in range(10):
     assert  fmt_row[i]==tgt_row[i]

