from dbconfig import dbConfig
from tracker import Tracker
from utspark import utSpark
from ut_log import ut_log

from reporter import Reporter
'''
class Reporter:
    def __init__(self,spark, my_config):
        pass
    def report(self, spark, trade_dt, eod_dir=None):
        print('hello')
'''


def run_reporter_etl(my_config):

    reporter = Reporter('2020-08-06')
    tracker = Tracker('analytical_etl', my_config)

    ut_log.log_info('analytical_etl started...')

    spark=utSpark.creat_spark()
    try:
        reporter.report(spark)
        tracker.update_job_status("success")
        ut_log.log_info('analytical_etl succeeded.')
    except Exception as e:

        tracker.update_job_status("failed")
        ut_log.log_info('analytical_etl failed due to ' + type(e).__name__)
        ut_log.log_info(e)
    return


if __name__ == '__main__':


    dbconfig = dbConfig("cuser2", "cuser2","localhost", "5432","capstone2")

    run_reporter_etl(dbconfig)


