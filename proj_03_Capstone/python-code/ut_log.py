import logging

from ut_store import ut_store

from datetime import datetime


"""***************************************************************************
               Set up gobal loggers:
    1. glb_logger:to log the progress and information for all the process
    2. metric_logger: to log pipeline metrics, such as read/write,log file, row counts, column counts
    2. exception_logger: to log the exeption details
    
**********************************************************************************"""

c_global_log_name ='log_pipeline.log'
c_metric_log_name ='log_metric.log'
c_exception_log_name='log_exception.log'


format_def='%(asctime)s:%(name)s:%(levelname)s:%(message)s'
glb_logger= logging.getLogger('gloabal_logger')
   # (__name__)
glb_logger.setLevel(logging.INFO)
#glb_formatter = logging.Formatter('%(asctime)s:%(filename)s:%(name)s:%(levelname)s:%(message)s')
glb_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')

glb_log_fp=ut_store.folder_log +c_global_log_name
glb_file_handler = logging.FileHandler(glb_log_fp )
print('pipeline_log_path:'.format(glb_log_fp))
glb_file_handler.setLevel(logging.INFO)
glb_file_handler.setFormatter(glb_formatter)
glb_logger.addHandler((glb_file_handler))


metric_logger= logging.getLogger('metric_logger')
metric_logger.setLevel(logging.INFO)
metric_formatter = logging.Formatter('%(message)s')

metric_file_handler = logging.FileHandler(ut_store.folder_log +c_metric_log_name  )
metric_file_handler.setLevel(logging.INFO)
metric_file_handler.setFormatter(metric_formatter)
metric_logger.addHandler(metric_file_handler)

exception_logger= logging.getLogger('exception_logger')
exception_logger.setLevel(logging.INFO)
exception_formatter = logging.Formatter('%(message)s')

exception_file_handler = logging.FileHandler(ut_store.folder_log + c_exception_log_name )
exception_file_handler.setLevel(logging.INFO)
exception_file_handler.setFormatter(exception_formatter)
exception_logger.addHandler(exception_file_handler)

"""*******************************************************************************
    ut_log:  a class of utilites to log all kinds of information
**********************************************************************************"""


class ut_log:
    logger_excption = None
    logger_app = None
    c_date_format = "%m/%d/%Y-%H:%M:%S"

    @staticmethod
    def curr_time_str():
        dt_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        return dt_str

    @staticmethod
    def curr_time_fmt():
        """Get a formatted string of current timestamp: 2022-04-02:10:34:22"""
        tm_now = datetime.now()
        dt_str = tm_now.strftime("%Y-%m-%d_%H:%M:%S")
        return dt_str

    '''*******************************************************************************
            Logging for generic purpose (glb_looger)
    *******************************************************************************'''

    @staticmethod
    def get_log_id():
        '''To create a unique for logging.  Here we use timestamp.'''
        return ut_log.curr_time_fmt()


    @staticmethod
    def log_info(msg):
        """Log a piece of information on glb_logger"""
        glb_logger.info(msg)
        print(msg)

    @staticmethod
    def log_error(msg):
        """Log an errormessage on glb_logger"""
        glb_logger.error(msg)
        print(msg)

    @staticmethod
    def log_warning(msg):
        """Log a warning message on glb_logger"""
        glb_logger.warning(msg)
        print(msg)

    @staticmethod
    def log_metric(line):
        """Log one entry on metric_logger"""
        metric_logger.info(line)



    @classmethod
    def log_exeption(cls, e):
        """Log an exception on exception_logger and print"""
        glb_logger.error("-----------An excepiton raised at {}: {}".format(cls.curr_time_fmt(), type(e).__name__))
        exception_logger.error("-----------An excepiton raised at {}: {}".format(cls.curr_time_fmt(), type(e).__name__))
        exception_logger.error(e)
        print(e)

    '''*******************************************************************************
            Logging related to a Download/ETL/Integrationprocess 
    *******************************************************************************'''

    @classmethod
    def log_proc_start(cls, proc_name):
        """Log the start of a process, such as ETL-flight"""
        msg1 = ' '+ proc_name + ' started at {}'.format(cls.curr_time_fmt())
        msg2 = msg1.center (90,'-')
        print(msg2)

        #print a blank line
        glb_logger.info(" ")
        glb_logger.info(msg2)

    @classmethod
    def log_proc_end(cls, proc_name:str):
        """Log the completion of a process. In most case, this means a successful completion."""
        msg1 = ' '+ proc_name + ' completed at {}'.format(cls.curr_time_fmt())
        msg2 = msg1.center (90,'-')
        print(msg2)
        glb_logger.info(msg2)

    @classmethod
    def log_proc_fail(cls, proc_name:str, due_to=None):
        """ log an abortion of a process, and the cause if provided"""

        if due_to==None:
            msg1 = ' '+ proc_name + ' aborted.'
        else:
            msg1 = ' ' + proc_name + ' at {}'.format(cls.curr_time_fmt())+ \
                   ' failed, due to ' + due_to +'.'

        msg2 = msg1.center (90, '-')
        print(msg2)
        glb_logger.info(msg2)

    @classmethod
    def log_proc_abort(cls, proc_name:str, due_to=None):
        """ log an abortion of a process, and the cause if provided"""

        if due_to==None:
            msg1 = ' '+ proc_name + ' aborted.'
        else:
            msg1 = ' ' + proc_name + ' at {}'.format(cls.curr_time_fmt())+ \
                   ' aborted, due to ' + due_to +'.'

        msg2 = msg1.center (90, '-')
        print(msg2)
        glb_logger.info(msg2)


    @staticmethod
    def log_metric(line):
        """Log one entry on metric_logger"""
        metric_logger.info(line)


    @staticmethod
    def log_proc_metric(log_id, proc_name, df, rw_type, file_path, file_format=None):
        ''' Log metric of one read/write acton of pipeline process, such read from
        a raw file or write to a processed file
        '''

        c_metric_time_fmt ="%m/%d/%Y-%H:%M:%S"

        cnt_col = len(df.columns)
        cnt_row = df.count()
        dt_str = datetime.today().strftime(c_metric_time_fmt)
        metric_line = "{},{},{},{},{},{},{},{}".format(proc_name, dt_str, rw_type, file_path, file_format, cnt_col, cnt_row,log_id)
        ut_log.log_info(metric_line)
        ut_log.log_metric(metric_line)

    '''*******************************************************************************
            Logging related to a moudle (ETL/Inegration/Downgrade)
    *******************************************************************************'''

    @classmethod
    def log_module_start(cls, module_name:str):
        """Log start of execution of a moudle, such as ETL manager"""
        msg1 = ' '+ module_name + ' started at {}'.format(cls.curr_time_fmt())
        msg2 = msg1.center(90,'*')
        print(msg2)

        #print a blank line
        glb_logger.info(" ")
        glb_logger.info(msg2)

    @classmethod
    def log_module_end(cls, module_name:str):
        """Log start of execution of a moudle"""
        msg1 = ' ' + module_name + ' completed at {}'.format(cls.curr_time_fmt())
        msg2 = msg1.center (90,'*')
        print(msg2)
        glb_logger.info(msg2)

    @classmethod
    def log_module_abort(cls, module_name:str):
        """Log start of execution of a moudle"""
        msg1 = ' ' + module_name + ' aborted at {}'.format(cls.curr_time_fmt())
        msg2 = msg1.center (90,'*')
        print(msg2)
        glb_logger.info(msg2)


    @staticmethod
    def setup_logger(name, log_file_name, level=logging.INFO, format=format_def):
            """To setup a customized logger"""

            log_file_path = ut_store.folder_log + log_file_name
            f_handler = logging.FileHandler(log_file_path)
            f_formatter = logging.Formatter(format)
            f_handler.setFormatter(f_formatter)

            logger = logging.getLogger(name)
            logger.setLevel(level)
            logger.addHandler(f_handler)

            return logger


if __name__ =='__main__':
    print(ut_log.curr_time_str())