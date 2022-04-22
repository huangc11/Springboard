import logging

from ut_store import ut_store
from ut_base import ut_base



format_def='%(asctime)s:%(name)s:%(levelname)s:%(message)s'
glb_logger= logging.getLogger('gloabal_logger')
   # (__name__)
glb_logger.setLevel(logging.INFO)
#glb_formatter = logging.Formatter('%(asctime)s:%(filename)s:%(name)s:%(levelname)s:%(message)s')
glb_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')

glb_file_handler = logging.FileHandler(ut_store.folder_log + 'log_pipeline.log')
glb_file_handler.setLevel(logging.INFO)
glb_file_handler.setFormatter(glb_formatter)
glb_logger.addHandler((glb_file_handler))


metric_logger= logging.getLogger('metric_logger')
metric_logger.setLevel(logging.INFO)
metric_formatter = logging.Formatter('%(message)s')

metric_file_handler = logging.FileHandler(ut_store.folder_log + 'log_metric.log')
metric_file_handler.setLevel(logging.INFO)
metric_file_handler.setFormatter(metric_formatter)
metric_logger.addHandler(metric_file_handler)

exception_logger= logging.getLogger('exception_logger')
exception_logger.setLevel(logging.INFO)
exception_formatter = logging.Formatter('%(message)s')

exception_file_handler = logging.FileHandler(ut_store.folder_log + 'log_exception.log')
exception_file_handler.setLevel(logging.INFO)
exception_file_handler.setFormatter(exception_formatter)
exception_logger.addHandler(exception_file_handler)








class ut_log:
    logger_excption =None
    logger_app = None

    def print_one(one):
        print('********** {} **************'.format(one))

    def print_error(one):
        print('####### {} ########'.format(one))

    def print_success(one):
       # print('!!!!!!!!!! {} !!!!!!!!!!!!'.format(one)
        print(one.center(70,'!'))

    def print_info(one):
       print(one)

    def msg_fmt_success(one):
        return (one.center(100,'!'))

    def msg_fmt_error(one):
        return (one.center(70,'#'))

    def log_exeption(e):
        glb_logger.info("An excepiton has been raised...")
        exception_logger.error("-----------An excepiton raised at {}".format(ut_base.curr_time_fmt()))
        exception_logger.error(e)
        print(e)
        #Utility.logger_excption.exception(e)

    def log_info(msg):
        glb_logger.info(msg)
        print(msg)

    def log_error(msg):
        glb_logger.error(msg)
        print(msg)

    def log_warning(msg):
        glb_logger.warning(msg)
        print(msg)


    def log_proc_start(proc_name):
        msg1 = ' '+ proc_name + ' started at {}'.format(ut_base.curr_time_fmt())
        msg2 = msg1.center (90,'-')
        print(msg2)

        #print a blank line
        glb_logger.info(" ")
        glb_logger.info(msg2)

    def log_proc_end(proc_name):
        msg1 = ' '+ proc_name + ' completed at {}'.format(ut_base.curr_time_fmt())
        msg2 = msg1.center (90,'-')
        print(msg2)
        glb_logger.info(msg2)



    def log_proc_fail(proc_name, reason=None):
        msg1 = ' '+ proc_name + ' failed'
        if reason==None:
            msg1 = ' '+ proc_name + ' failed.'
        else:
            msg1 = ' ' + proc_name + ' at {}'.format(ut_base.curr_time_fmt())+ \
                   ' failed due to ' +reason+'.'

        msg2 = msg1.center (90,'-')
        print(msg2)
        glb_logger.info(msg2)


    def log_module_start(module_name):
        msg1 = ' '+ module_name + ' started at {}'.format(ut_base.curr_time_fmt())
        msg2 = msg1.center (90,'*')
        print(msg2)

        #print a blank line
        glb_logger.info(" ")
        glb_logger.info(msg2)

    def log_module_end(module_name):
        msg1 = ' '+ module_name + ' completed at {}'.format(ut_base.curr_time_fmt())
        msg2 = msg1.center (90,'*')
        print(msg2)
        glb_logger.info(msg2)


    def log_metric(line):
        metric_logger.info(line)

    @staticmethod
    def setup_logger(name, log_file_name, level=logging.INFO, format=format_def):
            """To setup a logger"""

            log_file_path = ut_store.folder_log + log_file_name
            f_handler = logging.FileHandler(log_file_path)
            f_formatter = logging.Formatter(format)
            f_handler.setFormatter(f_formatter)

            logger = logging.getLogger(name)
            logger.setLevel(level)
            logger.addHandler(f_handler)

            return logger
