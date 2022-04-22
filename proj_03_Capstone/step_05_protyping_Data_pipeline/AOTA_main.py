
from ut_log import ut_log
from download_manager import download_manager
from    ETL_Manager import ETL_Manger
from  intgr_Manager import  intgr_Manager




if __name__ =='__main__':
    try:
       download_manager.run()
       ETL_Manger.run()
       intgr_Manager.run()
    except Exception as e:
        ut_log.log_exeption(e)
        ut_log.log_error("AOTA pipleline run failed.")
