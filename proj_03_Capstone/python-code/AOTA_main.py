
from ut_log import ut_log

from downloadManager import downloadManager
from    ETLManager import ETLManger
from  integrationManager import  integrationManager



if __name__ =='__main__':
    try:
       downloadManager.run()
       ETLManger.run()
       integrationManager.run()

    except Exception as e:
        ut_log.log_exeption(e)
        ut_log.log_error("AOTA pipleline run failed.")
