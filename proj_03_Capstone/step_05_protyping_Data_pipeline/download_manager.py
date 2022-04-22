
from ut_log import ut_log
from  downloader import downloader

class download_manager:
    def __init__(self):
        pass

    @staticmethod
    def run():
        ut_log.log_module_start("Download manager execution")
        downloader.download_small_dataset()
        downloader.download_dataset_flight()

        ut_log.log_module_end("Download manager execution")



if __name__ =='__main__':

    download_manager.execute()


