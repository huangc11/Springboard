
import requests


from ut_store import ut_store
from ut_log import ut_log


class Downloader:
    """ A utility  to download a file from a given source url to a target location"""
    tgt_folder = ut_store.folder_raw
    c_const_success = 1
    c_const_fail = -1

    def __init__(self, src_url, target_file, target_folder=ut_store.folder_raw):
        self.src_url= src_url
        self.target_file = target_file
        self.target_folder= target_folder

    def get_file(self):
        src_url = self.src_url
        tgt_file = self.target_file
        tgt_folder = self.target_folder

        try:

            ut_log.log_info("Downloading {} from {}...".format(tgt_file, src_url))
            file_obtained = requests.get(src_url, stream=True)
            path = tgt_folder + tgt_file
            local_file = open(path, 'wb')
            local_file.write(file_obtained.content)
            local_file.close()

            ut_log.log_info('Downloading completed.')
            return (Downloader.c_const_success)

        except Exception as e:
            ut_log.log_exeption(e)
            ut_log.log_error('Downloading failed.')
            return (Downloader.c_const_fail)

