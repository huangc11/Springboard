
import requests

from ut_base import ut_base as ut_base
from ut_store import ut_store
from ut_log import ut_log



class downloader:
    """ A class to download file"""
    tgt_folder = ut_store.folder_raw
    const_success = 1
    const_fail = -1

    flight_dfiles = [
        {"year": "2001",
         "filename": "2001.csv.bz2",
         "url": "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/CI5CEM"
         },
        {"year": "2002",
         "filename": "2002.csv.bz2",
         "url": "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/OWJXH3"
         },

        {"year": "2008",
         "filename": "2008.csv.bz2",
         "url": "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/EIR0RA"
         }
    ]

    @classmethod
    def get_file(cls, src_url, tgt_file):

        try:
            ut_log.log_info("Downloading {} from {}...".format(tgt_file,src_url))
            file_obtained = requests.get(src_url, stream=True)
            path = cls.tgt_folder + tgt_file
            local_file = open(path, 'wb')
            local_file.write(file_obtained.content)
            local_file.close()
            return (cls.const_success)
        except Exception as e:
            print(e)
            return (cls.const_fail)


    @staticmethod
    def download_file(url, tgt_file):
        result = downloader.get_file(url, tgt_file)

        if result == downloader.const_success:
            ut_log.log_info('Downloading {} completed.'.format(tgt_file))
        else:
            ut_log.log_info('Downloading {} failed.'.format(tgt_file))

    @classmethod
    def download_small_dataset(cls):

        ut_log.log_proc_start('Downloading-small-datasets')

        file='airport.csv'
        url='https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/XTPZZY'
        cls.download_file(url, file)

        file= 'carrier.csv'
        url='https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/3NOQ6Q'
        cls.download_file(url, file)

        file='plane.csv'
        url='https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/XXSL8A'
        cls.download_file(url, file)

        ut_log.log_proc_end('Downloading-small-datasets')

    @classmethod
    def download_dataset_flight(cls):

        ut_log.log_proc_start('Downloading-dataset_flight')

        for fi in cls.flight_dfiles:
                cls.download_file(fi["url"], fi["filename"])

        ut_log.log_proc_end('Downloading-dataset_flight')


if __name__ =='__main__':
    downloader.download_dataset_flight()

