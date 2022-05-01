from downloader import Downloader
from downloaderOfFlight import downloaderOfFlight
from ut_log import ut_log


class downloadManager:
    '''The main process to download flight dataset and other  datasets.'''

    @classmethod
    def run(cls):
        '''The main process to download flight dataset and other  datasets'''

        ut_log.log_module_start('downloadManager')

        # Download airport, carrier, plane dataset

        list1 = \
            [
                {'tgt_file': 'airport.csv',
                 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/XTPZZY'
                 },
                {'tgt_file': 'carrier.csv',
                 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/3NOQ6Q'
                 },
                {'tgt_file': 'plane.csv',
                 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/XXSL8A'
                 }
            ]
    
        for l in list1:
            ut_log.log_proc_start('downloading {}'.format(l['tgt_file']))

            downloader = Downloader(l['url'], l['tgt_file'])
            downloader.get_file()

        # Download flight datasets

        list2 = ['1997','1998', '1999', '2000', '2001', '2002', '2003','2004', '2005']

        for year in list2:
            ut_log.log_proc_start('downloading flight dataset -- year of {}'.format(year))

            fd_1 = downloaderOfFlight(year)
            fd_1.download()


        ut_log.log_module_end('downloadManager')


if __name__ == '__main__':
    downloadManager.run()





