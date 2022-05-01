from downloader import Downloader
class download_airport:
    @staticmethod
    def run():
        list1 = \
            [
                {'tgt_file': 'airport.csv',
                 'url': 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/XTPZZY'
                 }
            ]

        l =list1[0]
        d_1= Downloader(l['url'], l['tgt_file'])
        d_1.get_file()



if __name__ =="__main__":
    download_airport.run()


