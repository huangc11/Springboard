

from  downloader import downloader

if __name__ =='__main__':
    def download_file(url, tgt_file):
        result = downloader.get_file(url, tgt_file)

        if result == downloader.const_success:
            print('Downloading {} succeeded.'.format(tgt_file))
        else:
            print('Downloading {} fail.'.format(tgt_file))

    #url_source ='https://sprbcapstonechu01.blob.core.windows.net/flighttime-data/sales_info.csv?sp=r&st=2022-04-02T20:31:38Z&se=2022-04-03T04:31:38Z&sv=2020-08-04&sr=b&sig=fx3M08Knx7%2BiK6p2yhB8Bsoeq2obvTRQu4b7A6yHeSk%3D'
    file ='2008.csv.bz2'
    url ='https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/EIR0RA'
    #downloader.get_file(url, file)

    file='airports.csv'
    url='https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/XTPZZY'


    file= 'carriers.csv'
    url='https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/3NOQ6Q'
    #download_file(url, file)

    file='plane-data.csv'
    url='https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/XXSL8A'
    download_file(url, file)
