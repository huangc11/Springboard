
import requests


class downloader:
    """ A class to represent the databse"""
    tgt_folder ="C:\\demo\\capstone\\raw_data\\"
    const_success = 1
    const_fail = -1



    @classmethod
    def get_file(cls, src_url, tgt_file):

        try:
            file_obtained = requests.get(src_url, stream=True)
            path = cls.tgt_folder + tgt_file
            local_file = open(path, 'wb')
            local_file.write(file_obtained.content)
            local_file.close()
            return (cls.const_success)
        except Exception as e:
            print(e)
            return (cls.const_fail)


if __name__ =='__main__':

    #url_source ='https://sprbcapstonechu01.blob.core.windows.net/flighttime-data/sales_info.csv?sp=r&st=2022-04-02T20:31:38Z&se=2022-04-03T04:31:38Z&sv=2020-08-04&sr=b&sig=fx3M08Knx7%2BiK6p2yhB8Bsoeq2obvTRQu4b7A6yHeSk%3D'


    pass


#initialize connection to database
#Database.initialise()





