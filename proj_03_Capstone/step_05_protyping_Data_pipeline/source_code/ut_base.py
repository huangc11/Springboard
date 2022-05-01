
import time
import contextlib

from datetime import  datetime


from pathlib import Path

class ut_base:
    const_succ = 1
    const_fail = -1
    date_format = "%m/%d/%Y-%H:%M:%S"
    F_FAIL = -1

    '''****************************************************************
            Time/Date related methods
    ********************************************************************'''

    @staticmethod
    def curr_time():
      return datetime.now()

    @staticmethod
    def curr_date():
      return datetime.today()

    @staticmethod
    def curr_time_str():
      ''' Get a string out of current timestamp. Format: 20220321_120301'''
      return ut_base.time_to_str(datetime.now(),"%Y%m%d_%H%M%S")

    @staticmethod
    def curr_time_fmt():
      ''' Get a string out of current timestamp. Format: 2022-03-21_12:03:01'''

      return ut_base.time_to_str(datetime.now(),"%Y-%m-%d_%H:%M:%S")


    @staticmethod
    def time_to_str(p_datetime, p_dt_format=None):
      '''To conver a timestamp to a string.  Format can vary or be empty.'''
      if p_dt_format ==None:
         p_dt_format = ut_base.date_format

      return p_datetime.strftime(p_dt_format)

    @staticmethod
    def str_to_time(p_dt_string, p_dt_format=None):
      '''To convert a string to a timestamp .  Format can vary or be empty.'''
      if p_dt_format ==None:
         p_dt_format =ut_base.date_format
      return datetime.strptime(p_dt_string, p_dt_format)



    @staticmethod
    @contextlib.contextmanager
    def timer():
      """Time the execution of a context block.

      Yields:
        None
      """
      start = time.time()
      # Send control back to the context block
      yield
      end = time.time()
      #print('Elapsed: {:.2f}s'.format(end - start))
      print('Elapsed:%6f'%(end - start))


    '''****************************************************************
            Methods to pring dict, list and check file path
    ********************************************************************'''

    @staticmethod
    def isFile(f_path):
      # Checking if a file exists with Pathlib
      file_path = Path(f_path)
      return file_path.is_file()


    def print_dict(p_dict):
      line1 = ''
      for key in p_dict:
        line1 = line1 + '{}:{},'.format(key, p_dict[key])

      print(line1)

    def print_dict_val(p_dict:dict):
      line1 = ''
      for key in p_dict:
        line1 = line1 + '{},'.format(p_dict[key])
      print(line1)



    @staticmethod
    def print_list(list:list):
      '''Print a list object'''
      [print(l) for l in list]


    @staticmethod
    def print_dict_v(p_dict):
          '''Print  all the values of a dict object'''
          line1=''
          for key in p_dict:
              line1=line1+'{},'.format(p_dict[key])
          print(line1)

    def print_dict(p_dict):
          '''Print out all the keys of a dict object '''
          line1=''
          for key in p_dict:
              line1=line1+'{}:{},'.format(key,p_dict[key])

if __name__=='__main__':


  dt_str='04/17/2022-10:04:21'
  dt1 = ut_base.str_to_time(dt_str)

  dt_str1= ut_base.time_to_str(dt1)
 # print(dt_str1)
 # print(ut_base.)



  print(ut_base.curr_time_str())

