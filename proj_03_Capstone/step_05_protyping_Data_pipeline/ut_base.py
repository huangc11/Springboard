
import time
import contextlib

from datetime import  datetime


from pathlib import Path

class ut_base:
  const_succ = 1
  const_fail = -1
  date_format = "%m/%d/%Y-%H:%M:%S"
  F_FAIL =-1

  @staticmethod
  def print_list(list):
    [print(l) for l in list]

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

  def print_dict_val(p_dict):
    line1 = ''
    for key in p_dict:
      line1 = line1 + '{},'.format(p_dict[key])
    print(line1)

  @staticmethod
  def curr_time():
    return datetime.now()

  @staticmethod
  def curr_date():
    return datetime.today()

  @staticmethod
  def curr_time_str():
    return ut_base.time_to_str(datetime.now(),"%Y%m%d_%H%M%S")

  @staticmethod
  def curr_time_fmt():
    return ut_base.time_to_str(datetime.now(),"%Y-%m-%d_%H:%M:%S")


  @staticmethod
  def time_to_str(p_datetime, p_dt_format=None):
    if p_dt_format ==None:
       p_dt_format =ut_base.date_format


    dt_str = p_datetime.strftime(p_dt_format)
    return dt_str

  @staticmethod
  def str_to_time(p_dt_string, p_dt_format=None):
    if p_dt_format ==None:
       p_dt_format =ut_base.date_format
    return datetime.strptime(p_dt_string, p_dt_format)

  @staticmethod
  def print_dict_v(p_dict):
        line1=''
        for key in p_dict:
            line1=line1+'{},'.format(p_dict[key])
        print(line1)

  def print_dict(p_dict):
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

