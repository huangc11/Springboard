
import time
import contextlib


class ut_spr:
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


if __name__=='__main__':
  with ut_spr.timer():
    print('hello')


