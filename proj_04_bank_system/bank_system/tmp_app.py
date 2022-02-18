import emp
from database import Database
from emp import Emp

import logging


app_logger= logging.getLogger(__name__)
app_logger.setLevel(logging.INFO)

app_formatter = logging.Formatter('%(asctime)s:%(filename)s:%(name)s:%(levelname)s:%(message)s')

file_handler = logging.FileHandler('app.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(app_formatter)

app_logger.addHandler(file_handler)

logging.basicConfig(filename='app_basic.log', level = logging.DEBUG,
                    format= '%(asctime)s:%(name)s:%(levelname)s:%(message)s')

Database.initialise()
app_logger.info('creating an emp;;;')
logging.info('hello!')
Emp.new_emp('Jerry')