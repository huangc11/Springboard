
from database import Database
from employee import Employee
import pytest




#create_account(p_customer_id, p_account_type='checking', p_balance=0, p_intrs_rate=0.01):

def test_new_employee():

    emp_name = 'Henry'
    new_emp = Employee.new_emp(emp_name)

    assert  new_emp.name ==emp_name
    assert  new_emp.password !=None


def te2st_password():

    new_password = 789

    emp = Employee.get_by_id(14)
    emp.set_password(new_password)
    emp.update_in_db()
    emp = Employee.get_by_id(14)
    print(emp.password)
    assert emp.password==new_password

def te2st_get_by_login():
    emp= Employee.get_by_login('a0729')
    assert emp.name =='Henry'

