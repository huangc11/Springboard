
from database import Database

from manager_account import  AccountManager
import pytest




#create_account(p_customer_id, p_account_type='checking', p_balance=0, p_intrs_rate=0.01):

def t2est_log():
    AccountManager.log_show_success('this is a success')
    AccountManager.log_show_error('this is a failure')
    AccountManager.log_show_info('this is a message')

def  test_create_savings_account():
    AccountManager.create_account(7,'s',92,.021)

def  test_create_checking_account():
    AccountManager.create_account(7,'c',91)