
from database import Database
from manager_creditcard import CreditCardManager
import pytest




#create_account(p_customer_id, p_account_type='checking', p_balance=0, p_intrs_rate=0.01):

def test_get_total_savings_by_customer_id():

    b1 =CreditCardManager.get_total_savings_by_customer_id(1)
    assert b1==2000
