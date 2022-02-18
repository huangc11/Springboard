
from database import Database
from customer import Customer
#from bankaccount import create_account
import bankaccount as bkacc
from customer_account import CustomerAccount
import pytest

Database.initialise()



#create_account(p_customer_id, p_account_type='checking', p_balance=0, p_intrs_rate=0.01):
def test_CustomerAccount():
    customer_id = 3
    account_id =990
    result = CustomerAccount.create_association (customer_id, account_id)
   # assert result==None

    customer_id = 3
    account_id = 2
    new_ca = CustomerAccount.create_association(customer_id, account_id)
    assert  new_ca.customer_id == customer_id
    assert   new_ca.account_id ==account_id

def test_list_account_by_customer_id():
    CustomerAccount.list_account_id_by_customer_id(1)





