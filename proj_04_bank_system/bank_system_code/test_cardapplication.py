
from database import Database
from card_application import CardApplication


import pytest


#create_account(p_customer_id, p_account_type='checking', p_balance=0, p_intrs_rate=0.01):
def test2_create_single_appl():
   CardApplication.create_app(1)

def test2_create_mul_appl():
    CardApplication.create_app(2)
    CardApplication.create_app(3)
    CardApplication.create_app(5)
    CardApplication.create_app(4)
    CardApplication.create_app(6)

