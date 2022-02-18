from database import Database
from creditcard import CreditCard

import pytest

def test_generate_creditcard():
   new_card=CreditCard.generate_a_new_card( customer_id=1, cardholder_name='Tom')
   assert  new_card!=None
