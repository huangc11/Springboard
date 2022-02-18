
from database import Database
from manager_transaction import  TransactionManager
import pytest



def  test_deposit_to_account():
    # TransactionManager.withdraw_from_account(20172,30)
    TransactionManager.deposit_to_account(20701, 400)

def  test_withdraw_from_account():
    # TransactionManager.withdraw_from_account(20172,30)
    TransactionManager.withdraw_from_account(20701, 300)


def  test_withdraw_from_account_2():
    # TransactionManager.withdraw_from_account(20172,30)
    TransactionManager.withdraw_from_account(20701, 500)
