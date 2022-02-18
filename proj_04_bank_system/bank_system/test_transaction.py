
from database import Database
from account_transaction import AccountTransaction
import pytest




def test_create_deposit_trans():
    new_trans=AccountTransaction.create_trans(20701, 'd',30)
    assert new_trans.account_no == 20701
    assert new_trans.type=='d'
    assert new_trans.amount==30


def test_create_withdraw_trans():
    new_trans=AccountTransaction.create_trans(20701, 'w',50)
    assert new_trans.account_no == 20701
    assert new_trans.type=='w'
    assert new_trans.amount==50


def test_create_invalid_trans():
    #set type ='a', which is invalid
    new_trans=AccountTransaction.create_trans(20701, 'a',50)
    assert new_trans == None


