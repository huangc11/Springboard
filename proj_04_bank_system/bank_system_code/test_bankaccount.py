
from database import Database
from customer import Customer
#from bankaccount import create_account
import bankaccount as bkacc
import pytest


Database.initialise()



#create_account(p_customer_id, p_account_type='checking', p_balance=0, p_intrs_rate=0.01):
def test_create_account():
    pass

    '''

    result = bkacc.create_account(2, 0)
    assert  result == False

    result = bkacc.create_account(3456, 'checking',300)
    assert result ==True
    
    '''

def test_get_prefix():
   # assert bkacc.BankAccount.get_prefix('c') ==20000
  #  assert bkacc.BankAccount.get_prefix('s') ==30000
  pass

def test_get_by_account_no():
    assert  bkacc.BankAccount.get_by_account_no(3456)==None
    assert  bkacc.BankAccount.get_by_account_no(1)==None


def test_get_by_account_id():
    result =bkacc.BankAccount.get_by_account_id(1950)
    print(result)
    assert  result==None

def test_savings_account_init():
    s_acc = bkacc.SavingsAccount(300,0.01)

    assert  s_acc.balance ==300
    assert  s_acc.account_type =='savings'
    assert  s_acc.intrst_rate ==0.01
    assert  s_acc.account_no>30000



    #new_acc=s_acc.insert_to_db()
    #assert new_acc!=None

def test_checking_account_init():
    s_acc = bkacc.CheckingAccount(200)
    assert  s_acc.balance ==200
    assert  s_acc.account_type =='checking'
    assert   s_acc.account_no<30000 and s_acc.account_no>20000

    #new_acc=s_acc.insert_to_db()
    #assert new_acc!=None

def test_create_savings():
    new_acc=bkacc.SavingsAccount.create_account(303,0.08)
    assert  new_acc.balance ==303
    assert new_acc.intrst_rate<=bkacc.SavingsAccount._max_intrst_rate


def test_create_checkings():
    new_acc=bkacc.CheckingAccount.create_account(304)
    assert  new_acc.balance ==304

