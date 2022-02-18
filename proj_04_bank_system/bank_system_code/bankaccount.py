import datetime as dt
from sqlalchemy import Column,  INTEGER, VARCHAR, FLOAT, DATE, or_, func
from sqlalchemy.ext.declarative import declarative_base

from  utility import Utility as ut
from  database import Database
from  sequence import Sequence


Base = declarative_base()

class BankAccount(Base):
    ''' A class to represent a bank account'''

    __tablename__ = 'bankaccount'

    account_id = Column(INTEGER, primary_key=True)
    account_no = Column(INTEGER)
    account_type = Column(VARCHAR(30), nullable=False)
    balance = Column(FLOAT)
    intrst_rate = Column(FLOAT)
    cr_date = Column(DATE)

    def __init__(self, balance):
        self.balance=balance
        self.cr_date= dt.datetime.today()
        self.accno_prefix =80000

      #  self.account_type =''


    def __repr__(self):
        return ("BankAccount(account_id={id}, account_no={acc_no}, account_type='{acc_type}', balance={balance})".
                format(id=self.account_id, acc_no=self.account_no, acc_type=self.account_type, balance=self.balance))

    def withdraw(self, p_amount):
        """ withraw month from this account
          Args:
            p_amount(float): amount to withdraw
        Returns:
            a tuple (a,b)
                 a: amount that being withdrawn
                 b: account's new balance
        """
        if p_amount<=0:
            return (0, self.balance)

        amt = min(self.balance, p_amount)
        self.balance -= amt
        return (amt, self.balance)

    def deposit(self, p_amount):
        """ deposit money to  this account
          Args:
            p_amount(float): amount to deposit. if p_amount<0 set p_amount = 0`
          Returns:
            a tuple (a,b)
                 a: amount that being withdrawn
                 b: account's new balance
        """
        if p_amount>0:
            self.balance += p_amount
            return(p_amount, self.balance)

        else:
            return(0, self.balance)

    def insert_to_db(self):
        """insert a new account record into database
          Returns:
                  object of account if succeed
                  None, if failure
        """
        if self.account_type not in ('savings', 'checking'):
            ut.log_info('Account operation, insert_to_db, failed due to invalid account_type'.format(self.account_type))
            return None

        # create a record in database
        new_account = Database.new_rec_in_db(self)

        return new_account


    @staticmethod
    def get_by_account_no(p_account_no):
        """get the account record in database by account_no; return None if fails
        """
        try:
            session = Database.get_session()
            rec = session.query(BankAccount).\
                    filter(BankAccount.account_no == p_account_no).\
                    scalar()
        except Exception as e:
            return None
        return rec

    @staticmethod
    def get_by_account_id(p_account_id):
        """get the account record in database by id ; return None if fails
        """
        try:
            session = Database.get_session()
            rec = session.query(BankAccount).\
                    filter(BankAccount.account_id == p_account_id).\
                    scalar()
        except Exception as e:
            return None
        return rec



class SavingsAccount(BankAccount):
    """ A class to represent a bank account"""
    _max_intrst_rate = 0.025
    _prefix_of_accno=30000

    def __init__(self, balance=0, p_intrst_rate=0.0):
       BankAccount.__init__(self, balance)
       self.account_type = "savings"
       self.balance = balance

       #
       self.intrst_rate = min(p_intrst_rate,SavingsAccount._max_intrst_rate)


       #generaet account_no,  a 5-digit number whic starts with '3'
       self.account_no = Sequence.next() + SavingsAccount._prefix_of_accno

    def __repr__(self):
        return ("BankAccount(account_id={id}, account_no={acc_no}, account_type='{acc_type}', balance={balance}, intrst_rate={intrst_rate} )".
                format(id=self.account_id, acc_no=self.account_no, acc_type=self.account_type, balance=self.balance, intrst_rate=self.intrst_rate))

    @staticmethod
    def create_account(p_balance,  p_intrs_rate):
        o_account= SavingsAccount(p_balance,  p_intrs_rate)
        new_account=o_account.insert_to_db()
        return new_account

class CheckingAccount(BankAccount):
    ''' A class to represent a bank account'''

    _prefix_of_accno = 20000

    def __init__(self, balance=0):
       BankAccount.__init__(self, balance)
       self.account_type = 'checking'
       self.balance = balance

       # generaet account_no,  a 5-digit number whic starts with '2'
       self.account_no = Sequence.next() +  CheckingAccount._prefix_of_accno

    @staticmethod
    def create_account(p_balance):
        o_account= CheckingAccount(p_balance)
        new_account=o_account.insert_to_db()
        return new_account

    def deposit(self, amount):
        self.balance += amount

if __name__ == '__main__':
    #Database.initialise()


    new_checking=CheckingAccount.create_account(311)
    new_savings =SavingsAccount.create_account(312,0.08)



