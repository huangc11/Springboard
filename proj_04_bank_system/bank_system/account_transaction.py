import datetime as dt
from sqlalchemy import Column,  INTEGER, VARCHAR, FLOAT, DATE, or_
from sqlalchemy.ext.declarative import declarative_base

from database import Database
from bankaccount import BankAccount

from utility import Utility as ut


Base = declarative_base()

class AccountTransaction(Base):
    ''' A class to represent a bank account transaction

    Attributes
    ----------
    id (int):          transaction id
    account_no (str):   account number
    type (str):         transaction type.  Must be either 'd' for deposit or 'w' for withdrawal
    amount (str):       transaction amount.
    cr_date(date):            transaction date
    '''

    __tablename__ = 'acc_transaction'

    id = Column(INTEGER, primary_key=True)
    account_no = Column(INTEGER)
    type = Column(VARCHAR(30), nullable=False)
    amount = Column(FLOAT)
    cr_date = Column(DATE)

    def __init__(self, account_no, type, amount):
        self.account_no=account_no
        self.type=type
        self.amount=amount
        self.cr_date= dt.datetime.today()

    @staticmethod
    def create_trans(p_account_no, p_trans_type, p_amount):
        if p_trans_type not in ('d','w'):
            return None

        o_trans = AccountTransaction(account_no=p_account_no, type=p_trans_type, amount=p_amount)
        new_trans = Database.new_rec_in_db(o_trans)
        return new_trans


if __name__ =="__main__" :

   pass