
from sqlalchemy import Column,  INTEGER, VARCHAR, FLOAT, DATE, DATETIME, or_
from sqlalchemy.ext.declarative import declarative_base

from utility import Utility as ut
from sequence import Sequence as Seq
from database import Database
from bank import Bank

import datetime as dt


Base = declarative_base()



class CreditCard(Base):
    ''' A class to represent a credit card'''

    __tablename__ = 'creditcard'

    id = Column(INTEGER, primary_key=True)
    card_no = Column(INTEGER)
    bank_name = Column(VARCHAR(60))
    exp_date =  Column(DATE)
    customer_id = Column(INTEGER)
    cardholder_name  = Column(VARCHAR(60))
    credit_limit = Column(FLOAT)
    employee_id =Column(INTEGER)
    cr_date = Column(DATE)



    def __init__(self, customer_id, cardholder_name=None, employee_id=None, exp_date=None, credit_limit=1000 ):

        #exp_date =
        tmpseq = Seq.next()
        self.card_no= tmpseq + 88000000
        self.bank_name = Bank.get_name()
        self.cr_date= dt.datetime.today()
        self.credit_limit = credit_limit
        self.employee_id= employee_id
        self.customer_id = customer_id
        self.cardholder_name = cardholder_name
        if exp_date ==None:
            curr_date = dt.datetime.today()
            self.exp_date = curr_date.replace(curr_date.year +1)

    def __repr__(self):
        return("Credit card(id={},card_no='{}', cardholder_name={}, bank_name='{}', exp_date='{}', customer_id='{}', employee_id={})".
               format(self.id, self.card_no, self.cardholder_name, self.bank_name,self.exp_date,self.customer_id, self.employee_id))
        #return ("Employee({id}, '{name}', '{login}'".format(self.id, self.name, self.login))

    def insert_into_db(self):
        """insert a new credit card record into database"""
        new_card= Database.new_rec_in_db(self)
        # db creaton succeeds
        if new_card!=None:
            ut.log_info("A new credit card has been created.")
        else:
            ut.log_info("The credit card creation faild due to low level errors.")

        return new_card

    @staticmethod
    def generate_a_new_card(customer_id, cardholder_name=None, employee_id=None, exp_date=None,credit_limit=1000):
        card = CreditCard(customer_id=customer_id,
                          cardholder_name=cardholder_name,
                          employee_id=employee_id,
                          exp_date=exp_date,
                          credit_limit=credit_limit)
        new_card = card.insert_into_db()
        return new_card





if __name__ =="__main__" :

    new_card=CreditCard.generate_a_new_card(customer_id=2, cardholder_name='Tom')
    print(new_card)


