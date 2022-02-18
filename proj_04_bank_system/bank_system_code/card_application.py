
from sqlalchemy import Column,  INTEGER, VARCHAR, FLOAT, DATE, DATETIME, or_
from sqlalchemy.ext.declarative import declarative_base

from utility import Utility as ut
from sequence import Sequence as Seq
from database import Database
import datetime as dt


Base = declarative_base()



class CardApplication(Base):
    ''' A class to represent a bank account transaction'''

    __tablename__ = 'card_application'


    id = Column(INTEGER, primary_key=True)
    customer_id = Column(INTEGER)
    status = Column(VARCHAR)
    note =  Column(VARCHAR)
    apply_date= Column(DATE)
    apprv_date = Column(DATE)
    employee_id = Column(INTEGER)
    card_id = Column(INTEGER)

    def __init__(self, customer_id):
        self.customer_id = customer_id
        self.apply_date =  dt.datetime.today()


    def insert_into_db(self):
        """insert a new credit card record into database"""
        new_rec= Database.new_rec_in_db(self)
        # db creaton succeeds
        return new_rec


    def update_at_db(self, dict_for_change ):
        Database.update_rec_in_db(CardApplication, self.id, dict_for_change)

    @staticmethod
    def create_appl(customer_id):
        app = CardApplication(customer_id)
        new_app = app.insert_into_db()
        return new_app


if __name__ =="__main__" :
    #new_app=CardApplication.create_appl(customer_id=1)
    pass

