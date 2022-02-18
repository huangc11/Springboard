import datetime as dt
from sqlalchemy import Column,  INTEGER, VARCHAR, FLOAT, DATETIME, or_, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base

from utility import Utility as ut
from database import Database
from customer import Customer
import bankaccount as m_ba

Base = declarative_base()



class CustomerAccount(Base):
    ''' A class to represent a bank account transaction'''

    __tablename__ = 'ass_customer_account'

    id = Column(INTEGER, primary_key=True)
    customer_id  = Column(INTEGER)
    account_id = Column(INTEGER)
    cr_datetime = Column(DATETIME)


#recipe_id = Column(Integer, ForeignKey('recipes.id'))
    def __init__(self, customer_id , account_id):
        self.account_id=account_id
        self.customer_id = customer_id
        self.cr_datetime= dt.datetime.now()

    def __repr__(self):
        return ('id={}, customer_id={},account_id={}'.format(self.id, self.customer_id, self.account_id))

    def insert_to_db(self):
        new_record = Database.new_rec_in_db(self)
        return new_record

    @staticmethod
    def create_association(customer_id, account_id):
        o_cust_acc = CustomerAccount(customer_id, account_id)
        return Database.new_rec_in_db(o_cust_acc)

    @staticmethod
    def list_account_id_by_customer_id(p_id):
        try:
            session = Database.get_session()
            recs = session.query(CustomerAccount).filter(CustomerAccount.customer_id == p_id).all()
            for rec in recs:
                print(rec.account_id)

        except Exception as e:
            ut.log_exeption(e)
            print(e)
            return None


if __name__ =="__main__" :

    CustomerAccount.create_association(1,2)
   # CustomerAccount.connect_customer_account(customer_id=101,account_id =104)
    #ut.print_success('succ!')
    CustomerAccount.list_account_id_by_customer_id(1)

