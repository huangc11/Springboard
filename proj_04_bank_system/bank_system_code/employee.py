
from sqlalchemy import Column,  INTEGER, VARCHAR, FLOAT, DATETIME, or_
from sqlalchemy.ext.declarative import declarative_base

from utility import Utility as ut
from sequence import Sequence as Seq
from database import Database


Base = declarative_base()

class Employee(Base):
    ''' A class to represent a bank account transaction'''

    __tablename__ = 'employee'

    id = Column(INTEGER, primary_key=True)
    name  = Column(VARCHAR(60))
    login = Column(VARCHAR(20))
    password =  Column(VARCHAR(20))


    __pwd_key =43989

    def __init__(self, name):
        self.name=name

        tmpseq = Seq.next()
        self.login = 'a'+str(tmpseq).rjust(4,'0')
        self.password = str(tmpseq)


    def __repr__(self):
        return("Employee(id={},name='{}',login='{}')".format(self.id, self.name, self.login))
        #return ("Employee({id}, '{name}', '{login}'".format(self.id, self.name, self.login))


    def insert_into_db(self):
        """insert a new account record into database"""
        new_emp= Database.new_rec_in_db(self)
        return new_emp

    def update_passwd(self,  password ):
        self.password = password
        Database.update_rec_in_db(Employee, self.id, {'password':self.password})


    @staticmethod
    def get_by_id(p_id):
        """Get the employee  record from database by id. Returns employee object (if found) or None (not found)
        """
        session = Database.get_session()
        rec = session.query(Employee).filter(Employee.id == p_id).scalar()
        return rec

    def get_by_login(p_login):
        """Get the employee  record from database by id. Returns employee object (if found) or None (not found)
        """
        session = Database.get_session()
        rec = session.query(Employee).filter(Employee.login == p_login).scalar()
        return rec

    @staticmethod
    def new_emp(emp_name):
        tmp_emp =Employee(emp_name)
        new_emp = tmp_emp.insert_into_db()
        return new_emp

if __name__ =="__main__" :


    emp = Employee.get_by_login('a0733')
    emp.update_passwd('844')






