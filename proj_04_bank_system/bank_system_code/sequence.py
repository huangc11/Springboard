from sqlalchemy import Column,  INTEGER, func
from sqlalchemy.ext.declarative import declarative_base

from database import Database
from utility import Utility as ut

Base = declarative_base()


class Sequence(Base):
    ''' A class to represent a sequence generator'''

    __tablename__ = 'sequence'

    id = Column(INTEGER, primary_key=True)

    def __init__(self):
        note =' '

    @staticmethod
    def curr():
        """return the current max value of the sequence"""
        try:
            session = Database.get_session()
            id_curr = session.query(func.max(Sequence.id)).scalar()
            return id_curr
        except Exception as e:
            ut.log_info(e)
            return None

    @staticmethod
    def next():
        """return a new value of sequence"""
        seq=Sequence()
        new_seq=Database.new_rec_in_db(seq)
        if new_seq!=None:
           return new_seq.id
        else:
            return None



