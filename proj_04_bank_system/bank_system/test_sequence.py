
from database import Database
from sequence  import Sequence
import pytest


#Database.initialise()

def test_next():

    curr = Sequence.curr()
    assert Sequence.next() == curr +1


