
from database import Database
from sequence import Sequence
import pytest

def test_connection():
    Database.initialise()
    session = Database.get_session()
    assert session!=None


    import pytest

    # Database.initialise()

def test_seq_currt():
        assert Sequence.curr()!=None
