from sparkconnector import sparkConnector
import pytest



def test_get_spark():

    sparkcon = sparkConnector()
    spark = sparkcon.get_spark()
    assert spark!=None

