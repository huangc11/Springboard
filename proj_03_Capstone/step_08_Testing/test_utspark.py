from ut_spark import ut_spark
from pyspark.sql.functions import col
import pytest
import chispa

def test_get_sparksession():

    spark = ut_spark.get_sparksession()
    assert spark!=None

def test_df_read_from_csv():
    df=ut_spark.df_read_from_csv('C:/demo/testaota/data1.txt')
    assert df.filter(col('lastname') == 'tom').count() == 1

def test_write_read_parquet():

    df1=ut_spark.df_read_from_csv('C:/demo/testaota/data1.txt')

    ut_spark.df_write_to_file(df1, 'C:/demo/testaota/data2', 'parquet')

    df2=ut_spark.df_read_from_parquet('C:/demo/testaota/data2')
    chispa.assert_df_equality(df1, df2)

    #df_diff = ut_spark.df_read_from_csv('C:/demo/testaota/data_diff.txt')