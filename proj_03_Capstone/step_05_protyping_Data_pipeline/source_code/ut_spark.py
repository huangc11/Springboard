from pathlib import Path

from ut_log import ut_log
from  ut_store import ut_store

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
from pyspark.sql import functions as f
#from pyspark.sql.functions import spark_partition_id


from pyspark.sql.types import \
        StructType,\
        StringType, \
        IntegerType, \
        DateType, \
        TimestampType,\
        FloatType,\
        BooleanType

class ut_spark:
    """ a class of utitlies related to Spark, including tools to:
    1. get spark session
    2. read from/write to files
    3. translate atrribute list to spark schema



    """

    c_succ = True
    c_fail = False
    c_spark_sql_shuffle_partitions = ut_store.sparksql_shuffle_partition_num

    __spark =None

    @staticmethod
    def isFile(f_path):
        # Checking if a file exists with Pathlib
        file_path = Path(f_path)
        return file_path.is_file()

    @classmethod
    def log1_disp_error(cls, msg):
        print(msg)



    '''***********************************************************************

    Create Spark / Get spark session /Init

    ***********************************************************************'''

    @classmethod
    def creat_spark(cls):
        '''Create and return a spark session'''

        try:
            spark = SparkSession. \
                builder. \
                appName("Aggregation"). \
                master("local[4]"). \
                getOrCreate()

            spark.conf.set("spark.sql.shuffle.partitions", cls.c_spark_sql_shuffle_partitions)

            return spark

        except Exception as e:
            ut_log.log_exeption(e)
            return None


    @staticmethod
    def get_sparksession():
        if ut_spark.__spark==None:
            ut_spark.__spark=ut_spark.creat_spark()



        return ut_spark.__spark

    @classmethod
    def  initialise(cls):
        if cls.__spark == None:
         ut_spark.__spark = ut_spark.creat_spark()




    '''***********************************************************************
    
    Read from /Write to files
    
    ***********************************************************************'''

    @classmethod
    def df_read_from_csv(cls, file_path, schema=None):
        ''' Create a dataframe from a csv file,with header, with/without schema'''

        msg_fail = 'Spark-read-from-csv2 of {} failed --File does not exist!'.format((file_path))

        if cls.isFile(file_path) != True:
            msg_fail_on_file = 'Spark-read-from-csv1 of {} failed --File does not exist!'.format((file_path))
            ut_log.log_error(msg_fail_on_file)
            return None

        try:
            spark = cls.get_sparksession()
            if schema==None:
                  try:
                      return spark.read\
                       .option("header","true") \
                        .option("inferSchema","true") \
                        .csv(file_path)
                  except Exception as e:
                      ut_log.log_error(msg_fail)
                      ut_log.log_exeption(e)
                      return None
            else:
                  try:
                      return spark.read \
                          .option("header", "true") \
                          .schema(schema) \
                          .csv(file_path)
                  except:
                      ut_log.log_error(msg_fail)
                      return None
        except Exception as e:
            ut_log.log_error(msg_fail)
            ut_log.log_exeption(e)
            return None



    @classmethod
    def df_read_from_parquet(cls, file_path):

       try:
          spark = ut_spark.get_sparksession()
          df=  spark.read\
                .option("path", file_path)\
                .option("format", "parquet")\
                .load()
          return df
       except Exception as e:
           ut_log.log_exeption(e)
           return None



    @staticmethod
    def df_write_to_file(df, file_path, format="parquet"):

        msg_write_fail = "Writing file (file={}, format={}) failed.".format(file_path, format)

        if format not in ("csv","parquet","json"):
            ut_spark.log_disp_error("Invalide format. Action failed.")
            return ut_spark.c_fail

        try:
            df.write \
                .format(format) \
                .mode("overwrite") \
                .option("path", file_path) \
                .save()

            return ut_spark.c_succ
        except Exception as e:
            print(e)
            ut_log.log_error(msg_write_fail)
            ut_log.log_exeption(e)
            return ut_spark.c_fail


    '''***********************************************************************
    
    Schema  Translation
    
    ***********************************************************************'''


    @classmethod
    def list_to_schema(cls, fieldlist):
        def get_datatype(field_type):
            f_type = field_type[0:3]
            if f_type == 'str':
                return StringType()
            elif f_type == 'int':
                return IntegerType()
            elif f_type == "dat":
                return DateType()
            elif f_type == "boo":
                return BooleanType()
            elif f_type == "flo":
                return FloatType()
            else:
                return StringType()

        structSchema = StructType()

        for fieldTuple in fieldlist:
            structSchema.add(fieldTuple[0], get_datatype(fieldTuple[1]), True)

        return (structSchema)


    '''***********************************************************************
    
    Other tools
    ***********************************************************************'''

    @staticmethod
    def df_cr_tempview(df, table_name):
        try:
            df.createOrReplaceTempView(table_name)
        except Exception as e:
            ut_log.log_exeption(e)
            ut_log.log_warning(('Dataframe ({}) temp view creation failed.').format(df))



    @staticmethod
    def show_partition_count(df):
        df.groupBy(f.spark_partition_id()).count().show()

    @staticmethod
    def add_col(df, new_col, exp):
        '''Add a  monotonically increasing id column to a dataframe'''
        df1 = df.withColumn(new_col, exp)
        return df1

    @staticmethod
    def add_mono_incr_id(df, new_col):
        '''Add a  monotonically increasing id column to a dataframe'''
        df1 = df.withColumn(new_col, f.monotonically_increasing_id())
        return df1


    @staticmethod
    def df_dup_by_col(df, colname):
        return df.groupBy(col(colname)).agg(count(colname).alias("count")).filter(col("count") > 1)


    @staticmethod
    def get_partition_num(df):
        ''' Get number of partitions of a dataframe
        '''
        try:
            n = df.rdd.getNumPartitions()
            return n
        except Exception as e:
            ut_log.log_exeption(e)
            return None


if __name__ == "__main__":
    pass


