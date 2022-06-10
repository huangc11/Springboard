from pyspark.sql import functions as f

def disp_config(spark):
    app_name = spark.conf.get('spark.app.name')

    # Driver TCP port
    driver_tcp_port = spark.conf.get('spark.driver.port')


    # Number of join partitions
    num_partitions = spark.conf.get('spark.sql.shuffle.partitions')

    # Show the results
    print("Name: %s" % app_name)
    print("Driver TCP port: %s" % driver_tcp_port)
    print("Number of partitions: %s" %num_partitions)


def df_write_parquet_orw(df, path):
    df.write.format("parquet").mode("overwrite").option("path", path).save()


def get_par(spark):
    return spark.conf.get("spark.sql.shuffle.partitions")


from ut_spark  import ut_spark
def get_part(df_fl):
    ut_spark.show_partition_count(df_fl)
    ut_spark.get_partition_num(df_fl)

def show_partition_count(df):
        df.groupBy(f.spark_partition_id()).count().show()