from pyspark.sql import functions as f

from ut_base import ut_base as ut_base
from ut_spark import ut_spark
from ut_store import ut_store
from ut_log import ut_log
from ETLProcessor import ETLProcessor


class ETLProcessorOfPlane(ETLProcessor):
    def Transform(self, df1):
        df2 = df1.filter(f.col('manufacturer').isNotNull())
        return df2

