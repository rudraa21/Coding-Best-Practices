import os
import sys
#setting path
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(current)
sys.path.append(parent)

# importing necessary functions
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pytest import fixture


@fixture(scope = 'session')
def spark() -> SparkSession:
    return SparkSession.builder.getOrCreate() 

@fixture(scope = 'session')
def dbutils(spark: SparkSession):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns.get("dbutils")
        if not dbutils:
            log.warning("could not initialise dbutils!")
    return dbutils