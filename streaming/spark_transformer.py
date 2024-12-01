from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as func

def addRecordIdToCsv(rawDf):
    return rawDf.withColumn('record_id', func.expr("uuid()"))