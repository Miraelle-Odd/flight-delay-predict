from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as func

def addRecordIdToCsv(rawDf):
    return rawDf.withColumn('record_id', func.expr("uuid()"))

def lowercaseAllHeader(rawDf):
    return rawDf.toDF(*[col.lower() for col in rawDf.columns])