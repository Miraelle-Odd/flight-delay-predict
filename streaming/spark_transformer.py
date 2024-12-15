from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as func

def addRecordIdToCsv(rawDf):
    return rawDf.withColumn('record_id', func.expr("uuid()"))

def lowercaseAllHeader(rawDf):
    return rawDf.toDF(*[col.lower() for col in rawDf.columns])

def uppercaseAllHeader(rawDf):
    return rawDf.toDF(*[col.upper() for col in rawDf.columns])

def parseJsonFromReadingStream(rawDf, schema=0):
    df = rawDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .select(func.from_json(func.col("value"), schema).alias("data")) \
            .select("data.*")
    return lowercaseAllHeader(df)