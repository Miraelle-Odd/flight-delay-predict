from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as func
from spark_transfromer import *

def createSparkSession(appName):
    return SparkSession.builder.appName(appName).getOrCreate()

def readCsvData(sparkSession, filePath='Final_Data/kg-fightdelay-dataset/T_ONTIME_REPORTING_2021_M1.csv'):
    return sparkSession.read.format('csv').\
        option('inferSchema','true').\
        option('header', 'true').\
        option('path', filePath).\
        load()

def createCassSession(appName, hostIp="127.0.0.1" ):
    return SparkSession.builder.\
        appName(appName).\
        config("spark.cassandra.connection.host", hostIp).\
        getOrCreate()

def writeToCass(dataset, table, keyspace='testframe'):
    dataset.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table=table, keyspace=keyspace)\
        .save()

sparkSession = createSparkSession("FlightData")
data = readCsvData(sparkSession)

data.printSchema()