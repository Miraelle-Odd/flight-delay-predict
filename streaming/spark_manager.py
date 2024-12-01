from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as func
from spark_transformer import *
import os
import pandas as pd

# File paths
spk_manager_dir = os.path.dirname(__file__)
DATA_FILE_PATH = os.path.join(spk_manager_dir, "..", "Final_Data/kg-fightdelay-dataset", "T_ONTIME_REPORTING_2021_M1.csv")
SPARK_TEMP_FOLDER = os.path.join(spk_manager_dir, 'spark-temp')

CASS_HOST_IP = '127.0.0.1'
CASS_PORT = '9042'
CASS_USERNAME = 'cassandra'
CASS_PASSWORD = 'cassandra'

def createSparkSession(appName):
    return SparkSession.builder.appName(appName).getOrCreate()

def readCsvData(sparkSession, filePath=DATA_FILE_PATH):
    return sparkSession.read.format('csv').\
        option('inferSchema','true').\
        option('header', 'true').\
        option('path', filePath).\
        load()

def createCassSession(appName):
    try:
        print('--- Connecting to Cassandra ---')
        cassSession = SparkSession.builder.\
            appName(appName)\
            .config("spark.cassandra.connection.host", CASS_HOST_IP)\
            .config("spark.cassandra.connection.port", CASS_PORT)\
            .config("spark.cassandra.auth.username", CASS_USERNAME)\
            .config("spark.cassandra.auth.password", CASS_PASSWORD)\
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0") \
            .config("spark.local.dir", SPARK_TEMP_FOLDER) \
            .getOrCreate()
        print('--- Connection to Cassandra created successfully! ---')
        return cassSession
    except Exception as e:
        print('-- Fail to create Cassandra connection with error: --', e)

def writeToCass(dataset, table, keyspace='testframe'):
    try:
        print(f'-- Start writing to table {table} in Cassandra --')
        dataset.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table=table, keyspace=keyspace)\
            .save()
        print(f'-- Completed Writing to table {table} in Cassandra --')
    except Exception as e:
        print('-- Writing to Cassandra fail with error: --', e)

def readFromCass(cassSession, table, keyspace='testframe'):
    try:
        print(f'-- Start reading table {table} from Cassandra --')
        df = cassSession.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=table, keyspace=keyspace) \
            .load()
        print(f'--- Reading table {table} from Cassandra successfully! ---')
        return df
    except Exception as e:
        print('-- Reading from Cassandra fail with error: --', e)
        return
    
def testable():
    print('--Test Starting--')
    cassSession = createCassSession("FlightData")    
    
    data = [(6, 28, "IO1"), (7, 32, "NA1")]
    columns = ["id", "age", "name"]
    print('--We will be writing these following data to cassandra and show it--')
    print(data,columns)
    
    new_df = cassSession.createDataFrame(data, columns)
    writeToCass(new_df, 'testable')
    
    df = readFromCass(cassSession, 'testable')
    df.printSchema()
    df.show()
    
    cassSession.stop()
    print('--Test Finish--')
    
def spark_manager():
    cassSession = createCassSession("FlightData")
    data = readCsvData(cassSession)
    
    data.printSchema()
    data.limit(2).show()    
    
    # data.createOrReplaceTempView('flights_temp')
    # print(cassSession.catalog.listTables())
    cassSession.stop()

if __name__ == "__main__":
    try:
        # testable()     
        spark_manager()
                
    except Exception as e:
        print('-- General exception caught: --', e)