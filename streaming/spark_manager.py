from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as func
from spark_transformer import *
import os

# File paths
spk_manager_dir = os.path.dirname(__file__)
DATA_FILE_PATH = os.path.join(spk_manager_dir, "..", "Final_Data/kg-fightdelay-dataset", "stream_data.csv")
SPARK_TEMP_FOLDER = os.path.join(spk_manager_dir, 'spark-temp')

CASS_HOST_IP = '127.0.0.1'
CASS_PORT = '9042'
CASS_USERNAME = 'cassandra'
CASS_PASSWORD = 'cassandra'

# Kafka configuration
KAFKA_TOPIC_NAME_CONS = "flight-delay-predict"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
KAFKA_CHECKPOINT_DIR = os.path.join(spk_manager_dir, 'kafka-checkpoint')

def createSparkSession(appName):
    return SparkSession.builder.appName(appName).getOrCreate()

def readCsvData(filePath=DATA_FILE_PATH):
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
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0") \
            .config("spark.local.dir", SPARK_TEMP_FOLDER) \
            .config("spark.streaming.stopGracefullyOnShutdown", True)\
            .getOrCreate()
        print('--- Connection to Cassandra created successfully! ---')
        cassSession.conf.set('spark.sql.shuffle.partitions', 8)
        return cassSession
    except Exception as e:
        print('-- Fail to create Cassandra connection with error: --', e)

def writeToCass(dataset, table='flightdelay', keyspace='testframe'):
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

def readFromCass(table='flightdelay', keyspace='testframe'):
    try:
        print(f'-- Start reading table {table} from Cassandra --')
        df = sparkSession.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=table, keyspace=keyspace) \
            .load()
        print(f'--- Reading table {table} from Cassandra successfully! ---')
        return df
    except Exception as e:
        print('-- Reading from Cassandra fail with error: --', e)
        return
    
def showDataframeInfo(dataframe, streamMode=False, dataShowLimit=5):
    dataframe.printSchema()
    if (streamMode == False):
        dataframe.show(dataShowLimit)
    
def preprecessingBeforeWriteToCass(rawData):
    return lowercaseAllHeader(addRecordIdToCsv(rawData))

def startKafkaReadStream(schema=0, kafkaTopic=KAFKA_TOPIC_NAME_CONS, kafkaServer=KAFKA_BOOTSTRAP_SERVERS_CONS):
    try:
        print(f'-- Start Kafka reading stream --')
        df = sparkSession \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafkaServer) \
            .option("subscribe", kafkaTopic) \
            .option("startingOffsets", "earliest")\
            .load()
        parsedDf = parseJsonFromReadingStream(df, schema)
        print(f'--- Kafka stream reading... ---')
        return parsedDf
    except Exception as e:
        print('-- Fail to open Kafka reading stream with error: --', e)
        return

def output(dataframe, batchId):
    recordCount = dataframe.count()
    print(f"Batch {batchId} contains {recordCount} records.")
    writeToCass(dataframe)
    df = readFromCass()
    df.printSchema()
    df.show()
    

def startKafkaWriteStream(dataframe):
    try:
        print(f'-- Start Kafka writing stream --')
        processedData = preprecessingBeforeWriteToCass(dataframe)
        query = processedData.writeStream \
            .foreachBatch(output)\
            .trigger(processingTime='10 seconds')\
            .option('checkpointLocation', KAFKA_CHECKPOINT_DIR)\
            .start()
        print(f'-- Kafka stream writing --')
        return query
    except Exception as e:
        print('-- Fail to open Kafka writing stream with error: --', e)

def testable():
    print('--Test Starting--')   
    
    data = [(6, 28, "IO1"), (7, 32, "NA1")]
    columns = ["id", "age", "name"]
    print('--We will be writing these following data to cassandra and show it--')
    print(data,columns)
    
    new_df = sparkSession.createDataFrame(data, columns)
    writeToCass(new_df, 'testable')
    
    df = readFromCass('testable')
    df.printSchema()
    df.show()
    
    sparkSession.stop()
    print('--Test Finish--')
    
def spark_manager():
    
    # get Cass schema
    df = readFromCass()
    schema = uppercaseAllHeader(df).schema
    
    # Kafka streaming
    dfStream = startKafkaReadStream(schema)
    
    query = startKafkaWriteStream(dfStream)
    
    # await termination
    query.awaitTermination()
    
    # sparkSession.stop()

if __name__ == "__main__":
    try:
        sparkSession = createCassSession("FlightData")     
        spark_manager()
                
    except Exception as e:
        print('-- General exception caught: --', e)