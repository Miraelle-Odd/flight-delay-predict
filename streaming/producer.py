from kafka import KafkaProducer
import pandas as pd 
import numpy as np

# Thực tế thì chỗ này nên crawl data nhưng thời gian có hạn nên data sẽ đc add sẵn
# có thể nghiên cứu lấy từ drive data hoặc cassandra
data_predict = pd.read_csv("",)

KAFKA_TOPIC_NAME_CONS = "project"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")
    # process the data from data_predict
    print("Kafka Producer Application Completed. ")