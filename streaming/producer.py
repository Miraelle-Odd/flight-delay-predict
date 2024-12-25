from kafka import KafkaProducer
import pandas as pd
import json
import os
import time

# Kafka configuration
KAFKA_TOPIC_NAME_CONS = "flight-delay-predict"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:19092'

# File paths
script_dir = os.path.dirname(__file__)  # Directory where producer.py is located
#DATA_FILE_PATH = os.path.join(script_dir, "..", "Final_Data", "Training_Stream_Data.csv")
DATA_FILE_PATH = os.path.join(script_dir, "..", "Final_Data", "kg-flightdelay-dataset\stream_data.csv")

def read_data(file_path):
    """
    Reads the flight data CSV file into a Pandas DataFrame.
    """
    try:
        data_predict = pd.read_csv(file_path)
        print(f"Loaded {len(data_predict)} rows from {file_path}")
        return data_predict
    except Exception as e:
        print(f"Error reading data: {e}")
        return None

def send_to_kafka(producer, topic, data, batch_size=20000, delay=5):
    """
    Sends data to Kafka in batches with a delay between each batch.
    
    Args:
        producer: Kafka producer instance.
        topic: Kafka topic name.
        data: DataFrame containing the data to send.
        batch_size: Number of rows to send in each batch.
        delay: Delay in seconds between sending each batch.
    """
    total_rows = len(data)
    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        batch = data.iloc[start:end]
        
        for _, row in batch.iterrows():
            message = row.to_dict()
            try:
                producer.send(topic, value=message)
            except Exception as e:
                print(f"Error sending message: {e}")
        
        print(f"Batch {start // batch_size + 1} sent: {len(batch)} messages.")
        
        if end < total_rows:
            print(f"Waiting for {delay} seconds before sending the next batch...")
            time.sleep(delay)

def producer(file_path):
    """
    Main function to initialize the Kafka producer and send data.
    """
    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize as JSON
    )

    # Read the data
    data = read_data(file_path)
    if data is not None:
        # Send data to Kafka
        send_to_kafka(producer, KAFKA_TOPIC_NAME_CONS, data)

    # Close producer
    producer.flush()
    producer.close()
    print("All messages sent and producer closed.")

if __name__ == "__main__":
    print("Kafka Producer Application Started ...")
    # Path to your CSV file containing flight data
    producer(DATA_FILE_PATH)
    print("Kafka Producer Application Completed.")
