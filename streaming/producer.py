from kafka import KafkaProducer
import pandas as pd
import json
import os


# Kafka configuration
KAFKA_TOPIC_NAME_CONS = "flight-delay-predict"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

# File paths
script_dir = os.path.dirname(__file__)  # Directory where producer.py is located
DATA_FILE_PATH = os.path.join(script_dir, "..", "Final_Data", "Training_Stream_Data.csv")

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

def send_to_kafka(producer, topic, data):
    """
    Sends each row of the data as a message to the Kafka topic.
    """
    for _, row in data.iterrows():
        # Convert each row to a dictionary, then to JSON
        message = row.to_dict()
        try:
            producer.send(topic, value=message)
            print(f"Message sent: {message}")
        except Exception as e:
            print(f"Error sending message: {e}")

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
