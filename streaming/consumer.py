from kafka import KafkaConsumer
import json
import pandas as pd

# Kafka configuration
KAFKA_TOPIC_NAME = "flight-delay-predict"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:19092'

def consume_messages():
    """
    Consumes messages from the Kafka topic and processes flight delay data.
    """
    # Initialize Kafka Consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',  # Start reading from the beginning of the topic
        enable_auto_commit=True,       # Commit offsets automatically
        group_id='project-consumer-group',  # Consumer group ID
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON data
    )

    print("Kafka Consumer Application Started...")

    # Initialize a list to store processed data
    processed_data = []

    try:
        for message in consumer:
            # Deserialize the message
            flight_data = message.value

            # Print raw data for debugging
            print(f"Raw Message: {flight_data}")

            # Process data (optional: for demonstration, converting to DataFrame-friendly format)
            processed_data.append(flight_data)

            # Optionally: Display certain fields
            print(
                f"Flight {flight_data['OP_CARRIER_FL_NUM']} from {flight_data['ORIGIN_CITY_NAME']} "
                f"to {flight_data['DEST_CITY_NAME']} delayed by {flight_data['DEP_DELAY']} minutes."
            )

    except KeyboardInterrupt:
        print("Kafka Consumer Application Stopped.")
    finally:
        # Convert processed data to DataFrame
        if processed_data:
            df = pd.DataFrame(processed_data)
            print("\nFinal Processed Data:")
            print(df.head())  # Display the first few rows of the DataFrame

        # Close the consumer
        consumer.close()

if __name__ == "__main__":
    print("Starting Kafka Consumer...")
    consume_messages()
