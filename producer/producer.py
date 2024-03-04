"""
Kafka Producer for Exporting Fraud Log Data

This script reads data from a JSON file ('logFraud.json'), serializes it, and sends it to a Kafka topic named 'activity'.

Requirements:
- Python 3.x
- kafka-python

Usage:
1. Ensure the required packages are installed: pip install kafka-python
2. Update the Kafka server address.
3. Provide the correct path to the 'logFraud.json' file.
4. Run the script.
"""

import json
import time
from kafka import KafkaProducer

def json_serializer(data):
    """
    Serialize data to JSON format.
    """
    return json.dumps(data).encode("utf-8")

def main():
    """
    Main function to read data from a JSON file and send it to a Kafka topic.
    """
    # Load data from the JSON file
    with open('logFraud.json', 'rb') as file:
        data_file = json.load(file)

    # Initialize Kafka producer
    producer = KafkaProducer(bootstrap_servers=['34.126.160.212'], value_serializer=json_serializer)
    print("Starting the Kafka producer")

    try:
        while True:
            # Iterate through data and send to Kafka topic
            for data in data_file:
                print(data)
                producer.send("activity", data)
                time.sleep(10)  # Adjust the sleep time as needed

    except KeyboardInterrupt:
        print("Kafka producer interrupted by user.")

if __name__ == "__main__":
    main()
