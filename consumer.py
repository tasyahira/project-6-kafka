"""
Kafka Consumer for User Activity Data

This script consumes messages from a Kafka topic named 'activity', processes the JSON data,
and stores it in a PostgreSQL database table named 'user_activity'.

Requirements:
- Python 3.x
- pandas
- kafka-python
- SQLAlchemy

Usage:
1. Ensure the required packages are installed: pip install pandas kafka-python SQLAlchemy
2. Update the Kafka server address and PostgreSQL database connection string.
3. Run the script.
"""

import json
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine

def main():
    """
    Main function to consume Kafka messages, process data, and store it in PostgreSQL.
    """
    # Connect to PostgreSQL database
    engine = create_engine('postgresql://de17:de17!#@34.126.160.212:5432/project6')

    # Set up Kafka consumer
    consumer = KafkaConsumer("activity", bootstrap_servers='34.126.160.212')
    print("Starting the Kafka consumer")

    # Consume messages from Kafka topic
    for msg in consumer:
        print(f"Received JSON data: {json.loads(msg.value)}")

        # Process data and convert to DataFrame
        data = json.loads(msg.value)
        df = pd.DataFrame(data, index=[0])

        # Store data in PostgreSQL database
        df.to_sql('user_activity', engine, if_exists='append', index=False)
        print("Data stored in PostgreSQL database.")

if __name__ == "__main__":
    main()
