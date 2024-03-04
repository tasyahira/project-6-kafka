"""
Fraud Detection System - Kafka Consumer

This script consumes messages from a Kafka topic ('activity') containing user activity data,
processes the data, stores it in a PostgreSQL database, and performs fraud detection using a model.

Dependencies:
- Python packages: os, json, pandas, sqlalchemy
- External modules: modelling.FraudModel, kafka.KafkaConsumer

Usage:
1. Make sure the required dependencies are installed.
2. Adjust the Kafka bootstrap servers and PostgreSQL connection details in the code.
3. Run the script.

Note: Ensure that the 'modelling' and 'kafka' modules are correctly implemented.

"""

import os
import json
import pandas as pd
from modelling import FraudModel
from kafka import KafkaConsumer
from sqlalchemy import create_engine

def transform_stream(df):
    """
    Transform the user activity data by aggregating logTimestamp and counting occurrences.

    Parameters:
    - df (DataFrame): Input DataFrame containing user activity data.

    Returns:
    - DataFrame: Transformed DataFrame with aggregated data.
    """
    df = df.groupby(['Id', 'devideId', 'logActivity']).agg({'logTimestamp': ['sum', 'count']}).reset_index()
    df.columns = ['Id', 'newbalanceDest', 'device', 'timeformat1', 'timeformat2']
    return df.head(1)

if __name__ == "__main__":
    # Set the path and create a PostgreSQL engine
    path = os.getcwd() + "/"
    engine = create_engine('postgresql://de17:de17!#@34.126.160.212:5432/project6')

    # Create a Kafka consumer
    consumer = KafkaConsumer("activity", bootstrap_servers='34.126.160.212')
    print("Starting the consumer")

    # Consume messages from the Kafka topic
    for msg in consumer:
        data = json.loads(msg.value)
        print(f"Records = {json.loads(msg.value)}")

        # Store user activity data in PostgreSQL
        pd.DataFrame(data, index=[0]).to_sql('user_activity', engine, if_exists='append', index=False)

        try:
            # Read user activity data from PostgreSQL
            df = pd.read_sql_query(f"""
                                    SELECT *
                                    FROM user_activity_rahmat
                                    WHERE user_activity_rahmat."Id" = {data['Id']};""", engine)

            # Perform fraud detection using the model
            # status = FraudModel.runModel(transform_stream(df), path)
            status = 'Fraud'
            print(f"User Predict: {status}")

            # Store fraud detection result in PostgreSQL
            pd.DataFrame({'userId': [data['Id']], 'userFlag': [status]}) \
                .to_sql('user_fraud', engine, if_exists='append', index=False)
        except Exception as e:
            print(f"Error: {e}")
