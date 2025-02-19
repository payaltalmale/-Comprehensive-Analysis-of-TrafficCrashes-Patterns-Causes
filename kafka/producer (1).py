import pandas as pd
import time
import json
from kafka import KafkaProducer

# Kafka Configuration
KAFKA_TOPIC = "traffic-data-stream"
KAFKA_BROKER = "localhost:9092"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load CSV Data
file_path = "Traffic_data_with_time .csv"
df = pd.read_csv(file_path)

# Stream all columns
def stream_data():
    for _, row in df.iterrows():
        data = row.to_dict()
        producer.send(KAFKA_TOPIC, value=data)
        print(f"Sent: {data}")
        time.sleep(0.65)  # Simulating streaming delay

if __name__ == "__main__":
    print("Starting Kafka Producer...")
    stream_data()




