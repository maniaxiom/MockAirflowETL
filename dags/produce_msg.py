from confluent_kafka import Producer
import json

# Define your Kafka producer configuration
producer_config = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',  # Replace with your Kafka broker(s)
}

# Initialize the Kafka producer
producer = Producer(producer_config)

# Define the message to send
message = {
    "user_id": 2,
    "program_id": 102,
    "start_time": "2024-12-22T10:00:00Z",
    "session_id": "session789",
    "device": "iphone",
    "event_time": "2024-12-22T10:04:00Z"
}

# Convert the message dictionary to a JSON string
message_json = json.dumps(message)

# Define the Kafka topic
topic = 'netflix-stream'

# Send the JSON message to the Kafka topic
try:
    producer.produce(topic=topic, value=message_json.encode('utf-8'))
    producer.flush()
    print(f"Sent message to topic {topic}: {message_json}")
except Exception as e:
    print(f"Failed to send message: {e}")
