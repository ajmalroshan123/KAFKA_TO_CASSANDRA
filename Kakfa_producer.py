from time import sleep
import pandas as pd
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

def on_send_success(record_metadata):
    print(f"Message sent successfully. Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")

def on_send_error(exception):
    print(f'Error while sending message: {exception}')

# Mention bootstrap and topic
bootstrap_servers = 'localhost:9092'
kafka_topic = 'kafka_to_cassandra'

# Create Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    acks='all',
    #compression_type='gzip',
    retries=3,
    retry_backoff_ms=100,
    #request_timeout_ms=5000,
    #linger_ms=100
)

# Produce messages to the topic
df = pd.read_csv('retail_data.csv')
df = df.fillna('null')

try:
    # Iterate over DataFrame rows and produce to Kafka
    for index, row in df.iterrows():
        # create a dictionary from the row values
        value = row.to_dict()
        # produce to Kafka
        producer.send(topic='kafka_to_cassandra', value=value).add_callback(on_send_success).add_callback(on_send_error)
        sleep(1)
        break
        producer.flush()
except Exception as e:
    print(e)
finally:
    producer.close()
