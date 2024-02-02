import json
from datetime import datetime
from cassandra.cluster import Cluster
from kafka import KafkaConsumer

# Function to convert date format
def convert_date_format(input_date_str):
    input_format = '%m/%d/%Y %H:%M'
    output_format = '%Y-%m-%d %H:%M:%S'
    return datetime.strptime(input_date_str, input_format).strftime(output_format)

# Cassandra connection
cassandra_cluster = Cluster(['127.0.0.1'], port = 9042)
session = cassandra_cluster.connect('market')

# Kafka consumer configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'kafka_to_cassandra'
#consumer_group_id = 'your_consumer_group_id'

# Kafka consumer
kafka_consumer = KafkaConsumer(
    kafka_topic,
    #group_id=consumer_group_id,
    bootstrap_servers=[kafka_bootstrap_servers],
    auto_offset_reset = 'earliest',
    enable_auto_commit=True
)

try:
    while True:
        for msg in kafka_consumer:
            # Parse JSON and insert into Cassandra
            try:
                json_data = json.loads(msg.value.decode('utf-8'))

                # Convert date format
                json_data['InvoiceDate'] = convert_date_format(json_data['InvoiceDate'])

                # Rename "Customer ID" to match Cassandra column name
                json_data['customerid'] = json_data.pop('Customer ID')

                insert_query = f"INSERT INTO market.retail_data JSON '{json.dumps(json_data)}';"
                session.execute(insert_query)
                print("Inserted into Cassandra:", json_data)
                #kafka_consumer.commit()
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
            except Exception as e:
                print(f"Error inserting into Cassandra: {e}")


except KeyboardInterrupt:
    pass

finally:
    kafka_consumer.close()
    cassandra_cluster.shutdown()
