import os

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
REGISTRY = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
HOSTS = os.getenv('CASSANDRA_HOSTS', 'localhost').split(',')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'warehouse')
GROUP = os.getenv('CONSUMER_GROUP', 'warehouse-state-consumer')
TOPIC = 'warehouse-events'
DLQ = 'warehouse-events-dlq'
SUBJECT = TOPIC + '-value'
