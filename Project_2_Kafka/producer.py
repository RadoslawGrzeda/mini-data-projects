# import kafka
from kafka import KafkaProducer
import json
import logging
import time
# from kafka.errors import KafkaError
import random
import string
from faker import Faker
from dotenv import load_dotenv
import os
import person_generator
load_dotenv()



# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

fake = Faker('pl')

bootstrap_servers = os.getenv('kafka_bootstrap_servers') 
# print(f"Bootstrap servers: {bootstrap_servers}")
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
topic_name = 'person'

def on_send_success(record_metadata):
    print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"Failed to send message: {excp}")

# person = person_generator.generate_person()

for i in range(10):
    person = person_generator.generate_person()
    producer.send(topic_name, person).add_callback(on_send_success).add_errback(on_send_error)
    time.sleep(2)
# producer.send(topic_name, {'message': 'Hello, Kafka!'}).add_callback(on_send_success).add_errback(on_send_error)