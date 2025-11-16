from kafka import KafkaProducer
import json
import personGenerator as personGenerator 
from dotenv import load_dotenv
import os
load_dotenv()


bootstrap_servers=os.getenv('bootstrap_servers')

person=personGenerator.generate_person()
kafka=KafkaProducer(bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def on_success(record_meta):
    print(f"Message sent to topic {record_meta.topic} partition {record_meta.partition} offset {record_meta.offset}")

def on_error(excp):
    print(f"Failed to send message: {excp}")
topic='personFlink'

try:
    kafka.send(topic,person).add_callback(on_success).add_errback(on_error)
except:
    print('error sending message to kafka')

kafka.flush()