from kafka import KafkaProducer
import json
from transactionGenerator import TransactionGenerator
from time import sleep
from dotenv import load_dotenv
import os

load_dotenv()
transactionObject=TransactionGenerator()
topic=os.getenv('topic_name')
bootstrap_server=os.getenv('bootstrap_servers')

producer = KafkaProducer(
    bootstrap_servers = bootstrap_server,
    value_serializer = lambda v : json.dumps(v).encode('UTF-8')
    )


def success(msg):
    print(f"Success: topicL {msg.topic}, partition: {msg.partition}, offset: {msg.offset} ")

def error(msg):
    print('Something went wrong with kafka send')


def send_mess():
    try:
        for i in range(10):
            transaction=transactionObject.generate_transaction()
            producer.send(topic,transaction).add_callback(success).add_errback(error)
            print(transaction)
            print("--------------------------------")
            sleep(1)
    except Exception as e:
        print(f"Something wrong {e}")
    finally:
        producer.flush()
        producer.close()



if __name__ == '__main__':
    send_mess()






