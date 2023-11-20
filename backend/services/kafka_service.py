
from config import loop, KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP, KAFKA_TOPIC
import json
from confluent_kafka import Producer, Consumer

def produce_message(message,producer):
        value_json = json.dumps(message).encode('utf-8')
        producer.produce(topic=KAFKA_TOPIC, value=value_json)
        print(f'sended')
         
def create_a_producer():
        producer_config = {
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            }
        producer = Producer(producer_config)
        print('started a producer')

        return producer


def create_a_consumer():
    consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': KAFKA_CONSUMER_GROUP,
    'auto.offset.reset': 'earliest', 
}

    consumer = Consumer(consumer_config)

    print('started a consumer')
    return consumer
