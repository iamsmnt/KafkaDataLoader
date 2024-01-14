from time import sleep
from json import dumps
from kafka import KafkaProducer

def message_producer(message):
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
        
        producer.send('test-topic',message)
    except Exception as e:
        return e