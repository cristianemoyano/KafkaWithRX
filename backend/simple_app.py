import threading
import logging
import time
import json
from kafka import KafkaConsumer, KafkaProducer

server = 'kafka:9093'


class Producer(threading.Thread):
    daemon = True

    def run(self):
        producer = KafkaProducer(bootstrap_servers=[server],
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        for i in range(10):
            producer.send('my-topic', {"dataObjectID": "test_{}".format(i)})
            time.sleep(1)


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer('my-topic',
                                 bootstrap_servers=[server],
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        for message in consumer:
            print(message)


def main():
    threads = [
        Producer(),
        Consumer()
    ]
    for t in threads:
        t.start()
    time.sleep(10)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
