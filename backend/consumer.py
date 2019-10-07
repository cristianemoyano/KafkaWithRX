from collections.abc import Iterable

import json

from kafka import KafkaConsumer
from rx import of

from abc import ABCMeta


class RXConsumer(object):
    __metaclass__ = ABCMeta

    consumer_topic = None
    consumer_group = None
    server = 'kafka:9093'

    def __init__(self, iterable=None):
        self.__iterable = iterable
        # self.source = of(self.iterable)
        # self.source.subscribe(
        #     on_next=self.on_next,
        #     on_error=self.on_error,
        #     on_completed=self.on_completed,
        # )

    @property
    def iterable(self):
        if self.__iterable:
            return self.__iterable
        return KafkaConsumer(
            self.consumer_topic,
            auto_offset_reset='earliest',
            bootstrap_servers=[self.server],
            api_version=(0, 10),
            value_deserializer=json.loads,
            consumer_timeout_ms=1000,
        )

    def process_consumer(self, msg):
        return NotImplemented

    def on_next(self, msg):
        print("On next: ", msg)

    def on_completed(self):
        print('completed')

    def on_error(self, error):
        print('Error:', error)


class MyImplementation1(RXConsumer):

    consumer_topic = "my-topic"
    consumer_group = 'foo'

    def process_consumer(self, msg):
        print(msg.value)
