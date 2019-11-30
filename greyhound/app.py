# Main class for the Greyhound framework
from collections import defaultdict
from greyhound.core.stream import Stream
from greyhound.kafka.consumer import KafkaConsumer
from greyhound.core.record import Record
import asyncio

loop = asyncio.get_event_loop()


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Greyhound(metaclass=Singleton):

    def __init__(self, app_name):
        self.app_name = app_name
        self.topics = set([])
        self.stream_registry = defaultdict(list)
        self.value_deserializer_registry = {}
        self.consumer = None

    def set_topics(self, topics):
        self.topics = topics

    def get_stream(self, topic, value_deserializer=None):
        stream = Stream(topic)
        self.stream_registry[topic].append(stream)

        if value_deserializer:
            self.value_deserializer_registry[topic] = value_deserializer

        return stream

    def run(self):
        # start the kafka consumer
        self.consumer = KafkaConsumer()\
            .set_topics(self.topics)\
            .build()

        async def run():
            async for record in self.consumer.start():
                topic = record.topic
                new_record = Record(record.topic, record.key, record.value)
                if self.value_deserializer_registry.get(topic):
                    new_record.value = self.value_deserializer_registry[topic](record.value)
                await self.process_record(new_record)

        loop.run_until_complete(run())

    async def process_record(self, record):
        # get the streams for this record
        topic = record.topic
        streams = self.stream_registry[topic]

        for stream in streams:
            await stream.process_record(record)
