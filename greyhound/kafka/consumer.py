from aiokafka import AIOKafkaConsumer
import asyncio

loop = asyncio.get_event_loop()


class KafkaConsumer(object):

    def __init__(self):
        self.kafka_settings = None
        self.group_id = None
        self.topics = None
        self.consumer = None

    def get_bootstrap_servers(self):
        return self.kafka_settings.bootstreap_servers

    def set_topics(self, topics):
        self.topics = topics
        return self

    def set_group_id(self, group_id):
        self.group_id = group_id

    def build(self):
        self.consumer = AIOKafkaConsumer(
            *self.topics, loop=loop, bootstrap_servers='localhost:9092',
            group_id="MyGreatConsumerGroup"  # This will enable Consumer Groups
        )
        return self

    async def start(self):
        await self.consumer.start()
        async for msg in self.consumer:
            yield msg
