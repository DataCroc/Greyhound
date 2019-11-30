from greyhound.configs.kafka_config import KafkaConfiguration
from greyhound.app import Greyhound
from greyhound.core.operators import *

# create the kafka configuration
kafka_configuration = KafkaConfiguration()
kafka_configuration.bootstrap_servers = "localhost:9092"

# init Greyhound
greyhound = Greyhound("MyExampleApp")
greyhound.set_topics(["words"])


def deserializer(value):
    return bytes(value).decode("utf-8")


class ConvertedOutcomeFilterOperator(FilterOperator):

    async def run(self, record):
        outcome = record.value
        converted_outcomes = frozenset(["awesome", "good", "nice"])

        return outcome in converted_outcomes


class ConvertedOutcomeCountOperator(MapOperator):

    async def run(self, record):
        outcome = record.value
        count = self.state.get(outcome, 0)
        self.state[outcome] = count + 1

        print(outcome, self.state[outcome])

        return record


words_stream = greyhound\
    .get_stream("words", value_deserializer=deserializer)\
    .filter(ConvertedOutcomeFilterOperator())\
    .map(ConvertedOutcomeCountOperator())

# this will run forever
greyhound.run()
