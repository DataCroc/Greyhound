from greyhound.configs.kafka_config import KafkaConfiguration
from greyhound.app import Greyhound
from greyhound.core.operators import *
from greyhound.core.record import Record

import examples.utils as Utils

# create the kafka configuration
kafka_configuration = KafkaConfiguration()
kafka_configuration.bootstrap_servers = "localhost:9092"

# init Greyhound
greyhound = Greyhound("TrendingTweets")
greyhound.set_topics(["tweets_fire_hose"])


def deserializer(value):
    return bytes(value).decode("utf-8")


class DetectHashTagsOperator(FlatMapOperator):

    async def run(self, record):
        tweet_text = record.value.strip()
        hashtags = Utils.extract_hashtags(str(tweet_text))
        return [
            Record("tweets_fire_hose", "", tag) for tag in hashtags
        ]


class DetectTrendingHashtags(MapOperator):

    async def run(self, record):
        hash_tag = record.value
        count = self.state.get(hash_tag, 0)
        self.state[hash_tag] = count + 1
        print(hash_tag, self.state[hash_tag])
        return record


tweets_stream = greyhound\
    .get_stream("tweets_fire_hose", value_deserializer=deserializer)\
    .flat_map(DetectHashTagsOperator())\
    .map(DetectTrendingHashtags())

# this will run forever
greyhound.run()
