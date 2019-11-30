from greyhound.core.operators import *

class Stream(object):

    def __init__(self, topic):
        self.topic = topic
        self.operators = []

    async def process_record(self, record):
        # record is the latest message that's arrived in this stream
        for operator in self.operators:
            res = await operator.run(record)

            if isinstance(operator, FilterOperator):
                if res is False:
                    break
            elif isinstance(operator, MapOperator):
                # replace with the new record that the map operator must have returned
                record = res

    def map(self, map_operator):
        # each stream will have a bunch of map operators
        self.operators.append(map_operator)
        return self

    def flat_map(self):
        # each stream will have a bunch of flat map operators
        pass

    def filter(self, filter_operator):
        # each stream will have a bunch of filter operators
        self.operators.append(filter_operator)
        return self
