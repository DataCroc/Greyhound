from greyhound.core.operators import *

class Stream(object):

    def __init__(self, topic):
        self.topic = topic
        self.operators = []

    async def process_record(self, record):

        queue = [(record, 0)]

        while queue:

            current_record, operator_index = queue[0]
            queue = queue[1:]

            if operator_index > len(self.operators) - 1:
                continue

            operator = self.operators[operator_index]
            res = await operator.run(current_record)

            if isinstance(operator, FilterOperator):
                if res:
                    queue.append((record, operator_index + 1))
            elif isinstance(operator, MapOperator):
                # replace with the new record that the map operator must have returned
                queue.append((res, operator_index + 1))
            elif isinstance(operator, FlatMapOperator):
                # We get a list of new records here, which have to flow individually through the rest of the stream
                for mappedRecord in res:
                    queue.append((mappedRecord, operator_index + 1))

    def map(self, map_operator):
        # each stream will have a bunch of map operators
        self.operators.append(map_operator)
        return self

    def flat_map(self, flat_map_operator):
        # each stream will have a bunch of flat map operators
        self.operators.append(flat_map_operator)
        return self

    def filter(self, filter_operator):
        # each stream will have a bunch of filter operators
        self.operators.append(filter_operator)
        return self
