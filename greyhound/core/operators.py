class BaseOperator(object):

    def run(self, record):
        raise NotImplementedError


class MapOperator(object):

    def __init__(self, state=True):
        self.state = None
        if state:
            self.state = {}

    def run(self, record):
        raise NotImplemented


class FilterOperator(object):

    def __init__(self, state=True):
        self.state = None
        if state:
            self.state = {}

    def run(self, record):
        raise NotImplemented


class FlatMapOperator(object):

    def __init__(self, state=True):
        self.state = None
        if state:
            self.state = {}

    def run(self, record):
        raise NotImplemented
