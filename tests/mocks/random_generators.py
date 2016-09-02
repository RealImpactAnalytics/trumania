from datagenerator.core.random_generators import *

class ConstantsMockGenerator(Generator):
    """
    For test only: a (non random) Generator returning pre-defined values
    """
    def __init__(self, values):
        Generator.__init__(self)
        self.values = values

    def generate(self, size):
        # (value is ignored)
        return self.values


class ConstantsProfiler(Generator):
    """
    For test only: a (non random) Profiler returning pre-defined values
    """
    def __init__(self, values_series):
        Generator.__init__(self)
        self.values_series = values_series

    def generate(self, weights):
        # (value is ignored)
        return self.values_series[weights.index]
