from datagenerator.random_generators import *


class ConstantsMockGenerator(Generator):
    """
    For test only: a (non random) Genrator returning pre-defined values
    """
    def __init__(self, values):
        Generator.__init__(self, {})
        self.values = values

    def generate(self, size):
        # (value is ignored)
        return self.values
