import numpy as np
import pandas as pd


class Product(object):
    def __init__(self):
        pass


class VoiceProduct(Product):
    def __init__(self, duration_generator, value_generator):
        """

        :param duration_generator:
        :param value_generator:
        :return:
        """
        Product.__init__(self)
        self._duration = duration_generator
        self._value = value_generator

    def generate(self,s):
        gen = dict([("TYPE","VOICE")])
        gen["DURATION"] = self._duration.generate(s)
        gen["VALUE"] = self._value.generate(gen["DURATION"])
        return pd.DataFrame(gen)


class SMSProduct(Product):
    def __init__(self, value_generator):
        """

        :param value_generator:
        :return:
        """
        Product.__init__(self)
        self._value = value_generator

    def generate(self,s):
        gen = dict([("TYPE","SMS"),("DURATION",np.nan)])
        gen["VALUE"] = self._value.generate(s)
        return pd.DataFrame(gen)