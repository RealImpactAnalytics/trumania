import numpy as np


class Product(object):
    def __init__(self):
        pass


class VoiceProduct(Product):
    def __init__(self, durationgenerator, valuegenerator):
        """

        :param durationgenerator:
        :param valuegenerator:
        :return:
        """
        Product.__init__(self)
        self._duration = durationgenerator
        self._value = valuegenerator

    def generate(self,s):
        gen = dict([("TYPE","VOICE")])
        gen["DURATION"] = self._duration.generate(s)
        gen["VALUE"] = self._value.generate(gen["DURATION"])
        return gen


class SMSProduct(Product):
    def __init__(self,valuegenerator):
        """

        :param valuegenerator:
        :return:
        """
        Product.__init__(self)
        self._value = valuegenerator

    def generate(self,s):
        gen = dict([("TYPE","SMS"),("DURATION",np.nan)])
        gen["VALUE"] = self._value.generate(s)
        return gen