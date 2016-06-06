import pandas as pd
import numpy as np


class Circus(object):
    """

    """

    def __init__(self):
        """

        :return:
        """
        self.__actors = {}
        self.__items = {}
        self.__relationships = {}

    def add_actor(self, name, actor):
        """

        :param name:
        :param actor:
        :return:
        """
        if self.__actors.has_key(name):
            raise Exception("Already having actors named %s" % name)
        self.__actors[name] = actor

    def add_item(self, name, item):
        """

        :param name:
        :param item:
        :return:
        """
        if self.__items.has_key(name):
            raise Exception("Already having items named %s" % name)
        self.__items[name] = item

    def add_relationship(self, r1, r2, rel):
        """

        :param r1:
        :param r2:
        :param rel:
        :return:
        """
        if self.__relationships.has_key((r1, r2)):
            raise Exception("Already having relationship between %s and %s" % (r1, r2))
        self.__relationships[(r1, r2)] = rel