"""Base file for generator

"""
import pandas as pd
import numpy as np

import random_generators

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

    def add_actor(self,name,actor):
        """

        :param name:
        :param actor:
        :return:
        """
        if self.__actors.has_key(name):
            raise Exception("Already having actors named %s" % name)
        self.__actors[name] = actor

    def add_item(self,name,item):
        """

        :param name:
        :param item:
        :return:
        """
        if self.__items.has_key(name):
            raise Exception("Already having items named %s" % name)
        self.__items[name] = item

    def add_relationship(self,r1,r2,rel):
        """

        :param r1:
        :param r2:
        :param rel:
        :return:
        """
        if self.__relationships.has_key((r1,r2)):
            raise Exception("Already having relationship between %s and %s" %(r1,r2))
        self.__relationships[(r1,r2)] = rel



class Actor(object):
    """

    """
    def __init__(self,size,id_start=0):
        """

        :param size:
        :param id_start:
        :return:
        """
        IDs = np.arange(id_start,id_start+size)
        self._table = pd.DataFrame({"ID":IDs,"clock":0})


    def add_attribute(self, name, generator):
        """Adds a column named "name" to the inner table of the actor, randomly generated from the generator.

        :param name: string, will be used as name for the column in the table
        :param generator: class from the random_generator series. needs to have a generate function that works
        :return: none
        """
        self._table[name] = generator.generate(len(self._table.index))

    def who_acts_now(self):
        """

        :return:
        """
        return (self._table["clock"]==0).index

    def update_clock(self,decrease=1):
        """

        :param decrease:
        :return:
        """
        self._table["clock"] -= 1


    def update_attribute(self, name, generator):
        """

        :param name:
        :param generator:
        :return:
        """
        self._table[name] = generator.generate(len(self._table.index))


    def __repr__(self):
        return self._table



class Item(object):
    """

    """
    def __init__(self,size,id_start=0):
        """

        :param size:
        :param id_start:
        :return:
        """
        IDs = np.arange(id_start,id_start+size)
        self._table = pd.DataFrame({"ID":IDs})


    def add_attribute(self, name, generator):
        """Adds a column named "name" to the inner table of the item, randomly generated from the generator.

        :param name: string, will be used as name for the column in the table
        :param generator: class from the random_generator series. needs to have a generate function that works
        :return: none
        """
        self._table[name] = generator.generate(len(self._table.index))

    def __repr__(self):
        return self._table


class Relationship(object):
    """

    """
    def __init__(self,r1,r2,weight=False):
        """

        :param r1:string
        :param r2:string
        :return:
        """
        cols = [r1,r2]
        if weight:
            cols.append("weight")
        self._table = pd.DataFrame(columns=cols)


    def add_relation(self,r1,A,r2,B,W=None):
        """

        :param A:
        :param B:
        :param W:
        :return:
        """