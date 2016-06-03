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
        return self._table[self._table["clock"]==0]


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


    def make_actions(self,new_time_generator):
        """

        :param new_time_generator:
        :return:
        """
        act_now = self.who_acts_now()
        out = pd.DataFrame(columns=["ID","action"])
        if len(act_now.index) > 0:
            out["ID"] = act_now["ID"]
            out["action"] = "ping"
            self._table.loc[act_now.index,"clock"] = new_time_generator.generate(len(act_now.index))
        self.update_clock()
        return out


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

        :param r1: string, name for first element
        :param r2: string, name for second element
        :param weight: bool, if True there will be a weight for each relationship
        :return:
        """
        cols = {r1:pd.Series(dtype=int),
                r2:pd.Series(dtype=int)}
        if weight:
            cols["weight"] = pd.Series(dtype=float)
        self._table = pd.DataFrame(cols)


    def add_relation(self,r1,A,r2,B,W=None):
        """

        :param r1:
        :param A:
        :param r2:
        :param B:
        :param W:
        :return:
        """
        df = pd.DataFrame({r1:A,r2:B})
        print df
        if W is not None:
            df["weight"] = W

        self._table = self._table.append(df,ignore_index=True)