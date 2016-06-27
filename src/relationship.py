import pandas as pd
import numpy as np


class Relationship(object):
    """

    """

    def __init__(self, r1, r2, chooser):
        """

        :type r1: str
        :param r1: name for first element
        :type r2: str
        :param r2: name for second element
        :type chooser: random_generator.Chooser
        :param chooser: Chooser object that will define the random selection of an element in the relationship
        :return:
        """
        cols = {r1: pd.Series(dtype=int),
                r2: pd.Series(dtype=int)}
        self._table = pd.DataFrame(cols)
        self.__chooser = chooser

    def add_relation(self, r1, A, r2, B):
        """

        :type r1: str
        :param r1: name of the first column
        :type A: Pandas Series object
        :param A: all entries of the first column in the relationship
        :type r2: str
        :param r2: name of the second column
        :type B: Pandas Series object
        :param B: all entries of the second column in the relationship
        :return: None
        """
        df = pd.DataFrame({r1: A, r2: B})

        self._table = self._table.append(df, ignore_index=True)

    def select_one(self, key_column, keys):
        """

        :param key_column:
        :param keys:
        :return:
        """

        small_tab = self._table[self._table[key_column].isin(keys)]
        return small_tab.groupby(key_column).aggregate(self.__chooser.generate)


class WeightedRelationship(object):
    """

    """

    def __init__(self, r1, r2, chooser):
        """

        :param r1: string, name for first element
        :param r2: string, name for second element
        :param chooser:
        :return:
        """
        cols = {r1: pd.Series(dtype=int),
                r2: pd.Series(dtype=int),
                "weight": pd.Series(dtype=float)}
        self.__r1 = r1
        self.__r2 = r2
        self._table = pd.DataFrame(cols)
        self.__chooser = chooser

    def add_relation(self, r1, A, r2, B, W):
        """

        :param r1:
        :param A:
        :param r2:
        :param B:
        :param W: weight column.
        :return:
        """
        df = pd.DataFrame({r1: A, r2: B, "weight": W})
        self._table = self._table.append(df, ignore_index=True)

    def select_one(self, key_column, keys):
        """

        :param key_column:
        :param keys:
        :return: Pandas Series, index are the ones from keys
        """
        if key_column == self.__r1:
            self.__chooser.update_choose_col(self.__r2)
        elif key_column == self.__r2:
            self.__chooser.update_choose_col(self.__r1)

        small_tab = self._table[self._table[key_column].isin(keys)]
        return small_tab.groupby(key_column).aggregate(self.__chooser.generate).drop("weight",axis=1)


class ProductRelationship(WeightedRelationship):
    """

    """

    def __init__(self, r1, r2, chooser, products):
        """

        :param r1:
        :param r2:
        :param chooser:
        :param products:
        :return:
        """
        WeightedRelationship.__init__(self, r1, r2, chooser)
        self._products = products

    def select_one(self, key_column, keys):
        choices = WeightedRelationship.select_one(self,key_column,keys)
        data_for_out = choices.copy()
        choices = choices.iloc[:,0]
        for p in choices.unique():
            this_p_index = choices[choices==p].index
            p_data = self._products[p].generate(len(this_p_index))
            for pdf in p_data.columns.values:
                data_for_out.loc[this_p_index,pdf] = p_data.loc[:,pdf].values
        return data_for_out


class AgentRelationship(WeightedRelationship):
    """

    """
    def __init__(self,r1,r2,chooser,agents=None):
        """

        :param r1:
        :param r2:
        :param chooser:
        :param agents:
        :return:
        """
        WeightedRelationship.__init__(self,r1,r2,chooser)
        self._agents = agents

    def select_one(self, key_column, keys):
        choices = WeightedRelationship.select_one(self,key_column,keys)
        choices["value"] = 100

        return choices


class SimpleMobilityRelationship(WeightedRelationship):
    """

    """

    def choose(self, clock, key_column, keys):
        return self.select_one(key_column, keys)


class HWRMobilityRelationship(WeightedRelationship):
    """

    """
    def __init__(self,r1,r2,chooser,time_f):
        """

        :param r1:
        :param r2:
        :param chooser:
        :param time_f:
        :return:
        """
        cols = {r1: pd.Series(dtype=int),
                r2: pd.Series(dtype=int),
                "weight": pd.Series(dtype=float)}
        self._home_table = pd.DataFrame(cols)
        self._work_table = pd.DataFrame(cols)
        self._random_table = pd.DataFrame(cols)
        self.__chooser = chooser
        self.__r1 = r1
        self.__r2 = r2
        self.__time_f = time_f

    def add_home(self, r1, A, r2, B):
        """

        :param r1:
        :param A:
        :param r2:
        :param B:
        :return:
        """
        df = pd.DataFrame({r1: A, r2: B})
        self._home_table = self._home_table.append(df, ignore_index=True)

    def add_work(self, r1, A, r2, B):
        """

        :param r1:
        :param A:
        :param r2:
        :param B:
        :return:
        """
        df = pd.DataFrame({r1: A, r2: B})
        self._work_table = self._work_table.append(df, ignore_index=True)

    def add_random(self, r1, A, r2, B):
        """

        :param r1:
        :param A:
        :param r2:
        :param B:
        :return:
        """
        df = pd.DataFrame({r1: A, r2: B})
        self._random_table = self._random_table.append(df, ignore_index=True)

    def choose(self, clock, key_column, keys):
        """

        :param clock:
        :param key_column:
        :param keys:
        :return:
        """
        # TODO: make a function of the clock that returns what's needed
        w_home,w_work,w_random = self.__time_f(clock)

        small_home = self._home_table[self._home_table[self.__r1].isin(keys)].copy()
        small_home["weight"] = small_home["weight"]*w_home

        small_work = self._work_table[self._work_table[self.__r1].isin(keys)].copy()
        small_work["weight"] = small_work["weight"]*w_work

        small_random = self._random_table[self._random_table[self.__r1].isin(keys)].copy()
        small_random["weight"] = small_random["weight"]*w_random

        small_tab = pd.concat([small_home,small_work,small_random],ignore_index=True)
        return small_tab.groupby(key_column).aggregate(self.__chooser.generate)
