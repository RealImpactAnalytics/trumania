import pandas as pd
import numpy as np


class Relationship(object):
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
                r2: pd.Series(dtype=int)}
        self._table = pd.DataFrame(cols)
        self.__chooser = chooser

    def add_relation(self, r1, A, r2, B):
        """

        :param r1:
        :param A:
        :param r2:
        :param B:
        :return:
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
        return small_tab.groupby(key_column).aggregate(self.__chooser.generate).reset_index()


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
        :return:
        """
        if key_column == self.__r1:
            self.__chooser.update_choose_col(self.__r2)
        elif key_column == self.__r2:
            self.__chooser.update_choose_col(self.__r1)
        small_tab = self._table[self._table[key_column].isin(keys)]
        return small_tab.groupby(key_column).aggregate(self.__chooser.generate).reset_index()
