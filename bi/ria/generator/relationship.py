import pandas as pd
from numpy.random import RandomState


class Relationship(object):
    """
        One-to-many relationship between actors.

        Each implementation of this provides a `select_one(from_ids)` method
        that randomly picks one "to" side for each of the provided "from"
        A similar `pop_one(from_ids)` method is availeble, which behaves
        similarly + removes the selected relationship.

        Both those method may return supplementary data with each "to" side.
        For example, a CustomerSellerRelationship could be designed to
        captures the available sellers of each buyers. `select_one` in that
        case could mean "sale", and supplementary properties could be the
        description of the sold items.

        `select_one` and `pop_one` must have the same return signature.

    """

    def __init__(self, name, seed):
        """

        :param r1: string, name for first element
        :param r2: string, name for second element
        :param chooser:
        :return:
        """

        self.__name = name
        self.__state = RandomState(seed)
        self._table = pd.DataFrame(columns=["from", "to", "weight"])

    def add_relations(self, from_ids, to_ids, weights=1):
        """

        """

        new_relations = pd.DataFrame({"from": from_ids,
                                    "to": to_ids,
                                    "weight": weights
                                    })

        self._table = pd.concat([self._table, new_relations])
        self._table.reset_index(drop=True, inplace=True)

    def select_one(self, from_ids, named_as="to"):
        """

        :param key_column:
        :param keys:
        :return: Pandas Series, index are the ones from keys
        """

        selected = self._table[self._table["from"].isin(from_ids)]

        if selected.shape[0] == 0:
            return pd.DataFrame(columns=["from", named_as])

        return (selected
                .groupby(by="from")
                .apply(lambda df: df.sample(n=1, weights="weight")[["to"]])
                .reset_index()
                .rename(columns={"to": named_as, "index": "from"})
                .drop("level_1", axis=1) # this one comes from the df in apply
                )

    def pop_one(self, **kwargs):
        """
        Same as select_one, but the chosen rows are deleted
        :param key_columnn:
        :param keys:
        :return:
        """

        choices = self.select_one(**kwargs)
        self._table.drop(choices.index, inplace=True)
        return choices

    def remove(self, from_ids, to_ids):
        lines = self._table[self._table["from"].isin(from_ids) &
                            self._table["to"].isin(to_ids)]

        self._table.drop(lines.index, inplace=True)


# TODO: same thing: move this to concern-specific module
class ProductRelationship(Relationship):
    """

    """

    def __init__(self, products, **kwargs):
        """

        :param r1:
        :param r2:
        :param chooser:
        :param products:
        :return:
        """
        Relationship.__init__(self, **kwargs)
        self._products = products

    def select_one(self, from_ids, named_as="to"):
        chosen_products = Relationship.select_one(self, from_ids, named_as)

        # TODO: cf request from Gautier: refactor this as 2
        # separate relationships
        def add_products(df):
            product = df[named_as].unique()[0]
            p_data = self._products[product].generate(df.shape[0])
            p_data.index = df.index
            df[p_data.columns] = p_data
            return df

        return chosen_products.groupby(named_as).apply(add_products)


# TODO: move this be in a "sales" package separated from the core
class AgentRelationship(Relationship):
    """

    """
    def __init__(self, **kwargs):
        """

        :param r1:
        :param r2:
        :param chooser:
        :param agents:
        :return:
        """
        Relationship.__init__(self, **kwargs)

    def select_one(self, **kwargs):
        choices = Relationship.select_one(self, **kwargs)
        choices["value"] = 1000

        return choices


# TODO: see with Gautier: this is no longer used anywhere => should we delete,
# or are these projects for later ?

# class SimpleMobilityRelationship(WeightedRelationship):
#     """
#
#     """
#
#     def choose(self, clock, key_column, keys):
#         return self.select_one(key_column, keys)
#
#
# class HWRMobilityRelationship(WeightedRelationship):
#     """
#
#     """
#     def __init__(self,r1,r2,chooser,time_f):
#         """
#
#         :param r1:
#         :param r2:
#         :param chooser:
#         :param time_f:
#         :return:
#         """
#         cols = {r1: pd.Series(dtype=int),
#                 r2: pd.Series(dtype=int),
#                 "weight": pd.Series(dtype=float)}
#         self._home_table = pd.DataFrame(cols)
#         self._work_table = pd.DataFrame(cols)
#         self._random_table = pd.DataFrame(cols)
#         self.__chooser = chooser
#         self.__r1 = r1
#         self.__r2 = r2
#         self.__time_f = time_f
#
#     def add_home(self, r1, A, r2, B):
#         """
#
#         :param r1:
#         :param A:
#         :param r2:
#         :param B:
#         :return:
#         """
#         df = pd.DataFrame({r1: A, r2: B})
#         self._home_table = self._home_table.append(df, ignore_index=True)
#
#     def add_work(self, r1, A, r2, B):
#         """
#
#         :param r1:
#         :param A:
#         :param r2:
#         :param B:
#         :return:
#         """
#         df = pd.DataFrame({r1: A, r2: B})
#         self._work_table = self._work_table.append(df, ignore_index=True)
#
#     def add_random(self, r1, A, r2, B):
#         """
#
#         :param r1:
#         :param A:
#         :param r2:
#         :param B:
#         :return:
#         """
#         df = pd.DataFrame({r1: A, r2: B})
#         self._random_table = self._random_table.append(df, ignore_index=True)
#
#     def choose(self, clock, key_column, keys):
#         """
#
#         :param clock:
#         :param key_column:
#         :param keys:
#         :return:
#         """
#         # TODO: make a function of the clock that returns what's needed
#         w_home,w_work,w_random = self.__time_f(clock)
#
#         small_home = self._home_table[self._home_table[self.__r1].isin(keys)].copy()
#         small_home["weight"] = small_home["weight"]*w_home
#
#         small_work = self._work_table[self._work_table[self.__r1].isin(keys)].copy()
#         small_work["weight"] = small_work["weight"]*w_work
#
#         small_random = self._random_table[self._random_table[self.__r1].isin(keys)].copy()
#         small_random["weight"] = small_random["weight"]*w_random
#
#         small_tab = pd.concat([small_home,small_work,small_random],ignore_index=True)
#         return small_tab.groupby(key_column).aggregate(self.__chooser.generate)
