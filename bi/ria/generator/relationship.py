import pandas as pd

from bi.ria.generator.random_generators import WeightedChooserAggregator, \
    ChooserAggregator


class Relationship(object):
    """

    """

    def __init__(self, name, seed):
        """

        :type r1: str
        :param r1: name for first element
        :type r2: str
        :param r2: name for second element
        :type chooser: random_generator.Chooser
        :param chooser: Chooser object that will define the random selection of an element in the relationship
        :return:
        """
#        self._table = pd.DataFrame(columns=["from", "to"], dtype=np.int)
        self._table = None
        self.__name = name
        # self.__chooser = chooser
        self.__chooser = ChooserAggregator(seed)

    def add_relations(self, from_ids, to_ids):
        """
        Adds connections from and to the specified ids

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

        new_relations = pd.DataFrame({"from": from_ids, "to": to_ids})
        self._table = pd.concat([self._table, new_relations]).reset_index()

    # def select_one(self, key_column, keys):
    def select_one(self, from_ids, named_as="to"):
        """

        :param key_column:
        :param keys:
        :return:
        """

        # TODO: we coud have a frozen relationship that avoid re-doing the
        # group by eah time
        return (self
                ._table[self._table["from"].isin(from_ids)]
                .groupby(by="from", as_index=False)
                .agg(self.__chooser.generate)
                .rename(columns={"to": named_as})
                )
        #return small_tab.groupby(key_column).aggregate(self.__chooser.generate)
        # small_tab = self._table[self._table[key_column].isin(keys)]
        # return small_tab.groupby(key_column).aggregate(self.__chooser.generate)

    # def pop_one(self,key_column,keys):
    def pop_one(self, from_ids):
        """
        Same as select_one, but the chosen rows are deleted
        :param key_columnn:
        :param keys:
        :return:
        """

        choices = self.select_one(from_ids)
        self._table.drop(choices.index, inplace=True)
        return choices

        # choices = self.select_one(key_column,keys)
        # non_key = self._table.columns.values[0]
        # if non_key == key_column:
        #     non_key = self._table.columns.values[1]
        #
        # lines = self._table[self._table[key_column].isin(choices.index.values) & self._table[non_key].isin(choices[non_key].values)]
        # self._table.drop(lines.index,inplace=True)
        # return choices

    def remove(self, from_ids, to_ids):
        lines = self._table[self._table["from"].isin(from_ids) &
                            self._table["to"].isin(to_ids)]

        self._table.drop(lines.index, inplace=True)

    # def remove(self,key_column,keys,value_colum,values):
    #     lines = self._table[self._table[key_column].isin(keys) & self._table[value_colum].isin(values)]
    #     self._table.drop(lines.index, inplace=True)


# TODO:lot's of copy/paste from the one above + missing methods
class WeightedRelationship(object):
    """

    """

    # def __init__(self, name, chooser):
    def __init__(self, name, seed):
        """

        :param r1: string, name for first element
        :param r2: string, name for second element
        :param chooser:
        :return:
        """

        self.__name = name
        # TODO: should'nt this code be inside this object ?
        self.__chooser = chooser = WeightedChooserAggregator("to",
                                                             "weight",
                                                             seed)

        # cols = {r1: pd.Series(dtype=int),
        #         r2: pd.Series(dtype=int),
        #         "weight": pd.Series(dtype=float)}
        # self.__r1 = r1
        # self.__r2 = r2
        # self._table = pd.DataFrame(cols)
        self.__chooser = chooser
        self._table = None

    def add_relations(self, from_ids, to_ids, weights):
        """

        :param r1:
        :param A:
        :param r2:
        :param B:
        :param W: weight column.
        :return:
        """

        new_relations = pd.DataFrame({"from": from_ids,
                                    "to": to_ids,
                                    "weight": weights
                                    })

        self._table = pd.concat([self._table, new_relations]).reset_index(
            drop=True)

        # df = pd.DataFrame({r1: A, r2: B, "weight": W})
        # self._table = self._table.append(df, ignore_index=True)

    # def select_one(self, key_column, keys):
    def select_one(self, from_ids, named_as="to"):
        """

        :param key_column:
        :param keys:
        :return: Pandas Series, index are the ones from keys
        """
        # if key_column == self.__r1:
        #     self.__chooser.update_choose_col(self.__r2)
        # elif key_column == self.__r2:
        #     self.__chooser.update_choose_col(self.__r1)

        return (self
                ._table[self._table["from"].isin(from_ids)]
                .groupby(by="from", as_index=False)
                .agg(self.__chooser.generate)
                .rename(columns={"to": named_as})
                .drop("weight", axis=1)
                )

#        small_tab = self._table[self._table[key_column].isin(keys)]
#        return small_tab.groupby(key_column).aggregate(
# self.__chooser.generate).drop("weight",axis=1)


# TODO: same thing: move this to concern-specific module
class ProductRelationship(WeightedRelationship):
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
        WeightedRelationship.__init__(self, **kwargs)
        self._products = products

    def select_one(self, **kwargs):
        chosen_products = WeightedRelationship.select_one(self, **kwargs)
        data_for_out = chosen_products.copy()

        print ("before retrieving products")
        print (chosen_products.head(15))
#        choices = choices.iloc[:, 0]
        for product in chosen_products.iloc[:, 1]:
            this_p_index = chosen_products[chosen_products == product].index

            # TODO: cf request from Gautier: refactor this as 2
            # separate relationships
            p_data = self._products[product].generate(size=len(this_p_index))
            for pdf in p_data.columns.values:
                data_for_out.loc[this_p_index, pdf] = p_data.loc[:,pdf].values

        print ("after retrieving products")
        print (data_for_out.head(15))

        return data_for_out


# TODO: if this one is really required, it should be in a "CDR" package,
# separated from the core
class AgentRelationship(WeightedRelationship):
    """

    """
    def __init__(self,
                 # TODO: ask Gautier: this seems never used => can we delete?
#                 agents=None,
                 **kwargs):
        """

        :param r1:
        :param r2:
        :param chooser:
        :param agents:
        :return:
        """
        WeightedRelationship.__init__(self, **kwargs)
#        self._agents = agents

    def select_one(self, **kwargs):
        choices = WeightedRelationship.select_one(self, **kwargs)
        # TODO: ask Gautier: why 1000 ? is it the number of initial
        # sim a agent has? shouldnt' this be a parameter ?
        choices["value"] = 1000

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
