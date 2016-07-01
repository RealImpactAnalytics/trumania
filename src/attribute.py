import numpy as np
import pandas as pd
from relationship import Relationship

class TransientAttribute(object):
    """

    """
    def __init__(self, ids):
        """

        :param ids:
        :return:
        """
        self._table = pd.DataFrame({ "value": "", "clock": 0, "activity":1.},index=ids)

    def update(self, ids_to_update, values):
        """

        :param values:
        :param ids_to_update:
        :return:
        """
        self._table.loc[ids_to_update, "value"] = values

    def set_activity(self, ids, activity):
        """

        :param ids:
        :param activity:
        :return:
        """
        self._table.loc[ids, "activity"] = activity

    def init_clock(self,new_time_generator):
        """

        :param new_time_generator:
        :return:
        """
        self._table["clock"] = new_time_generator.generate(self._table["activity"])

    def who_acts_now(self):
        """

        :return:
        """
        return self._table[self._table["clock"] == 0]

    def update_clock(self, decrease=1):
        """

        :param decrease:
        :return:
        """
        self._table["clock"] -= decrease


class ChoiceAttribute(TransientAttribute):
    """

    """

    def make_actions(self, new_time_generator, relationship, id1, id2):
        """

        :param new_time_generator:
        :param relationship:
        :param id1:
        :param id2:
        :return:
        """
        act_now = self.who_acts_now()
        out = pd.DataFrame(columns=["new"])
        if len(act_now.index) > 0:
            out = relationship.select_one(id1,act_now.index.values).rename(columns={id2:"new"})
            if len(out.index) > 0:
                self._table.loc[act_now.index, "value"] = out["new"].values
            self._table.loc[act_now.index, "clock"] = new_time_generator.generate(act_now["activity"])+1
        self.update_clock()
        out.reset_index(inplace=True)
        return out


class StockAttribute(TransientAttribute):
    """

    """

    def __init__(self, ids, trigger_generator):
        """

        :param ids:
        :param trigger_generator: Random Generator that returns 1 or 0 depending on 1 value (stock, and parameters)
        Usually  a check vs a logistic regression
        :return:
        """
        TransientAttribute.__init__(self, ids)
        self._table["value"] = pd.Series(0, index=self._table.index, dtype=int)
        self._trigger = trigger_generator

    def init_clock(self,new_time_generator):
        """

        :param new_time_generator:
        :return:
        """
        self._table["clock"] = new_time_generator.generate(len(self._table.index))

    def decrease_stock(self, values):
        """

        :param values: Pandas Series
        :param new_time_generator:
        :return:
        """
        self._table.loc[values.index,"value"] -= values.values

        triggers = self._trigger.generate(self._table.loc[values.index,"value"])
        small_table = self._table.loc[values.index]
        act_now = small_table[triggers]
        if len(act_now.index)>0:
            self._table.loc[act_now.index, "clock"] = 0

    def make_actions(self,relationship,id1,id2,id3):
        """

        :param relationship: AgentRelationship
        :param id1: id of customer
        :param id2: id of Agent
        :param id3: id of Value
        :return:
        """
        act_now = self.who_acts_now()
        out = pd.DataFrame(columns=["new"])
        if len(act_now.index) > 0:
            out = relationship.select_one(id1,act_now.index.values).rename(columns={id2:"AGENT",id3:"VALUE"})
            if len(out.index) > 0:
                self._table.loc[out.index,"value"] += out["VALUE"]

        out.reset_index(inplace=True)
        self.update_clock()
        return out


class LabeledStockAttribute(TransientAttribute):
    """Transient Attribute where users own some stock of labeled items

    """
    def  __init__(self, ids,chooser):
        """

        :param ids:
        :param chooser:
        :return:
        """
        TransientAttribute.__init__(self,ids)
        self.__stock = Relationship("AGENT","ITEM",chooser)

        self.update(ids,0)

    def get_item(self,ids):
        """

        :param ids:
        :return:
        """
        items = self.__stock.pop_one("AGENT",ids)
        self._table.loc[items.index,"value"] -= 1
        return items

    def add_item(self,ids,values):
        """

        :param ids:
        :param values:
        :return:
        """
        self.__stock.add_relation("AGENT",ids,"ITEM",values)
