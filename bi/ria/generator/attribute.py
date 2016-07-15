import pandas as pd


class TransientAttribute(object):
    """

    """
    def __init__(self, ids):
        """

        :param ids:
        :return:
        """
        self._table = pd.DataFrame({ "value": ""},index=ids)

    def update(self, ids_to_update, values):
        """

        :param values:
        :param ids_to_update:
        :return:
        """
        self._table.loc[ids_to_update, "value"] = values


class ChoiceAttribute(TransientAttribute):
    """

    """

    def make_actions(self, ids, new_time_generator, relationship, id1, id2):
        """

        :param new_time_generator:
        :param relationship:
        :param id1:
        :param id2:
        :return:
        """
        out = pd.DataFrame(columns=["new"])
        if len(ids) > 0:
            out = relationship.select_one(id1,ids.values).rename(columns={id2:"new"})
            if len(out.index) > 0:
                self._table.loc[out.index, "value"] = out["new"].values
        out.reset_index(inplace=True)
        return ids, out, None


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
        self._table["clock"] = new_time_generator.generate(size=len(self._table.index))

    def decrease_stock(self, values):
        """

        :param values: Pandas Series
        :param new_time_generator:
        :return:
        """
        self._table.loc[values.index,"value"] -= values.values

        triggers = self._trigger.generate(weights=self._table.loc[values.index,"value"])
        small_table = self._table.loc[values.index]
        act_now = small_table[triggers]

        return act_now.index

    def make_actions(self,ids,relationship,id1,id2,id3):
        """

        :param relationship: AgentRelationship
        :param id1: id of customer
        :param id2: id of Agent
        :param id3: id of Value
        :return:
        """
        out = pd.DataFrame(columns=["new"])
        if len(ids) > 0:
            out = relationship.select_one(id1,ids.values).rename(columns={id2:"AGENT",id3:"VALUE"})
            if len(out.index) > 0:
                self._table.loc[out.index,"value"] += out["VALUE"]

        out.reset_index(inplace=True)
        return [], out, None


class LabeledStockAttribute(TransientAttribute):
    """Transient Attribute where users own some stock of labeled items

    """
    def __init__(self, ids,relationship):
        """

        :param ids:
        :param relationship: Relationship object. Needs to have an "AGENT" and an "ITEM" field. No weights.
        :return:
        """
        TransientAttribute.__init__(self,ids)
        self.__stock = relationship

        self.update(ids,0)

    def get_item(self,ids):
        """

        :param ids:
        :return:
        """
        items = self.__stock.pop_one("AGENT",ids)
        self._table.loc[items.index,"value"] -= 1
        return items

    def add_item(self,ids,items):
        """

        :param ids:
        :param values:
        :return:
        """
        self.__stock.add_relation("AGENT",ids,"ITEM",items)
        cpt = pd.Series(ids).value_counts()
        self._table.loc[cpt.index,"value"] += cpt.values

    def remove_item(self,ids,items):
        """

        :param ids:
        :param items:
        :return:
        """
        self.__stock.remove("AGENT",ids,"ITEM",items)

    def stock(self):
        return self.__stock
