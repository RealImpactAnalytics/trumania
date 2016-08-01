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

    def make_actions(self, ids, actorid_field_name, new_time_generator,
                     relationship):
        """

        :param new_time_generator:
        :param relationship:
        :param id1:
        :param id2:
        :return:
        """
        out = pd.DataFrame(columns=["new"])
        if len(ids) > 0:
            out = (relationship
                    .select_one(from_ids=ids, named_as="new")
                    .rename(columns={"from": actorid_field_name})
                   )
            if len(out.index) > 0:
                self._table.loc[out[actorid_field_name], "value"] = out[actorid_field_name].values
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
        self._table["value"] = 0
        #pd.Series(0, index=self._table.index, dtype=int)
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

    def make_actions(self, ids, actorid_field_name, relationship,id2,id3):
        """

        :param relationship: AgentRelationship
        :param id1: id of customer
        :param id2: id of Agent
        :param id3: id of Value
        :return:
        """
        out = pd.DataFrame(columns=[actorid_field_name])
        if len(ids) > 0:
            out = (relationship
                    .select_one(from_ids=ids, named_as=id2)
                    # TODO: ask Gautier: in AgentRelationship, the value is
                    # hardcoded to 1000 => quid?
                    .rename(columns={"from": actorid_field_name,
                                     id3: "VALUE"}))

            if out.shape[0] > 0:
                self._table.loc[out[actorid_field_name], "value"] += out["VALUE"]

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
        items = self.__stock.pop_one(from_ids=ids)
        self._table.loc[items.index,"value"] -= 1
        return items

    def add_item(self,ids,items):
        """

        :param ids:
        :param values:
        :return:
        """
        self.__stock.add_relations(from_ids=ids, to_ids=items)
        cpt = pd.Series(ids).value_counts()
        self._table.loc[cpt.index,"value"] += cpt.values

    def remove_item(self, ids, items):
        """

        :param ids:
        :param items:
        :return:
        """
        self.__stock.remove(from_ids=ids, to_ids=items)

    def stock(self):
        return self.__stock