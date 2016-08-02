import pandas as pd


class Attribute(object):
    """
        Static actor attribute
    """

    def __init__(self, ids, init_values=None, init_values_generator=None):
        """
        :param ids:
        :return:
        """

        if not ((init_values is None) ^ (init_values_generator is None)):
            raise ValueError("Must pass exactly one of init_values or "
                             "init_values_generator arguments")

        if init_values is None:
            init_values = init_values_generator.generate(size=len(ids))

        # TODO: we can probably replace this with a Series
        self._table = pd.DataFrame({"value": init_values}, index=ids)

    def get_values(self, ids):
        """
        :param ids: actor ids for which the attribute values are desired
        :return: the current attribute values for those actors
        """
        return self._table.ix[ids, "value"].values


class TransientAttribute(Attribute):
    """
        Actor attribute with method allowing to update the values during the
        data generation.
    """

    def __init__(self, **kwargs):
        Attribute.__init__(self, **kwargs)

    def update(self, ids_to_update, values):
        """

        :param values:
        :param ids_to_update:
        :return:
        """
        # TODO:  bug here (and elsewhere in this class: confusion between
        # access by id and by location.. )
        self._table.loc[ids_to_update, "value"] = values


class ChoiceAttribute(TransientAttribute):
    """

    """

    def __init__(self, **kwargs):
        TransientAttribute.__init__(self, **kwargs)

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

    def __init__(self, trigger_generator, **kwargs):
        """

        :param ids:
        :param trigger_generator: Random Generator that returns 1 or 0 depending on 1 value (stock, and parameters)
        Usually  a check vs a logistic regression
        :return:
        """
        TransientAttribute.__init__(self, **kwargs)
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
        self._table.loc[values.index, "value"] -= values.values

        triggers = self._trigger.generate(weights=self._table.loc[values.index,"value"])
        small_table = self._table.loc[values.index]
        act_now = small_table[triggers]

        return act_now.index

    def make_actions(self, ids, actorid_field_name, relationship, id2):
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
                   .rename(columns={"from": actorid_field_name,
                                    "value": "VALUE"}))

            if out.shape[0] > 0:
                self._table.loc[out[actorid_field_name], "value"] += out["VALUE"]

        out.reset_index(inplace=True)
        return [], out, None


class LabeledStockAttribute(TransientAttribute):
    """Transient Attribute where users own some stock of labeled items

    """
    def __init__(self, relationship, **kwargs):
        """

        :param ids:
        :param relationship: Relationship object. Needs to have an "AGENT" and an "ITEM" field. No weights.
        :return:
        """
        TransientAttribute.__init__(self, **kwargs)
        self.__stock = relationship

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

        # TODO: bug here? I think we should update self._table
        self.__stock.remove(from_ids=ids, to_ids=items)

    def stock(self):
        return self.__stock
