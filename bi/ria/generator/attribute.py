from bi.ria.generator.operations import *


class Attribute(object):
    """
        Static actor attribute, with various ways to initialize it randomly
    """

    def __init__(self,

                 # if initializing with value, must provide ids and one of the
                 # init values
                 ids=None,
                 init_values=None,
                 init_values_generator=None,

                 # otherwise, we can also initialise randomly from a
                 # relationship (in which case the ids are extracted from the
                 # "from" field
                 relationship=None):
        """
        :param ids:
        :return:
        """

        if ids is not None:
            if not ((init_values is None) ^ (init_values_generator is None)):
                raise ValueError("if ids is provided, you must also provide "
                                 "init_values or init_values_generator")

            if init_values is None:
                init_values = init_values_generator.generate(size=len(ids))

            self._table = pd.DataFrame({"value": init_values}, index=ids)

        else:
            if relationship is None:
                raise ValueError("must provide either ids or relationship to "
                                 "initialize the attribute")

            self._table = (relationship
                           .select_one()
                           .set_index("from", drop=True)
                           .rename(columns={"to": "value"})
                           )

    def get_values(self, ids):
        """
        :param ids: actor ids for which the attribute values are desired
        :return: the current attribute values for those actors, as Series
        """
        r = self._table.loc[ids]["value"]
        return r

    def update(self, ids, values):
        """

        :param values:
        :param ids:
        :return:
        """

        self._table.loc[ids, "value"] = values


class StockAttribute(Attribute):
    """
        A stock attribute keeps the stock level of some quantity and relies
        on a "provider" relationship for each actor to be able to obtain topups
    """

    def __init__(self, trigger_generator, **kwargs):
        """

        :param ids:
        :param trigger_generator: Random Generator that returns 1 or 0 depending on 1 value (stock, and parameters)
        Usually  a check vs a logistic regression
        :return:
        """
        Attribute.__init__(self, **kwargs)
        self._trigger = trigger_generator

    def init_clock(self,new_time_generator):
        """

        :param new_time_generator:
        :return:
        """
        self._table["clock"] = new_time_generator.generate(size=len(self._table.index))

    # only actions have clocks: not attributes

    # this is associated with an action that has a logistic clock

    # this re-uses the incrementAttributesOp below + other operation: set to
    # zero some other clocks, based on weight

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

    # TODO: make this an impact like "buy recharge"

    # same as choiceOp, except the second is increment attribute instead
    #  of setAttributeOp (both based on column names)
    def make_actions(self, ids, actorid_field_name, relationship, id2):
        """

        :param relationship: AgentRelationship
        :param id1: id of customer
        :param id2: id of Agent
        :param id3: id of Value
        :return:
        """

        if len(ids) == 0:
            return [], pd.DataFrame(columns=[actorid_field_name, "new"])

        out = (relationship
               .select_one(from_ids=ids, named_as=id2)
               .rename(columns={"from": actorid_field_name,
                                "value": "VALUE"}))

        if out.shape[0] > 0:
            self._table.loc[out[actorid_field_name], "value"] += out["VALUE"]

        return [], out


class LabeledStockAttribute(Attribute):
    """Transient Attribute where users own some stock of labeled items

    """
    def __init__(self, relationship, **kwargs):
        """

        :param ids:
        :param relationship: Relationship object. Needs to have an "AGENT" and an "ITEM" field. No weights.
        :return:
        """
        Attribute.__init__(self, **kwargs)
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
