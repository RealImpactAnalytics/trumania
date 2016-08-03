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

            random_init = relationship.select_one()
            self._table = pd.DataFrame({"value": random_init["to"]},
                                       index=random_init["from"])


    def get_values(self, ids):
        """
        :param ids: actor ids for which the attribute values are desired
        :return: the current attribute values for those actors
        """
        return self._table.loc[ids]["value"].values


class TransientAttribute(Attribute):
    """
        Actor attribute with method allowing to update the values during the
        data generation.
    """

    def __init__(self, **kwargs):
        Attribute.__init__(self, **kwargs)
        self.ops = self.AttributeOps(self)

    def update(self, ids, values):
        """

        :param values:
        :param ids:
        :return:
        """

        # print ("table: {}".format(self._table))
        # print ("ids: {}".format(ids))

        self._table.loc[ids, "value"] = values

    class AttributeOps(object):
        def __init__(self, attribute):
            self.attribute = attribute

        class Overwrite(Operation):
            def __init__(self, attribute, copy_from_field):
                self.attribute = attribute
                self.copy_from_field = copy_from_field

            def transform(self, data):
                # print ("current data: {}".format(data))

                if data.shape[0] > 0:
                    self.attribute.update(ids=data.index.values,
                                          values=data[self.copy_from_field])

                # input data is returned as is, we merely update values here
                return data

        def overwrite(self, copy_from_field):
            """
            Overwrite the value of this attribute with values in this field
            """
            return self.Overwrite(self.attribute, copy_from_field)


# class ChoiceAttribute(TransientAttribute):
#     """
#         Actor attribute that simply keep a "current value" for each actor,
#         randomly picked among the "to" side of the configured relationship.
#     """
#
#     def __init__(self, relationship):
#
#         init_values = relationship.select_one()
#         print "init_values {}".format(init_values)
#         TransientAttribute.__init__(self, ids=init_values["from"],
#                                     init_values=init_values["to"])
#
#
#         print "init_tale {}".format(self._table)
#        self.relationship = relationship

    # 2 operations:
    #  - relationshipOperation (used everywhere where we traverse a rel)
    #   => this one comes from the relationship class
    #  - setAttributeOp: to set the location: this one is an operation
    #  coming from the TransientAttribute: cf update method

    # + 1 emission: the mobility log: select,...
    # def make_actions(self, ids, actorid_field_name):
    #     """
    #
    #     :param new_time_generator:
    #     :param relationship:
    #     :param id1:
    #     :param id2:
    #     :return:
    #     """
    #
    #     if len(ids) == 0:
    #         return ids, pd.DataFrame(columns=[actorid_field_name, "new"])
    #
    #     out = (self.relationship
    #            .select_one(from_ids=ids, named_as="new")
    #            .rename(columns={"from": actorid_field_name}))
    #
    #     if out.shape[0] > 0:
    #         # TODO: bug here? I think we should .ix[] with the current way of
    #         #  indexing
    #         self._table.loc[out[actorid_field_name], "value"] = out[actorid_field_name].values
    #
    #     return ids, out


class StockAttribute(TransientAttribute):
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
        TransientAttribute.__init__(self, **kwargs)
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
