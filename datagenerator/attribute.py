from datagenerator.operations import *


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
        self.ops = self.LabeledStock(self)

    def add_item(self, ids, items):
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

    class LabeledStock(object):
        def __init__(self, labeled_stock):
            self.labeled_stock = labeled_stock

        class AddItem(SideEffectOnly):
            def __init__(self, labeled_stock, actor_id_field, item_field):
                self.labeled_stock = labeled_stock
                self.actor_id_field = actor_id_field
                self.item_field = item_field

            def side_effect(self, data):
                if data.shape[0] > 0:
                    self.labeled_stock.add_item(
                        ids=data[self.actor_id_field],
                        items=data[self.item_field])

        def add_item(self, actor_id_field, item_field):
            return self.AddItem(self.labeled_stock, actor_id_field,
                                item_field)

        class RemoveItem(SideEffectOnly):
            def __init__(self, labeled_stock, actor_id_field, item_field):
                self.labeled_stock = labeled_stock
                self.actor_id_field = actor_id_field
                self.item_field = item_field

            def side_effect(self, data):
                if data.shape[0] > 0:
                    self.labeled_stock.remove_item(
                        ids=data[self.actor_id_field],
                        items=data[self.item_field])

        def remove_item(self, actor_id_field, item_field):
            return self.AddItem(self.labeled_stock, actor_id_field,
                                item_field)
