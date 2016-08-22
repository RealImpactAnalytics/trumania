from datagenerator.operations import *
from datagenerator.relationship import *


class Attribute(object):
    """
        Static actor attribute, with various ways to initialize it randomly
    """

    def __init__(self,
                 actor,

                 # if initializing with value, must provide ids and one of the
                 # init values
                 init_values=None,
                 init_values_gen=None,

                 # otherwise, we can also initialise randomly from a
                 # relationship (in which case the ids are extracted from the
                 # "from" field. init_relationship is a string that contains
                 # the name of the
                 init_relationship=None):
        """
        :param ids:
        :return:
        """

        if init_relationship is None:
            if not ((init_values is None) ^ (init_values_gen is None)):
                raise ValueError("if init_relationship is not provided, "
                                 "you must also provide init_values or "
                                 "init_values_gen")

            elif init_values is None:
                init_values = init_values_gen.generate(size=actor.size)

            self._table = pd.DataFrame({"value": init_values}, index=actor.ids)

        else:
            if init_relationship is None:
                raise ValueError("must provide either ids or relationship to "
                                 "initialize the attribute")

            self._table = (actor.get_relationship(init_relationship)
                           .select_one()
                           .set_index("from", drop=True)
                           .rename(columns={"to": "value"}))

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


class MultiAttribute(Attribute):
    """
        Attribute that can hold several values per ids (e.g. a list of SIM ids).
        Internally, mostly everything is delegated to a relationship.
    """
    def __init__(self, seed, **kwargs):
        """
        :return:
        """
        Attribute.__init__(self, init_values=0, **kwargs)
        self.values = Relationship(seed=seed)
        self.ops = self.MultiAttributeOps(self)

    def add(self, actor_ids, items):
        """

        :param actor_ids:
        :param items:
        :return:
        """
        self.values.add_relations(from_ids=actor_ids, to_ids=items)
        cpt = pd.Series(actor_ids).value_counts()
        self._table.loc[cpt.index, "value"] += cpt.values

    def remove(self, actor_ids, items):
        """

        :param actor_ids:
        :param items:
        :return:
        """

        self.values.remove(from_ids=actor_ids, to_ids=items)
        cpt = pd.Series(actor_ids).value_counts()
        self._table.loc[cpt.index, "value"] -= cpt.values

    def pop(self, actor_ids):
        """

        :param actor_ids:
        :return:
        """

        taken = self.values.select_one(from_ids=actor_ids, drop=True)
        cpt = pd.Series(actor_ids).value_counts()
        self._table.loc[cpt.index, "value"] -= cpt.values
        return taken

    def stock(self):
        return self.values

    class MultiAttributeOps(object):
        def __init__(self, labeled_stock):
            self.m_attribute = labeled_stock

        class Add(SideEffectOnly):
            def __init__(self, m_attribute, actor_id_field, item_field):
                self.m_attribute = m_attribute
                self.actor_id_field = actor_id_field
                self.item_field = item_field

            def side_effect(self, action_data):
                if action_data.shape[0] > 0:
                    self.m_attribute.add(
                        actor_ids=action_data[self.actor_id_field],
                        items=action_data[self.item_field])

        def add(self, actor_id_field, item_field):
            return self.Add(self.m_attribute, actor_id_field, item_field)

        class Remove(SideEffectOnly):
            def __init__(self, m_attribute, actor_id_field, item_field):
                self.m_attribute = m_attribute
                self.actor_id_field = actor_id_field
                self.item_field = item_field

            def side_effect(self, action_data):
                if action_data.shape[0] > 0:
                    self.m_attribute.remove(
                        actor_ids=action_data[self.actor_id_field],
                        items=action_data[self.item_field])

        def remove(self, actor_id_field, item_field):
            return self.Add(self.m_attribute, actor_id_field, item_field)

        class PopOne(Operation):
            def __init__(self, m_attribute, actor_id_field, named_as):
                Operation.__init__(self)
                self.m_attribute = m_attribute
                self.actor_id_field = actor_id_field
                self.named_as = named_as

            def transform(self, action_data):
                if action_data.shape[0] > 0:
                    taken = self.m_attribute.pop(
                        actor_ids=action_data[self.actor_id_field])

                    # TODO: this is mostly copy-paste from Relationship :(
                    taken.rename(columns={"to": self.named_as}, inplace=True)
                    # saves index as a column to have an explicit column that will
                    # survive the join below
                    action_data["index_backup"] = action_data.index

                    merged = pd.merge(left=action_data, right=taken,
                                      left_on=self.actor_id_field, right_on="from")

                    merged.drop("from", axis=1, inplace=True)

                    # puts back the index in place, for further processing
                    merged.set_index("index_backup", inplace=True)

                    return merged

        def pop_one(self, actor_id_field, named_as):
                return self.PopOne(self.m_attribute, actor_id_field, named_as)
