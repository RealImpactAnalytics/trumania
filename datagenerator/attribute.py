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
                 init_gen=None,

                 # otherwise, we can also initialise randomly from a
                 # relationship (in which case the ids are extracted from the
                 # "from" field. init_relationship is a string that contains
                 # the name of the
                 init_relationship=None):
        """
        :param ids:
        :return:
        """
        self.ops = self.AttributeOps(self)

        if init_relationship is None:
            if not ((init_values is None) ^ (init_gen is None)):
                raise ValueError("if init_relationship is not provided, "
                                 "you must also provide init_values or "
                                 "init_values_gen")

            elif init_values is None:
                init_values = init_gen.generate(size=actor.size)

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
        return self._table.loc[ids]["value"]

    def update(self, ids, values):
        """
        :param values:
        :param ids:
        :return:
        """
        self._table.loc[ids, "value"] = values

    def add(self, ids, added_values):
        """
        This only makes sense for attributes that support a + operation (e.g. numerical values or list)
        : this simply performs a += operation
        """
        assert len(ids) == len(added_values)

        # putting together any add to the same attribute id
        to_add = pd.Series(added_values, index=ids).groupby(level=0).agg(sum)

        self._table.loc[to_add.index, "value"] = self._table.loc[to_add.index, "value"] + to_add

    class AttributeOps(object):
        def __init__(self, attribute):
            self.attribute = attribute

        class Overwrite(SideEffectOnly):
            def __init__(self, attribute, actor_id_field,
                         copy_from_field):
                self.attribute = attribute
                self.copy_from_field = copy_from_field
                self.actor_id_field = actor_id_field

            def side_effect(self, action_data):
                if action_data.shape[0] > 0:
                    self.attribute.update(
                        ids=action_data[self.actor_id_field].values,
                        values=action_data[self.copy_from_field].values)

        def overwrite(self, actor_id_field, copy_from_field):
            """
            Overwrite the value of this attribute with values in this field

            :param actor_id_field: name of the field of the action data
                containing the actor ids whose attribute should be updated
            :param copy_from_field: name of the field of the action data
                containing the new values of the attribute
            :return:
            """
            return self.Overwrite(self.attribute, actor_id_field,
                                  copy_from_field)

        class Add(SideEffectOnly):
            def __init__(self, attribute, actor_id_field,
                         added_value_field, subtract):
                self.attribute = attribute
                self.added_value_field = added_value_field
                self.actor_id_field = actor_id_field
                self.subtract = subtract

            def side_effect(self, action_data):
                if action_data.shape[0] > 0:

                    values = action_data[self.added_value_field].values
                    if self.subtract:
                        values = -values

                    self.attribute.add(
                        ids=action_data[self.actor_id_field].values,
                        added_values=values)

        def add(self, actor_id_field, added_value_field):
            return self.Add(self.attribute, actor_id_field, added_value_field, subtract=False)

        def subtract(self, actor_id_field, subtracted_value_field):
            return self.Add(self.attribute, actor_id_field, subtracted_value_field, subtract=True)

