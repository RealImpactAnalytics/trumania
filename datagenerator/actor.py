import numpy as np
from datagenerator.operations import *


class Actor(object):
    """

    """

    def __init__(self, size, id_start=0, prefix="", max_length=10):
        """

        :param size:
        :param id_start:
        :return:
        """
        self.ids = [prefix + str(x).zfill(max_length) for x in np.arange(
            id_start, id_start + size)]
        self._attributes = {}
        self.ops = self.ActorOps(self)

    def size(self):
        return len(self.ids)

    def add_attribute(self, name, attr):
        self._attributes[name] = attr

    def make_attribute_action(self, attr_name, actorid_field_name, ids, params):
        """

        :param attr_name:
        :param params:
        :return:
        """
        if self._attributes.has_key(attr_name):
            return self._attributes[attr_name].make_actions(
                ids=ids, actorid_field_name=actorid_field_name, **params)
        else:
            raise Exception("No transient attribute named %s" % attr_name)

    def apply_to_attribute(self,attr_name,function,params):
        if self._attributes.has_key(attr_name):
            return getattr(self._attributes[attr_name], function)(**params)
        else:
            raise Exception("No transient attribute named %s" % attr_name)

    def get_attribute(self, attribute_name):
        return self._attributes[attribute_name]

    def get_attribute_values(self, attribute_name, ids):
        """
        :return: the values of this attribute, as a Series
        """

        return self.get_attribute(attribute_name).get_values(ids)

    # TODO: refactor this as a lambda or np.nfunc
    def check_attributes(self, ids, field, condition, threshold):
        """

        :param ids:
        :param field:
        :param condition:
        :param threshold:
        :return:
        """

        attr = self.get_attribute_values(ids, field)

        if condition == ">":
            return attr > threshold
        if condition == ">=":
            return attr >= threshold
        if condition == "<":
            return attr < threshold
        if condition == "<=":
            return attr <= threshold
        if condition == "==":
            return attr == threshold
        raise Exception("Unknown condition : %s" % condition)

    class ActorOps(object):
        def __init__(self, actor):
            self.actor = actor

        class Lookup(AddColumns):
            def __init__(self, actor, actor_id_field, select_dict):
                AddColumns.__init__(self)
                self.actor = actor
                self.actor_id_field = actor_id_field
                self.select_dict = select_dict

            def build_output(self, data):

                output = data[[self.actor_id_field]]
                for attribute, named_as in self.select_dict.items():

                    actor_ids = data[self.actor_id_field].unique()
                    vals = pd.DataFrame(
                        self.actor.get_attribute_values(attribute, actor_ids),
                        ).rename(columns={"value": named_as})

                    output = pd.merge(left=output, right=vals,
                                      left_on=self.actor_id_field,
                                      right_index=True)

                # self.actor_id_field is already in the parent result, we only
                # want to return the new columns from here
                return output.drop(self.actor_id_field, axis=1)

        def lookup(self, actor_id_field, select):
            """
            Looks up some attribute values by joining on the specified field
            of the current data
            """
            return self.Lookup(self.actor, actor_id_field, select)

        class Overwrite(SideEffectOnly):
            def __init__(self, actor, attribute, copy_from_field):
                self.actor=actor
                self.attribute_name = attribute
                self.copy_from_field = copy_from_field

            def side_effect(self, data):
                if data.shape[0] > 0:
                    attribute = self.actor.get_attribute(self.attribute_name)
                    attribute.update(ids=data.index.values,
                                     values=data[self.copy_from_field])

        def overwrite(self, attribute, copy_from_field):
            """
            Overwrite the value of this attribute with values in this field
            """
            return self.Overwrite(self.actor, attribute, copy_from_field)
