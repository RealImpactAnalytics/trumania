import numpy as np


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

    def get_attribute_values(self, attribute_name, ids):
        """

        :param attribute_name:
        :param ids:
        :return:
        """

        return self._attributes[attribute_name].get_values(ids)

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
