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
        return self._table.loc[ids]["value"]

    def update(self, ids, values):
        """
        :param values:
        :param ids:
        :return:
        """
        self._table.loc[ids, "value"] = values
