import pandas as pd
import logging
from trumania.core.operations import SideEffectOnly


class Attribute(object):
    """
        Static population attribute, with various ways to initialize it randomly
    """

    def __init__(self,
                 population,

                 # if initializing with value, must provide ids and one of the
                 # init values
                 init_values=None,
                 init_gen=None,

                 # otherwise, we can also initialise randomly from a
                 # relationship (in which case the ids are extracted from the
                 # "from" field. init_relationship is a string that contains
                 # the name of the
                 init_relationship=None):
        self.ops = self.AttributeOps(self)

        if population.size == 0:
            self._table = pd.DataFrame(columns=["value"])

        elif init_relationship is None:
            if not ((init_values is None) ^ (init_gen is None)):
                raise ValueError("if init_relationship is not provided, "
                                 "you must also provide init_values or "
                                 "init_values_gen")

            elif init_values is None:
                init_values = init_gen.generate(size=population.size)

            if type(init_values) == pd.Series:
                logging.warn("  Trying to create attribute with a series "
                             "but indices will be lost.")
                init_values = init_values.tolist()

            self._table = pd.DataFrame({"value": init_values}, index=population.ids)

        else:
            if init_relationship is None:
                raise ValueError("must provide either ids or relationship to "
                                 "initialize the attribute")

            self._table = population.get_relationship(init_relationship).select_one()
            self._table.set_index("from", drop=True, inplace=True)
            self._table.rename(columns={"to": "value"}, inplace=True)

    def get_values(self, ids=None):
        """
        :param ids: members ids for which the attribute values are desired
        :return: the current attribute values for those members, as Series
        """
        if ids is None:
            return self._table["value"]
        else:
            return self._table.loc[ids]["value"]

    def update(self, series):
        """
        updates or adds values of this attributes from the values of the provided
        series, using its index as member id
        """
        self._table = self._table.reindex(self._table.index | series.index)
        self._table.loc[series.index, "value"] = series.values

    def add(self, ids, added_values):
        """
        This only makes sense for attributes that support a + operation (e.g. numerical values or list)
        : this simply performs a += operation
        """
        assert len(ids) == len(added_values)

        # putting together any add to the same attribute id
        to_add = pd.Series(added_values, index=ids).groupby(level=0).agg(sum)

        self._table.loc[to_add.index, "value"] = self._table.loc[to_add.index, "value"] + to_add

    def transform_inplace(self, f):
        """
        transform the values of this attribute inplace with f
        """
        self._table["value"] = self._table["value"].map(f)

    ############
    # IO
    def save_to(self, file_path):
        logging.info("saving attribute to {}".format(file_path))
        self._table.to_csv(file_path)

    @staticmethod
    def load_from(file_path):
        table = pd.read_csv(file_path, index_col=0)

        # we're basically hacking our own constructor, feeding it fake data
        # so it's initialized correctly.
        #
        # Don't do that outside this class!
        class FakePopulation(object):
            def __init__(self):
                self.size = table.shape[0]
                self.ids = table.index

        return Attribute(population=FakePopulation(), init_values=table["value"])

    ############
    # operations

    class AttributeOps(object):
        def __init__(self, attribute):
            self.attribute = attribute

        class Update(SideEffectOnly):
            def __init__(self, attribute, member_id_field, copy_from_field):
                self.attribute = attribute
                self.copy_from_field = copy_from_field
                self.member_id_field = member_id_field

            def side_effect(self, story_data):
                if story_data.shape[0] > 0:
                    update_series = pd.Series(
                        data=story_data[self.copy_from_field].values,
                        index=story_data[self.member_id_field].values)
                    self.attribute.update(update_series)

        def update(self, member_id_field, copy_from_field):
            """
            Overwrite the value of this attribute with values in this field

            :param member_id_field: name of the field of the story data
                containing the member ids whose attribute should be updated
            :param copy_from_field: name of the field of the story data
                containing the new values of the attribute
            :return:
            """
            return self.Update(self.attribute, member_id_field,
                               copy_from_field)

        class Add(SideEffectOnly):
            def __init__(self, attribute, member_id_field,
                         added_value_field, subtract):
                self.attribute = attribute
                self.added_value_field = added_value_field
                self.member_id_field = member_id_field
                self.subtract = subtract

            def side_effect(self, story_data):
                if story_data.shape[0] > 0:

                    values = story_data[self.added_value_field].values
                    if self.subtract:
                        values = -values

                    self.attribute.add(
                        ids=story_data[self.member_id_field].values,
                        added_values=values)

        def add(self, member_id_field, added_value_field):
            return self.Add(self.attribute, member_id_field, added_value_field, subtract=False)

        def subtract(self, member_id_field, subtracted_value_field):
            return self.Add(self.attribute, member_id_field, subtracted_value_field, subtract=True)
