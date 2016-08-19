from datagenerator.relationship import *
from datagenerator.random_generators import *


class Actor(object):
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
        self.size = len(self.ids)
        self.relationships = {}

    def create_relationship(self, name, seed):
        """
        creates an empty relation ship from this actor
        """

        if name is self.relationships:
            raise ValueError("cannot create a second relationship with "
                             "existing name {}".format(name))

        self.relationships[name] = Relationship(seed=seed)
        return self.relationships[name]

    def get_relationship(self, name):
        return self.relationships[name]

    def add_attribute(self, name, attr):
        self._attributes[name] = attr

    def get_attribute(self, attribute_name):
        return self._attributes[attribute_name]

    def get_attribute_values(self, attribute_name, ids):
        """
        :return: the values of this attribute, as a Series
        """

        return self.get_attribute(attribute_name).get_values(ids)

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

    def attribute_names(self):
        return self._attributes.keys()

    def to_dataframe(self):
        """
        :return: all the attributes of this actor as one single dataframe
        """
        df = pd.DataFrame(index=self.ids)

        for name in self.attribute_names():
            df[name] = self.get_attribute_values(name, self.ids)

        return df

    class ActorOps(object):
        def __init__(self, actor):
            self.actor = actor

        class Lookup(AddColumns):
            def __init__(self, actor, actor_id_field, select_dict):
                AddColumns.__init__(self)
                self.actor = actor
                self.actor_id_field = actor_id_field
                self.select_dict = select_dict

            def build_output(self, action_data):

                output = action_data[[self.actor_id_field]]
                for attribute, named_as in self.select_dict.items():

                    actor_ids = action_data[self.actor_id_field].unique()
                    vals = pd.DataFrame(
                        self.actor.get_attribute_values(attribute, actor_ids),
                        )
                    vals.rename(columns={"value": named_as}, inplace=True)

                    output = pd.merge(left=output, right=vals,
                                      left_on=self.actor_id_field,
                                      right_index=True)

                # self.actor_id_field is already in the parent result, we only
                # want to return the new columns from here
                output.drop(self.actor_id_field, axis=1, inplace=True)
                return output

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

            def side_effect(self, action_data):
                if action_data.shape[0] > 0:
                    attribute = self.actor.get_attribute(self.attribute_name)
                    attribute.update(ids=action_data.index.values,
                                     values=action_data[self.copy_from_field])

        def overwrite(self, attribute, copy_from_field):
            """
            Overwrite the value of this attribute with values in this field
            """
            return self.Overwrite(self.actor, attribute, copy_from_field)