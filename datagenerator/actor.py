from datagenerator.random_generators import *
from datagenerator.attribute import *


class Actor(object):
    def __init__(self, size, id_start=0, prefix="id_", max_length=10):
        """

        :param size:
        :param id_start:
        :return:
        """
        self.ids = build_ids(size, id_start, prefix, max_length)
        self._attributes = {}
        self.ops = self.ActorOps(self)
        self.size = size
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

    def create_attribute(self, name, **kwargs):
        self._attributes[name] = Attribute(actor=self, **kwargs)
        return self._attributes[name]

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
                if action_data.shape[0] == 0:
                    return pd.DataFrame()
                elif is_sequence(action_data.iloc[0][self.actor_id_field]):
                    return self._lookup_by_sequences(action_data)
                else:
                    return self._lookup_by_scalars(action_data)

            def _lookup_by_scalars(self, action_data):
                """
                looking up, after we know the ids are not sequences of ids
                """

                output = action_data[[self.actor_id_field]]
                actor_ids = action_data[self.actor_id_field].unique()

                for attribute, named_as in self.select_dict.items():
                    vals = pd.DataFrame(
                        self.actor.get_attribute_values(attribute, actor_ids))
                    vals.rename(columns={"value": named_as}, inplace=True)

                    output = pd.merge(left=output, right=vals,
                                      left_on=self.actor_id_field,
                                      right_index=True)

                # self.actor_id_field is already in the parent result, we only
                # want to return the new columns from here
                output.drop(self.actor_id_field, axis=1, inplace=True)
                return output

            def _lookup_by_sequences(self, action_data):

                # pd.Series containing seq of ids to lookup
                id_lists = action_data[self.actor_id_field]

                # unique actor ids of the attribute to look up
                actor_ids = np.unique(reduce(lambda l1, l2: l1 + l2, id_lists))

                output = pd.DataFrame(index=action_data.index)
                for attribute, named_as in self.select_dict.items():
                    vals = self.actor.get_attribute_values(attribute, actor_ids)

                    def attributes_of_ids(ids):
                        "return: list of attribute values for those actor ids"
                        return vals.loc[ids].tolist()

                    output[named_as] = id_lists.map(attributes_of_ids)

                return output

        def lookup(self, actor_id_field, select):
            """
            Looks up some attribute values by joining on the specified field
            of the current data

            :param actor_id_field: field name in the action_data.
              If the that column contains lists, then it's assumed to contain
              only list and it's flatten to obtain the list of id to lookup
              in the attribute. Must be a list of "scalar" values or list of
              list, not a mix of both.

            :param select: dictionary of (attribute_name -> given_name)
            specifying which attribute to look up and which name to give to
            the resulting column

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