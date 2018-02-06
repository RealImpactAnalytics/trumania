import pandas as pd
import logging
import numpy as np
import os
import functools

from trumania.core.operations import AddColumns, SideEffectOnly
from trumania.core.relationship import Relationship
from trumania.core.attribute import Attribute
from trumania.core.util_functions import make_random_assign, ensure_non_existing_dir, is_sequence
from trumania.core import random_generators


class Population(object):
    def __init__(self, circus, ids_gen=None, size=None, ids=None):
        """
        :param ids_gen: generator for the ids of the members of this population
        :param size: number of ids to generate (only relevant if is ids_gen
        is specified
        :param ids: if neither ids_gen nore size is specified, we can
          also specify the ids explicitly
        :return:
        """
        self.circus = circus

        if ids is not None:
            if ids_gen is not None or size is not None:
                raise ValueError("cannot specify ids_gen nor size if ids is "
                                 "provided")
            self.ids = pd.Index(ids)
            self.size = len(ids)

        else:
            if size == 0:
                self.ids = pd.Index([])

            elif ids_gen is not None and size is not None:
                self.ids = pd.Index(ids_gen.generate(size=size))

            else:
                raise ValueError("must specify ids_gen and size if ids is not "
                                 "provided")

            self.size = size

        # result of operations are joined by population member id
        # => we need each id to be unique
        if self.ids.has_duplicates:
            raise ValueError("Population id may be not have duplicates, "
                             "check the init values or id generator")

        self.attributes = {}
        self.relationships = {}

        self.ops = self.PopulationOps(self)

    def create_relationship(self, name, seed=None):
        """
        creates an empty relationship from the members of this population
        """

        if name is self.relationships:
            raise ValueError("cannot create a second relationship with "
                             "existing name {}".format(name))

        self.relationships[name] = Relationship(
            seed=seed if seed else next(self.circus.seeder))

        return self.relationships[name]

    def create_stock_relationship(self, name, item_id_gen, n_items_per_member):
        """
        Creates a relationship aimed at maintaining a stock, from a generator
        that create stock item ids.

        The relationship does not point to another population, but to items
        whose id is generated with the provided generator.
        """

        logging.info("generating initial {} stock".format(name))
        rel_to_items = self.create_relationship(name=name)

        assigned_items = make_random_assign(
            set1=item_id_gen.generate(size=n_items_per_member * self.size),
            set2=self.ids,
            seed=next(self.circus.seeder))

        rel_to_items.add_relations(
            from_ids=assigned_items["chosen_from_set2"],
            to_ids=assigned_items["set1"])

    def create_stock_relationship_grp(self, name, stock_bulk_gen):
        """
        This creates exactly the same kind of relationship as
        create_stock_relationship, but using a generator of list of stock
        items instead of a generators of items.
        """

        stock_rel = self.create_relationship(name)
        stock_rel.add_grouped_relations(
            from_ids=self.ids,
            grouped_ids=stock_bulk_gen.generate(size=self.size))

    def get_relationship(self, name):
        if name not in self.relationships:
            raise KeyError("{} not found among relationships of population :"
                           "{}".format(name, self.relationships.keys()))

        return self.relationships[name]

    def create_attribute(self, name, **kwargs):
        self.attributes[name] = Attribute(population=self, **kwargs)
        return self.attributes[name]

    def get_attribute(self, attribute_name):
        if attribute_name not in self.attributes:
            raise KeyError(
                "{} not found among attributes of population :{}".format(
                    attribute_name, self.attributes.keys()))

        return self.attributes[attribute_name]

    def get_attribute_values(self, attribute_name, ids=None):
        """
        :return: the values of this attribute, as a Series
        """
        return self.get_attribute(attribute_name).get_values(ids)

    def attribute_names(self):
        return self.attributes.keys()

    def relationship_names(self):
        return self.relationships.keys()

    def update(self, attribute_df):
        """
        Adds or updates members with the provided attribute ids and values

         :param attribute_df: must be a dataframe whose index contain the id
         of the inserted members. There must be as many
         columns as there are attributes currently defined in this population.

        If the members for the specified ids already exist, their values are
        updated, otherwise the members are created.
        """

        if set(self.attribute_names()) != set(attribute_df.columns.tolist()):
            # TODO: in case of update only, we could accept that.
            # ALso, for insert, we could also accept and just insert NA's
            # This method is currently just aimed at adding "full" members
            # though...
            raise ValueError("""must provide values for all attributes:
                    - population attributes: {}
                    - provided attributes: {}
            """.format(self.attribute_names(), attribute_df.columns))

        values_dedup = attribute_df[~attribute_df.index.duplicated(keep="last")]
        if attribute_df.shape[0] != values_dedup.shape[0]:
            logging.warn("inserted members contain duplicate ids => some will "
                         "be discarded so that all members ids are unique")

        new_ids = values_dedup.index.difference(self.ids)
        self.ids = self.ids | new_ids

        for att_name, values in values_dedup.items():
            self.get_attribute(att_name).update(values)

    def to_dataframe(self):
        """
        :return: all the attributes of this population as one single dataframe
        """
        df = pd.DataFrame(index=self.ids)

        for name in self.attribute_names():
            df[name] = self.get_attribute_values(name, self.ids)

        return df

    def description(self):
        """"
        :return a dictionary description of this population
        """

        return {
            "size": self.size,
            "attributes": self.attribute_names(),
            "relationships": self.relationship_names(),
        }

    #######
    # IO

    def save_to(self, target_folder):
        """
        Saves this population and all its attribute and relationships to the
        specified folder.

        If the folder already exists, it is deleted first
        """

        logging.info("saving population to {}".format(target_folder))

        ensure_non_existing_dir(target_folder)
        os.makedirs(target_folder)

        ids_path = os.path.join(target_folder, "ids.csv")
        self.ids.to_series().to_csv(ids_path, index=False)

        attribute_dir = os.path.join(target_folder, "attributes")

        if len(self.attributes) > 0:
            os.mkdir(attribute_dir)
            for name, attr in self.attributes.items():
                file_path = os.path.join(attribute_dir, name + ".csv")
                attr.save_to(file_path)

        if len(self.relationships) > 0:
            relationships_dir = os.path.join(target_folder, "relationships")
            os.mkdir(relationships_dir)
            for name, rel in self.relationships.items():
                file_path = os.path.join(relationships_dir, name + ".csv")
                rel.save_to(file_path)

    @staticmethod
    def load_from(folder, circus):
        """
        Reads all persistent data of this population and loads it

        :param folder: folder containing all CSV files of this population
        :param circus: parent circus containing this population
        :return:
        """

        ids_path = os.path.join(folder, "ids.csv")
        ids = pd.read_csv(ids_path, index_col=0, names=[]).index

        attribute_dir = os.path.join(folder, "attributes")
        if os.path.exists(attribute_dir):
            attributes = {
                filename[:-4]:
                    Attribute.load_from(os.path.join(attribute_dir, filename))
                for filename in os.listdir(attribute_dir)
            }
        else:
            attributes = {}

        relationships_dir = os.path.join(folder, "relationships")
        if os.path.exists(relationships_dir):
            relationships = {
                filename[:-4]:
                Relationship.load_from(os.path.join(relationships_dir, filename))
                for filename in os.listdir(relationships_dir)
            }
        else:
            relationships = {}

        population = Population(circus=circus, size=0)
        population.attributes = attributes
        population.relationships = relationships
        population.ids = ids
        population.size = len(ids)

        return population

    class PopulationOps(object):
        def __init__(self, population):
            self.population = population

        class Lookup(AddColumns):
            def __init__(self, population, id_field, select_dict):
                AddColumns.__init__(self)
                self.population = population
                self.id_field = id_field
                self.select_dict = select_dict

            def build_output(self, story_data):
                if story_data.shape[0] == 0:
                    return pd.DataFrame(columns=self.select_dict.values())
                elif is_sequence(story_data.iloc[0][self.id_field]):
                    return self._lookup_by_sequences(story_data)
                else:
                    return self._lookup_by_scalars(story_data)

            def _lookup_by_scalars(self, story_data):
                """
                looking up, after we know the ids are not sequences of ids
                """

                output = story_data[[self.id_field]]
                members_ids = story_data[self.id_field].unique()

                for attribute, named_as in self.select_dict.items():
                    vals = pd.DataFrame(
                        self.population.get_attribute_values(attribute,
                                                             members_ids))

                    vals.rename(columns={"value": named_as}, inplace=True)

                    output = pd.merge(left=output, right=vals,
                                      left_on=self.id_field,
                                      right_index=True)

                # self.id_field is already in the parent result, we only
                # want to return the new columns from here
                output.drop(self.id_field, axis=1, inplace=True)
                return output

            def _lookup_by_sequences(self, story_data):

                # pd.Series containing seq of ids to lookup
                id_lists = story_data[self.id_field]

                # unique member ids of the attribute to look up
                member_ids = np.unique(
                    functools.reduce(lambda l1, l2: l1 + l2, id_lists))

                output = pd.DataFrame(index=story_data.index)
                for attribute, named_as in self.select_dict.items():
                    vals = self.population.get_attribute_values(attribute, member_ids)

                    def attributes_of_ids(ids):
                        """
                        :param ids:
                        :return: list of attribute values for those member ids
                        """
                        return vals.loc[ids].tolist()

                    output[named_as] = id_lists.map(attributes_of_ids)

                return output

        def lookup(self, id_field, select):
            """
            Looks up some attribute values by joining on the specified field
            of the current data

            :param id_field: field name in the story_data.
              If the that column contains lists, then it's assumed to contain
              only list and it's flatten to obtain the list of id to lookup
              in the attribute. Must be a list of "scalar" values or list of
              list, not a mix of both.

            :param select: dictionary of (attribute_name -> given_name)
            specifying which attribute to look up and which name to give to
            the resulting column

            """
            return self.Lookup(self.population, id_field, select)

        class Update(SideEffectOnly):
            def __init__(self, population, id_field, copy_attributes_from_fields):
                self.population = population
                self.id_field = id_field
                self.copy_attribute_from_fields = copy_attributes_from_fields

            def side_effect(self, story_data):
                update_df = pd.DataFrame(
                    {attribute: story_data[field].values
                     for attribute, field in self.copy_attribute_from_fields.items()},
                    index=story_data[self.id_field]
                )
                self.population.update(update_df)

        def update(self, id_field, copy_attributes_from_fields):
            """

            Adds or update members and their attributes.

            Note that the index of story_data, i.e. the ids of the _triggering_
            members, is irrelevant during this operation.

            :param id_field: ids of the updated or created members
            :param copy_attributes_from_fields: dictionary of
                (attribute name -> story data field name)
             that describes which column in the population dataframe to use
               to update which attribute.
            :return:

            """
            return self.Update(self.population, id_field,
                               copy_attributes_from_fields)

        def select_one(self, named_as, weight_attribute_name=None):
            """

            Appends a field column to the story_data containing member ids
            taken at random among the ids of this population.

            This is similar to relationship_select_one(), except that no
            particular relation is required.

            It will select one randomly by default, but a weight attribute
            name can be provided as well to give a weight to your selection.

            :param named_as: the name of the field added to the story_data
            :param weight_attribute_name: the attribute name which contains
            the weights you want to use for the selection
            """

            p = None

            if weight_attribute_name:
                attributes = self.population.get_attribute(weight_attribute_name).get_values()

                if np.any(attributes < 0):
                    raise ValueError(
                        "weight_attribute_name contain negative values: cannot use that as weight")

                normalization_factor = attributes.sum()
                if normalization_factor == 0:
                    raise ValueError("weight_attribute_name in population select.one sum up to zero: cannot use that as weight")
                
                p = attributes / attributes.sum()

            gen = random_generators.NumpyRandomGenerator(
                method="choice",
                a=self.population.ids,
                p=p,
                seed=next(self.population.circus.seeder))

            return gen.ops.generate(named_as=named_as)
