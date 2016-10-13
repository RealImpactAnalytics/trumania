from datagenerator.core.attribute import *
from datagenerator.core.relationship import *


class Actor(object):
    def __init__(self, ids_gen=None, size=None, ids=None):
        """
        :param ids_gen: generator for the actor ids
        :param size: number of ids to generate (only relevant if is ids_gen
        is specified
        :param ids: if neither ids_gen nore size is specified, we can
          also specify the ids explicitally
        :return:
        """
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
        self.attributes = {}
        self.relationships = {}

        self.ops = self.ActorOps(self)

    def create_relationship(self, name, seed):
        """
        creates an empty relationship from this actor
        """

        if name is self.relationships:
            raise ValueError("cannot create a second relationship with "
                             "existing name {}".format(name))

        self.relationships[name] = Relationship(seed=seed)
        return self.relationships[name]

    def create_stock_relationship(self, name, item_id_gen, n_items_per_actor,
                                  seeder):
        """
        Creates a relationship aimed at maintaining a stock, from a generator
        that create stock item ids.

        The relationship does not point to another actor, but to items
        whose id is generated with the provided generator.
        """

        logging.info("generating initial {} stock".format(name))
        rel_to_items = self.create_relationship(name=name, seed=seeder.next())

        assigned_items = make_random_assign(
            set1=item_id_gen.generate(size=n_items_per_actor * self.size),
            set2=self.ids,
            seed=seeder.next())

        rel_to_items.add_relations(
            from_ids=assigned_items["chosen_from_set2"],
            to_ids=assigned_items["set1"])

    def create_stock_relationship_grp(self, name, stock_bulk_gen, seed):
        """
        This creates exactly the same kind of relationship as
        create_stock_relationship, but using a generator of list of stock
        items instead of a generators of items.
        """

        stock_rel = self.create_relationship(name, seed=seed)
        stock_rel.add_grouped_relations(
            from_ids=self.ids,
            grouped_ids=stock_bulk_gen.generate(size=self.size))

    def get_relationship(self, name):
        return self.relationships[name]

    def create_attribute(self, name, **kwargs):
        self.attributes[name] = Attribute(actor=self, **kwargs)
        return self.attributes[name]

    def get_attribute(self, attribute_name):
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
        Adds or updates actors with the provided attribute ids and values

         :param attribute_df: must be a dataframe whose index contain the id
         of the inserted actors. There must be as many
         columns as there are attributes currently defined in this actor.

        If the actors for the specified ids already exist, their values are
        updated, otherwise the actors are created.
        """

        if set(self.attribute_names()) != set(attribute_df.columns.tolist()):
            # TODO: in case of update only, we could accept that.
            # ALso, for insert, we could also accept and just insert NA's
            # This method is currently just aimed at adding "full" actors though...
            raise ValueError("""must provide values for all attributes:
                    - actor attributes: {}
                    - provided attributes: {}
            """.format(self.attribute_names(), attribute_df.columns))

        values_dedup = attribute_df[~attribute_df.index.duplicated(keep="last")]
        if attribute_df.shape[0] != values_dedup.shape[0]:
            logging.warn("inserted actors contain duplicate ids => some will "
                         "be discarded so that all actor ids are unique")

        new_ids = values_dedup.index.difference(self.ids)
        self.ids = self.ids | new_ids

        for att_name, values in values_dedup.iteritems():
            self.get_attribute(att_name).update(values)

    def to_dataframe(self):
        """
        :return: all the attributes of this actor as one single dataframe
        """
        df = pd.DataFrame(index=self.ids)

        for name in self.attribute_names():
            df[name] = self.get_attribute_values(name, self.ids)

        return df

    def description(self):
        """"
        :return a dictionary description of this actor
        """

        return {
            "size": self.size,
            "attributes": self.attribute_names(),
            "relationships": self.relationship_names(),
        }

    #######
    # IO

    def save_to(self, actor_dir):
        """
        Saves this actor and all its attribute and relationships to the
        specified folder.

        If the folder already exists, it is deleted first
        """

        logging.info("saving actor to {}".format(actor_dir))

        ensure_non_existing_dir(actor_dir)
        os.makedirs(actor_dir)

        ids_path = os.path.join(actor_dir, "ids.csv")
        self.ids.to_series().to_csv(ids_path, index=False)

        attribute_dir = os.path.join(actor_dir, "attributes")

        if len(self.attributes) > 0:
            os.mkdir(attribute_dir)
            for name, attr in self.attributes.iteritems():
                file_path = os.path.join(attribute_dir, name + ".csv")
                attr.save_to(file_path)

        if len(self.relationships) > 0:
            relationships_dir = os.path.join(actor_dir, "relationships")
            os.mkdir(relationships_dir)
            for name, rel in self.relationships.iteritems():
                file_path = os.path.join(relationships_dir, name + ".csv")
                rel.save_to(file_path)

    @staticmethod
    def load_from(actor_dir):

        ids_path = os.path.join(actor_dir, "ids.csv")
        ids = pd.read_csv(ids_path, index_col=0, names=[]).index

        attribute_dir = os.path.join(actor_dir, "attributes")
        if os.path.exists(attribute_dir):
            attributes = {
                filename[:-4]:
                    Attribute.load_from(os.path.join(attribute_dir, filename))
                for filename in os.listdir(attribute_dir)
            }
        else:
            attributes = {}

        relationships_dir = os.path.join(actor_dir, "relationships")
        if os.path.exists(relationships_dir):
            relationships = {
                filename[:-4]:
                Relationship.load_from(os.path.join(relationships_dir, filename))
                for filename in os.listdir(relationships_dir)
            }
        else:
            relationships = {}

        actor = Actor(size=0)
        actor.attributes = attributes
        actor.relationships = relationships
        actor.ids = ids
        actor.size = len(ids)

        return actor

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
                    return pd.DataFrame(columns=self.select_dict.values())
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

        class Update(SideEffectOnly):
            def __init__(self, actor, actor_id_field, copy_attributes_from_fields):
                self.actor = actor
                self.actor_id_field = actor_id_field
                self.copy_attribute_from_fields = copy_attributes_from_fields

            def side_effect(self, action_data):
                update_df = pd.DataFrame(
                    {attribute: action_data[field].values
                     for attribute, field in self.copy_attribute_from_fields.items()},
                    index=action_data[self.actor_id_field]
                )
                self.actor.update(update_df)

        def update(self, actor_id_field, copy_attributes_from_fields):
            """

            Adds or update actors and their attributes.

            Note that the index of action_data, i.e. the ids of the _triggering_
            actors, is irrelevant during this operation.

            :param actor_id_field: ids of the updated or created actors
            :param copy_attributes_from_fields: dictionary of
                (attribute name -> action data field name)
             that describes which column in the actor data dataframe to use
               to update which attribute.
            :return:

            """
            return self.Update(self.actor, actor_id_field,
                               copy_attributes_from_fields)

