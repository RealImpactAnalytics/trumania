from datagenerator.operations import *
from numpy.random import RandomState


class Relationship(object):
    """
        One-to-many relationship between actors.

        Each implementation of this provides a `select_one(from_ids)` method
        that randomly picks one "to" side for each of the provided "from"
        A similar `pop_one(from_ids)` method is availeble, which behaves
        similarly + removes the selected relationship.

        Both those method may return supplementary data with each "to" side.
        For example, a CustomerSellerRelationship could be designed to
        captures the available sellers of each buyers. `select_one` in that
        case could mean "sale", and supplementary properties could be the
        description of the sold items.

        `select_one` and `pop_one` must have the same return signature.

    """

    def __init__(self, seed):
        """

        :param r1: string, name for first element
        :param r2: string, name for second element
        :param chooser:
        :return:
        """

        self.state = RandomState(seed)
        self._table = pd.DataFrame(columns=["from", "to", "weight"])
        self.ops = self.RelationshipOps(self)

    def add_relations(self, from_ids, to_ids, weights=1):

        new_relations = pd.DataFrame({"from": from_ids,
                                      "to": to_ids,
                                      "weight": weights})

        self._table = pd.concat([self._table, new_relations])
        self._table.reset_index(drop=True, inplace=True)

    def get_relations(self, from_ids):
        if from_ids is None:
            return self._table
        else:
            return self._table[self._table["from"].isin(from_ids)]

    def missing_ids(self, from_ids):
        "return: the set of ids in from_ids not present in this relationship"

        return set(from_ids) - set(self._table["from"].unique())

    def select_one(self, from_ids=None, named_as="to", drop=False):
        """
        Select randomly one "to" for each specified "from" values.
         If drop is True, we the selected relations are removed
        """

        rows = self.get_relations(from_ids)

        if rows.shape[0] == 0:
            return pd.DataFrame(columns=["from", named_as])

        def pick_one(df):
            selected_to = df.sample(n=1, weights="weight",
                                    random_state=self.state)[["to"]]
            if drop:
                selected_to["selected_index"] = selected_to.index
            return selected_to

        selected = rows.groupby(by="from", sort=False).apply(pick_one)
        selected["from"] = selected.index.get_level_values(level="from")
        selected.rename(columns={"to": named_as}, inplace=True)

        if drop:
            self._table.drop(selected["selected_index"], inplace=True)
            selected.drop("selected_index", axis=1, inplace=True)

        return selected

    def remove(self, from_ids, to_ids):
        lines = self._table[self._table["from"].isin(from_ids) &
                            self._table["to"].isin(to_ids)]

        self._table.drop(lines.index, inplace=True)

    def select_all(self, from_ids, named_as="to"):

        rows = self.get_relations(from_ids)

        # a to b relationship as tuples in "wide format", e.g.
        # [ ("a1", ["b1", "b2"]), ("a2", ["b3", "b4", "b4]), ...]
        tuples = rows.set_index("to", drop=True).groupby("from").groups.items()

        empty_rels = [(missing, []) for missing in self.missing_ids(from_ids)]

        return pd.DataFrame(tuples + empty_rels, columns=["from", named_as])

    class RelationshipOps(object):
        def __init__(self, relationship):
            self.relationship = relationship

        class SelectOne(AddColumns):
            """
            Operation that wraps a select_one() call on the relationship
            """

            def __init__(self, relationship, from_field, named_as,
                         one_to_one, drop):

                # we force the join to be inner to allow dropping rows
                AddColumns.__init__(self, join_kind="inner")
                self.relationship = relationship
                self.from_field = from_field
                self.named_as = named_as
                self.one_to_one = one_to_one
                self.drop = drop

            # def transform(self, action_data):
            def build_output(self, action_data):

                selected = self.relationship.select_one(
                    from_ids=action_data[self.from_field],
                    named_as=self.named_as,
                    drop=self.drop)

                if self.one_to_one and selected.shape[0] > 0:
                    idx = self.relationship.state.permutation(selected.index)
                    selected = selected.loc[idx]
                    selected.drop_duplicates(subset=self.named_as,
                                             keep="first", inplace=True)

                selected.set_index("from", drop=True, inplace=True)
                return selected

        def select_one(self, from_field, named_as, one_to_one=False,
                       drop=False):
            """
            :param from_field: field corresponding to the "from" side of the
                relationship
            :param named_as: field name assigned to the selected "to" side
                of the relationship
            :param one_to_one: boolean indicating that any "to" value will be
                selected at most once
            :return: this operation adds a single column corresponding to a
                random choice from a Relationship
            """
            return self.SelectOne(self.relationship, from_field, named_as,
                                  one_to_one, drop)

        class SelectAll(AddColumns):
            def __init__(self, relationship, from_field, named_as):
                AddColumns.__init__(self)
                self.relationship = relationship
                self.from_field = from_field
                self.named_as = named_as

            def build_output(self, action_data):
                from_ids = action_data[self.from_field]
                selected = self.relationship.select_all(from_ids, self.named_as)
                selected.set_index("from", drop=True, inplace=True)
                return selected

        def select_all(self, from_field, named_as):
            """
            This simply creates a new action_data field containing all the
            "to" values of the requested from, as a set.
            """
            return self.SelectAll(self.relationship, from_field, named_as)

        class Add(SideEffectOnly):
            def __init__(self, relationship, from_field, item_field):
                self.relationship = relationship
                self.from_field = from_field
                self.item_field = item_field

            def side_effect(self, action_data):
                if action_data.shape[0] > 0:
                    self.relationship.add_relations(
                        from_ids=action_data[self.from_field],
                        to_ids=action_data[self.item_field])

        def add(self, from_field, item_field):
            return self.Add(self.relationship, from_field, item_field)

        class Remove(SideEffectOnly):
            def __init__(self, relationship, from_field, item_field):
                self.relationship = relationship
                self.from_field = from_field
                self.item_field = item_field

            def side_effect(self, action_data):
                if action_data.shape[0] > 0:
                    self.relationship.remove(
                        from_ids=action_data[self.from_field],
                        to_ids=action_data[self.item_field])

        def remove(self, from_field, item_field):
            return self.Add(self.relationship, from_field, item_field)




# class SimpleMobilityRelationship(WeightedRelationship):
#     """
#
#     """
#
#     def choose(self, clock, key_column, keys):
#         return self.select_one(key_column, keys)
#
#
# class HWRMobilityRelationship(WeightedRelationship):
#     """
#
#     """
#     def __init__(self,r1,r2,chooser,time_f):
#         """
#
#         :param r1:
#         :param r2:
#         :param chooser:
#         :param time_f:
#         :return:
#         """
#         cols = {r1: pd.Series(dtype=int),
#                 r2: pd.Series(dtype=int),
#                 "weight": pd.Series(dtype=float)}
#         self._home_table = pd.DataFrame(cols)
#         self._work_table = pd.DataFrame(cols)
#         self._random_table = pd.DataFrame(cols)
#         self.__chooser = chooser
#         self.__r1 = r1
#         self.__r2 = r2
#         self.__time_f = time_f
#
#     def add_home(self, r1, A, r2, B):
#         """
#
#         :param r1:
#         :param A:
#         :param r2:
#         :param B:
#         :return:
#         """
#         df = pd.DataFrame({r1: A, r2: B})
#         self._home_table = self._home_table.append(df, ignore_index=True)
#
#     def add_work(self, r1, A, r2, B):
#         """
#
#         :param r1:
#         :param A:
#         :param r2:
#         :param B:
#         :return:
#         """
#         df = pd.DataFrame({r1: A, r2: B})
#         self._work_table = self._work_table.append(df, ignore_index=True)
#
#     def add_random(self, r1, A, r2, B):
#         """
#
#         :param r1:
#         :param A:
#         :param r2:
#         :param B:
#         :return:
#         """
#         df = pd.DataFrame({r1: A, r2: B})
#         self._random_table = self._random_table.append(df, ignore_index=True)
#
#     def choose(self, clock, key_column, keys):
#         """
#
#         :param clock:
#         :param key_column:
#         :param keys:
#         :return:
#         """
#         # TODO: make a function of the clock that returns what's needed
#         w_home,w_work,w_random = self.__time_f(clock)
#
#         small_home = self._home_table[self._home_table[self.__r1].isin(keys)].copy()
#         small_home["weight"] = small_home["weight"]*w_home
#
#         small_work = self._work_table[self._work_table[self.__r1].isin(keys)].copy()
#         small_work["weight"] = small_work["weight"]*w_work
#
#         small_random = self._random_table[self._random_table[self.__r1].isin(keys)].copy()
#         small_random["weight"] = small_random["weight"]*w_random
#
#         small_tab = pd.concat([small_home,small_work,small_random],ignore_index=True)
#         return small_tab.groupby(key_column).aggregate(self.__chooser.generate)
