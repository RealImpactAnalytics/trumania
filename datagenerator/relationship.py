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

        self._table = pd.concat([self._table, new_relations], ignore_index=True)

    def get_relations(self, from_ids):
        if from_ids is None:
            return self._table
        else:
            return self._table[self._table["from"].isin(from_ids)]

    def _maybe_add_nones(self, should_inject_Nones, from_ids, selected):
        """
        if should_inject_Nones, this adds (from_id -> None) selection entries
        for any from_id present in the from_ids series but not found in this
        relationship
        """
        if should_inject_Nones and selected.shape[0] != len(from_ids):
            missing_index = from_ids.index.difference(selected.index)
            missing_values = pd.DataFrame({
                "from": from_ids.loc[missing_index],
                "to": None
                },
                index = missing_index
            )

            return pd.concat([selected, missing_values])
        else:
            return selected

    def select_one(self, from_ids=None, named_as="to", remove_selected=False,
                   keep_missing=False):
        """
        Select randomly one "to" for each specified "from" values.
         If drop is True, the selected relations are removed.

         from_ids must be a Series with an index that does not have duplicates.

         from_ids values can contain duplicates: if "DEALER_123" is present 5
         times in it, this will return 5 selections from it in the result,
         with index aligned with the one of from_ids.
        """
        if type(from_ids) == list:
            from_ids = pd.Series(from_ids)

        # TODO: performance: the logic with from_index below slows down the
        # lookup => we could test for unicity in from_ids first and only
        # apply it in this case.

        ###
        # selects the relations for the requested from_ids, keeping track of
        # the index of the original from_ids Series
        if from_ids is not None:
            from_df = pd.DataFrame({"from_id": from_ids})
            from_df["from_index"] = from_df.index
            relations = pd.merge(left=self.get_relations(from_ids),
                                 right=from_df,
                                 left_on="from", right_on="from_id")
            relations.drop("from_id", axis=1, inplace=True)

        else:
            relations = self._table.copy()
            relations["from_index"] = relations["from"]

        ###
        # actual selection of a "to" side
        if relations.shape[0] == 0:
            selected = pd.DataFrame(columns=["from", named_as])

        else:
            def pick_one(df):
                selected_to = df.sample(n=1, weights="weight",
                                        random_state=self.state)[["to"]]
                if remove_selected:
                    # This is the index of the relation in the _table. We keep
                    # track of it in case we want to drop any selected
                    # relationship
                    selected_to["selected_index"] = selected_to.index

                return selected_to

            # picking a "to" for each, potentially duplicated, "from" value
            grouped = relations.groupby(by=["from", "from_index"], sort=False)
            selected = grouped.apply(pick_one)

            if remove_selected:
                self._table.drop(selected["selected_index"], inplace=True)
                selected.drop("selected_index", axis=1, inplace=True)

            # shaping final results
            selected["from"] = selected.index.get_level_values(level="from")
            selected.rename(columns={"to": named_as}, inplace=True)
            selected.index = selected.index.get_level_values(level="from_index")

        selected = self._maybe_add_nones(keep_missing, from_ids, selected)
        return selected

    def remove(self, from_ids, to_ids):
        lines = self._table[self._table["from"].isin(from_ids) &
                            self._table["to"].isin(to_ids)]

        self._table.drop(lines.index, inplace=True)

    def select_all(self, from_ids, named_as="to"):
        """
        Return all the "to" sides starting from each "from", as a list.

        Any requested from_id that has no relationship is absent is the
        returned dataframe (=> the corresponding rows are dropped in the
        """

        rows = self.get_relations(from_ids)

        # a to b relationship as tuples in "wide format", e.g.
        # [ ("a1", ["b1", "b2"]), ("a2", ["b3", "b4", "b4]), ...]
        groups = rows.set_index("to", drop=True).groupby("from", sort=False)

        return pd.DataFrame(groups.groups.items(), columns=["from", named_as])

    class RelationshipOps(object):
        def __init__(self, relationship):
            self.relationship = relationship

        class SelectOne(AddColumns):
            """
            """

            def __init__(self, relationship, from_field, named_as,
                         one_to_one, remove_selected, keep_missing):
                # inner join instead of default left to allow dropping rows
                # in case of duplicates and one-to-one
                AddColumns.__init__(self, join_kind="inner")
                self.relationship = relationship
                self.from_field = from_field
                self.named_as = named_as
                self.one_to_one = one_to_one
                self.remove_selected = remove_selected
                self.keep_missing = keep_missing

            # def transform(self, action_data):
            def build_output(self, action_data):
                selected = self.relationship.select_one(
                    from_ids=action_data[self.from_field],
                    named_as=self.named_as,
                    remove_selected=self.remove_selected)

                if self.one_to_one and selected.shape[0] > 0:
                    idx = self.relationship.state.permutation(selected.index)
                    selected = selected.loc[idx]
                    selected.drop_duplicates(subset=self.named_as,
                                             keep="first", inplace=True)

                selected.drop("from", axis=1, inplace=True)
                return selected

        def select_one(self, from_field, named_as, one_to_one=False,
                       remove_selected=False, keep_missing=False):
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
                                  one_to_one, remove_selected, keep_missing)

        class SelectAll(Operation):
            def __init__(self, relationship, from_field, named_as):
                self.relationship = relationship
                self.from_field = from_field
                self.named_as = named_as

            def transform(self, action_data):

                from_ids = action_data[[self.from_field]].drop_duplicates()
                selected = self.relationship.select_all(
                    from_ids=from_ids[self.from_field].values,
                    named_as=self.named_as)

                selected.set_index("from", drop=True, inplace=True)
                return pd.merge(left=action_data, right=selected,
                                left_on=self.from_field, right_index=True)

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
