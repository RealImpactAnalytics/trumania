import pandas as pd
from numpy.random import RandomState
import numpy as np
import logging
import functools

from trumania.core.util_functions import cap_to_total
from trumania.core.operations import AddColumns, Operation, SideEffectOnly


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
        self.seed = seed
        self.state = RandomState(self.seed)
        self._table = pd.DataFrame(
            columns=["from", "to", "weight", "table_index"])
        self.ops = self.RelationshipOps(self)

    def add_relations(self, from_ids, to_ids, weights=1):

        new_relations = pd.DataFrame({"from": from_ids,
                                      "to": to_ids,
                                      "weight": weights})

        self._table = pd.concat([self._table, new_relations],
                                ignore_index=True, copy=False)

        self._table["weight"] = self._table["weight"].astype(float)

        # we sometimes want to keep track of the index of some rows when
        # accessed within a group-by operation => copying the info in a separate
        # column is the only way I found...
        self._table["table_index"] = self._table.index

    def add_grouped_relations(self, from_ids, grouped_ids):
        """
        Add "bulk" relationship, i.e. many "to" side for each "from" side at
        once.

        :param from_ids: list of "from" sides of the relationships to add
        :param grouped_ids: list of list of "to" sides of the relationships
            to add

        Note: we assume all weights are 1 for now...
        """

        for one_from, many_tos in zip(from_ids, grouped_ids):
            rels = pd.DataFrame({"from": one_from, "to": many_tos})
            self.add_relations(from_ids=rels["from"], to_ids=rels["to"])

    def get_relations(self, from_ids):
        if from_ids is None:
            return self._table
        else:
            return self._table[self._table["from"].isin(from_ids)]

    def get_neighbourhood_size(self, from_ids):
        """
        return a series indexed by "from" containing the number of "tos" for
        each requested from.
        """
        unique_froms = np.unique(from_ids)

        counts = self.get_relations(unique_froms)[["from", "to"]]\
            .groupby("from").count()["to"].astype(int)

        return counts.reindex(np.unique(unique_froms)).fillna(0)

    @staticmethod
    def _maybe_add_nones(should_inject_nones, from_ids, selected):
        """
        if should_inject_Nones, this adds (from_id -> None) selection entries
        for any from_id present in the from_ids series but not found in this
        relationship
        """
        if should_inject_nones and selected.shape[0] != len(from_ids):
            missing_index = from_ids.index.difference(selected.index)
            missing_values = pd.DataFrame(
                {
                    "from": from_ids.loc[missing_index],
                    "to": [None] * missing_index.shape[0]
                },
                index=missing_index)

            return pd.concat([selected, missing_values], copy=False)
        else:
            return selected

    def select_one(self, from_ids=None, named_as="to", remove_selected=False,
                   discard_empty=True, one_to_one=False,
                   overridden_to_weights=None):

        # TODO: the implementation of select_many is cleaner and probably
        # faster => refactor to make them one single thingy, select_one being
        # just a special case of select_many...

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
            selected = pd.DataFrame(columns=["from", "to"])

        else:
            def pick_one(df):
                return df.sample(n=1, weights="weight",
                                 random_state=self.state)[["to", "table_index"]]

            # overriding weights if requested. It's ok to override "weight"
            # since relations is a copy already
            if overridden_to_weights is not None:
                relations["weight"] = relations["to"].map(
                    overridden_to_weights).values

            # picking a "to" for each, potentially duplicated, "from" value
            grouped = relations.groupby(by=["from", "from_index"], sort=False)
            selected = grouped.apply(pick_one)

            if remove_selected:
                # We ignore errors here since several pop of the same "from"
                # could happen at the same time, e.g. same dealer trying to
                # sell the same sell, which would be de-duplicated
                # downstream, and should not lead this to crash
                self._table.drop(selected["table_index"], inplace=True,
                                 errors="ignore")
            selected.drop("table_index", axis=1, inplace=True)

            # shaping final results
            selected["from"] = selected.index.get_level_values(level="from")
            selected.index = selected.index.get_level_values(level="from_index")

        selected = self._maybe_add_nones(not discard_empty, from_ids, selected)
        selected.rename(columns={"to": named_as}, inplace=True)

        if one_to_one and selected.shape[0] > 0:
            idx = self.state.permutation(selected.index)
            selected = selected.loc[idx]
            selected.drop_duplicates(subset=named_as, keep="first",
                                     inplace=True)

        return selected

    def select_many(self, from_ids, named_as, quantities, remove_selected,
                    discard_empty):
        """
        This is functional similar to a select_one that returns list, although
        the implementation is much different. Here we optimize for sampling
        large contiguous chunks among the "to" parts of a relationship.

        Also, one-to-one is always assumed: there is never overlap between
        the returned chunks.

        The result is a dataframe with an index aligned with the one of from_ids
        (each "from" value could be present several times, so we cannot use
        that as index)
        """

        req = pd.DataFrame({"from": from_ids, "qties": quantities})
        req["qties"] = req["qties"].astype(np.int)

        # gathers all requests to the same "from" together, keeping track of
        # the index in the original "from_ids" to be able to merge it later
        def gather(df):
            return pd.Series({"quantities": df["qties"].tolist(),
                              "from_index": df.index.tolist()})

        all_reqs = req.groupby("from", sort=False).apply(gather)

        # potential relations in "horizontal" format, just kept with their index
        # in the original table for now
        relations = self.get_relations(from_ids).groupby(by="from", sort=False)
        relations = relations.apply(lambda r: pd.Series({
            "table_indices": r["table_index"].tolist()
        }))

        # => requested size and the available "tos" for each requested "from"
        all_infos = pd.merge(left=all_reqs, right=relations,
                             left_index=True, right_index=True)

        def make_selections(info_row):

            available_tos = len(info_row["table_indices"])

            # TODO: we should randomize the capping below s.t. the same
            # ones do not risk to get capped every time...

            qties = cap_to_total(info_row["quantities"], available_tos)
            t_idx = self.state.choice(a=info_row["table_indices"], replace=False,
                                      size=np.sum(qties)).tolist()

            # splits the results into several selections: one for each time
            # this "from" was requested
            to_idx = np.cumsum(qties).tolist()
            from_idx = [0] + to_idx[:-1]
            selected_tidx = [t_idx[lb:ub] for lb, ub in zip(from_idx, to_idx)]

            # creating this Series in an apply() will create columns named along
            # info_row["from_index"] => the stack() right after adds them into the
            # row index, besides each "from"
            selection_ser = pd.Series(selected_tidx, index=info_row["from_index"])
            return selection_ser

        # Df with multi-index index (from, from_index) and 1 column with the set of idx
        # in the relationship table of the chosen "t"os
        selected_tidx = pd.DataFrame(all_infos.apply(make_selections, axis=1))
        selected_tidx = pd.DataFrame(selected_tidx.stack(dropna=True))

        # this typically happens if none of the requested "froms" are present
        # in the relationship
        if selected_tidx.shape[0] == 0:
            selected = pd.DataFrame(columns=[named_as])

        else:
            selected = selected_tidx.applymap(
                lambda ids: self._table.ix[ids]["to"].values)
            selected.index = selected.index.droplevel(level=0)
            selected.columns = [named_as]

            # "pop" option: any selected relation is now removed
            if remove_selected:
                all_removed_idx = functools.reduce(lambda l1, l2: l1 + l2,
                                                   selected_tidx.iloc[:, 0])
                self._table.drop(all_removed_idx, axis=0, inplace=True)

        # "discard_empty" option: return empty result (instead of nothing) for
        # any non existing (i.e. empty) "from" relation
        if not discard_empty and selected.shape[0] != len(from_ids):
            missing_index = from_ids.index.difference(selected.index)
            missing_values = pd.DataFrame(
                {named_as: pd.Series([[] * missing_index.shape[0]],
                                     index=missing_index)})

            selected = pd.concat([selected, missing_values], copy=False)

        return selected

    def select_all(self, from_ids, named_as="to"):
        """
        Return all the "to" sides starting from each "from", as a list.

        Any requested from_id that has no relationship is absent is the
        returned dataframe (=> the corresponding rows are dropped in the
        """

        rows = self.get_relations(from_ids)
        groups = rows.set_index("to", drop=True).groupby("from", sort=False)
        df = pd.DataFrame(data=list(groups.groups.items()), columns=["from", named_as])
        df[named_as] = df[named_as].apply(lambda s: [el for el in s])
        return df

    def remove(self, from_ids, to_ids):
        lines = self._table[self._table["from"].isin(from_ids) &
                            self._table["to"].isin(to_ids)]

        self._table.drop(lines.index, inplace=True)

    ######################
    # IO                 #
    ######################

    def save_to(self, file_path):

        logging.info("saving relationship to {}".format(file_path))

        # creating a vertical dataframe to store the inner table
        saved_df = pd.DataFrame(self._table.stack())

        # we also want to save the seed => added an index level to separate
        # self._table from self.seed in the end result
        saved_df["param"] = "table"
        saved_df = saved_df.set_index("param", append=True)
        saved_df.index = saved_df.index.reorder_levels([2, 0, 1])

        # then finally added the seed
        saved_df.loc[("seed", 0, 0)] = self.seed
        saved_df.to_csv(file_path)

    @staticmethod
    def load_from(file_path):
        saved_df = pd.read_csv(file_path, index_col=[0, 1, 2])
        seed = int(saved_df.loc["seed"].values[0][0])

        table = saved_df.loc[("table", slice(None), slice(None))].unstack()
        table.columns = table.columns.droplevel(level=0)
        table.columns.name = None
        table.index.name = None
        table["weight"] = table["weight"].astype(float)
        table["table_index"] = table["table_index"].astype(int)

        rel = Relationship(seed)
        rel._table = table

        return rel

    class RelationshipOps(object):
        def __init__(self, relationship):
            self.relationship = relationship

        class AddNeighbourhoodSize(AddColumns):
            def __init__(self, relationship, from_field, named_as):
                AddColumns.__init__(self)

                self.relationship = relationship
                self.from_field = from_field
                self.named_as = named_as

            def build_output(self, action_data):

                requested_froms = action_data[self.from_field]
                sizes = self.relationship.get_neighbourhood_size(
                    from_ids=requested_froms)

                return pd.DataFrame(
                    {self.named_as: requested_froms.map(sizes).astype(int)})

        def get_neighbourhood_size(self, from_field, named_as):
            return self.AddNeighbourhoodSize(self.relationship, from_field,
                                             named_as)

        class SelectOne(AddColumns):
            """
            """

            def __init__(self, relationship, from_field, named_as,
                         one_to_one, pop, discard_missing, weight):

                # inner join instead of default left to allow dropping rows
                # in case of duplicates and one-to-one
                AddColumns.__init__(self, join_kind="inner")

                self.relationship = relationship
                self.from_field = from_field
                self.named_as = named_as
                self.one_to_one = one_to_one
                self.pop = pop
                self.discard_missing = discard_missing
                self.weight = weight

            # def transform(self, action_data):
            def build_output(self, action_data):
                selected = self.relationship.select_one(
                    from_ids=action_data[self.from_field],
                    named_as=self.named_as,
                    remove_selected=self.pop,
                    one_to_one=self.one_to_one,
                    discard_empty=self.discard_missing,
                    overridden_to_weights=self.weight)

                selected.drop("from", axis=1, inplace=True)
                return selected

        def select_one(self, from_field, named_as, one_to_one=False,
                       pop=False, discard_empty=False, weight=None):
            """
            :param from_field: field corresponding to the "from" side of the
                relationship

            :param named_as: field name assigned to the selected "to" side
                of the relationship

            :param one_to_one: boolean indicating that any "to" value will be
                selected at most once

            :param pop: if True, the selected relation is removed

            :param discard_empty: if False, any non-existing "from" in the
                relationship yields a None in the resulting selection. If
                true, that row is removed from the action_data.

            :param weight: weight to use for the "to" side of the
                relationship. Must be a Series whose index are the "to" values.
                Typical usage would be to plug an attribute of the "to" actor
                here.

            :return: this operation adds a single column corresponding to a
                random choice from a Relationship
            """
            return self.SelectOne(self.relationship, from_field, named_as,
                                  one_to_one, pop, discard_empty, weight)

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

        class SelectMany(AddColumns):
            """
            """

            def __init__(self, relationship, from_field, named_as,
                         quantity_field, pop, discard_missing):

                # inner join instead of default left to allow dropping rows
                # in case of duplicates and one-to-one
                AddColumns.__init__(self, join_kind="inner")

                self.relationship = relationship
                self.discard_missing = discard_missing
                self.from_field = from_field
                self.named_as = named_as
                self.quantity_field = quantity_field
                self.pop = pop

            # def transform(self, action_data):
            def build_output(self, action_data):
                selected = self.relationship.select_many(
                    from_ids=action_data[self.from_field],
                    named_as=self.named_as,
                    quantities=action_data[self.quantity_field],
                    remove_selected=self.pop,
                    discard_empty=self.discard_missing)

                return selected

        def select_many(self, from_field, named_as, quantity_field, pop=False,
                        discard_missing=True):
            return self.SelectMany(self.relationship, from_field, named_as,
                                   quantity_field, pop, discard_missing)

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

        class AddGrouped(SideEffectOnly):
            def __init__(self, relationship, from_field, grouped_items_field):
                self.relationship = relationship
                self.from_field = from_field
                self.grouped_items_field = grouped_items_field

            def side_effect(self, action_data):
                if action_data.shape[0] > 0:

                    self.relationship.add_grouped_relations(
                        from_ids=action_data[self.from_field],
                        grouped_ids=action_data[self.grouped_items_field])

        def add_grouped(self, from_field, grouped_items_field):
            """
            this is similar to add, execept that the "to" field should here
            contain lists of "to" values instead of single ones
            """
            return self.AddGrouped(self.relationship, from_field,
                                   grouped_items_field)

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
