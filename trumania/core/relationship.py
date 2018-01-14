import functools
import itertools
import logging

import numpy as np
import pandas as pd
from numpy.random import RandomState
from trumania.core import util_functions as utils
from trumania.core.operations import AddColumns, Operation, SideEffectOnly


# There are a lot of somewhat ugly optimizations here like in-place mutations,
# caching, or usage of numpy instead of a more readable pandas alternative. The
# reason is the methods of this filetend to be called a large amount of time
# in inner loop of the simulation, optimizing them make the whole simulation
# faster.


class Relations(object):
    """
     This entity contains all the "to" sides of the relationships of a given
     "from", together with the related weights.

     This data structure seems to be the most optimal since it corresponds to a cached
     group-by result, and those group-by are expensive in the select_one
     operation
    """

    def __init__(self, to_ids, weights):
        self.to_ids = np.array(to_ids)
        self.weights = np.array(weights)
        self.weights_normed = self.weights / self.weights.sum()

    def __len__(self):
        return self.to_ids.shape[0]

    def __repr__(self):
        return """to_ids: {},\nweights:{},\nweights_normed:{}""".format(
            self.to_ids, self.weights, self.weights_normed)

    @staticmethod
    def from_tuples(from_ids, to_ids, weights):
        """
         from_ids, to_ids and weights must be 3 arrays of identical size,
         a relationship is built here for each "line" read across those 3
         arrays.

         This methods builds one instance of Relations for each unique from_id
         value, containing all the to_id's it is related to.
        """

        from_ids = np.array(from_ids)
        to_ids = np.array(to_ids)

        if type(weights) is int or type(weights) is float:
            weights = np.repeat(weights, from_ids.shape)
        else:
            weights = np.array(weights)

        order = from_ids.argsort()
        ordered = zip(from_ids[order], to_ids[order], weights[order])

        def _relations():
            # itertools.groupby is much faster than pandas
            for from_id, tuples in itertools.groupby(ordered, lambda t: t[0]):
                to_ids, weights = list(zip(*tuples))[1: 3]
                yield from_id, Relations(list(to_ids), list(weights))

        return {from_id: relz for from_id, relz in _relations()}

    def plus(self, other):
        """
        Merge function for 2 sets of relations all starting from the same "from"
        """
        return Relations(
            np.hstack([self.to_ids, other.to_ids]),
            np.hstack([self.weights, other.weights]))

    def minus(self, other):
        """
        removes from self _all_ relations to the to_ids mentioned in the
        provided other Relation
        """
        removed_indices = np.argwhere(
            [idx in other.to_ids for idx in self.to_ids])
        return Relations(
            np.delete(self.to_ids, removed_indices),
            np.delete(self.weights, removed_indices))

    def pick_one(self, random_state, overridden_to_weights=None):
        """
        Randomly picks one of the to_ids of this Relation. By default this
        uses the weights encapsulated in this Relation, unless
        overridden_to_weights is specified.
        """

        if self.to_ids.shape[0] == 0:
            return None, None

        if self.to_ids.shape[0] == 1:
            return 0, self.to_ids[0]

        if overridden_to_weights is None:
            proba = self.weights_normed.astype(float)
        else:
            proba = np.array(
                [overridden_to_weights[to] for to in self.to_ids]).astype(float)
            proba = proba / proba.sum()

        idx = random_state.choice(
            a=range(self.to_ids.shape[0]), size=1, p=proba)[0]

        # we do not remove values here, even if "pop=true", since some
        # picked selections might be discarded later in case of one-to-one
        return idx, self.to_ids[idx]

    def pick_many(self, random_state, amount):
        """
        Quantities and req_indices should have the same size: the first
        one lists the ids of the index that requests some selections
        to be picked in the relationship, and the second provide
        the quantities that each request asked.

        The result will be in vertical format, with as many lines as the
        sum of the quantities.
        """

        sample_size = min(self.to_ids.shape[0], amount)

        # pick enough random index of "to_ids"
        indices = random_state.choice(
            a=range(self.to_ids.shape[0]),
            replace=False,
            size=sample_size
        ).tolist()

        return indices, self.to_ids[indices]

    def remove_inplace(self, removed_indices):
        self.to_ids = np.delete(self.to_ids, removed_indices)
        self.weights = np.delete(self.weights, removed_indices)
        self.weights_normed = np.delete(self.weights_normed, removed_indices)
        self.weights_normed = self.weights / self.weights.sum()


class Relationship(object):
    def __init__(self, seed):
        self.seed = seed
        self.state = RandomState(self.seed)
        self.grouped = {}
        self.ops = self.RelationshipOps(self)

    def add_relations(self, from_ids, to_ids, weights=1):
        """
        Add relations to this Relationships from from_ids, to_ids, weights
        """

        self.grouped = utils.merge_2_dicts(
            self.grouped,
            Relations.from_tuples(from_ids, to_ids, weights),
            lambda r1, r2: r1.plus(r2))

    def add_grouped_relations(self, from_ids, grouped_ids):
        """
        Add "bulk" relationship, i.e. many "to" sides for each "from" side at
        once.

        :param from_ids: list of "from" sides of the relationships to add
        :param grouped_ids: list of list of "to" sides of the relationships
            to add

        Note: we assume all weights are 1 for this use (for now
        """

        for one_from, many_tos in zip(from_ids, grouped_ids):
            rels = pd.DataFrame({"from": one_from, "to": many_tos})
            self.add_relations(from_ids=rels["from"], to_ids=rels["to"])

    def remove_relations(self, from_ids, to_ids):
        """
        Removes all relations between those from_ids and to_ids pairs (not combinatory: if each list is
        10 elements, we removed 10 pairs).
        If the same relation was stored several times between two ids, this removes them all
        """

        self.grouped = utils.merge_2_dicts(
            self.grouped,
            Relations.from_tuples(from_ids, to_ids, weights=0),
            lambda r1, r2: r1.minus(r2))

    def get_relations(self, from_ids=None):
        """
        This returns, as a dataframe, the sub-set of the relationships whose
        "from" is part of specified "from_ids".

        If no from_ids is provided, this just returns all the relations.
        """

        _from_ids = set(self.grouped.keys()) if from_ids is None else from_ids

        def _rel_arrays():
            for gid in set(_from_ids):
                if gid in self.grouped.keys():
                    relations = self.grouped[gid]
                    yield np.array([np.array([gid] * relations.to_ids.shape[0]),
                                    relations.to_ids,
                                    relations.weights])

        rel_arrays = list(_rel_arrays())
        if len(rel_arrays) == 0:
            return pd.DataFrame(columns=["from", "to", "weight"])

        else:
            df = pd.DataFrame(np.hstack(rel_arrays).T,
                              columns=["from", "to", "weight"])
            df["weight"] = df["weight"].astype(float)
            return df

    def get_neighbourhood_size(self, from_ids):
        """
        return a series indexed by "from" containing the number of "tos" for
        each requested from.
        """

        def size(from_id):
            if from_id in self.grouped:
                return len(self.grouped[from_id])
            else:
                return 0

        return pd.Series({from_id: size(from_id) for from_id in from_ids})

    def unique_tos(self):
        """
        :return: the set of unique "to" parts throughout all relationships
        """
        return {to for relations in self.grouped.values() for to in
                relations.to_ids}

    def select_one(self, from_ids=None, named_as="to", remove_selected=False,
                   discard_empty=True, one_to_one=False,
                   overridden_to_weights=None):
        """
        Randomly selects one "to" part for each specified id in from_ids. An
        id can be specified several times in that list, in which case we
        simply do a selection several times. The result is aligned with
        from_ids by index. i.e. the row in the return value that has the same
        pandas index than a rom in from_ids is the selection for that row.

        The selection in the resulting dataframe will by default be named
        "to", unless this is overridden by "named_as".

        If remove_selected is True, the selected relations are removed from
        the relationship. This is handy to model stocks or any container of
        things.

        If discard_empty is True, all specified from_ids will be present in
        the result, even if no relation is available for them or if some
        selection were dropped due to one-to-one config.

        If one_to_one is True, the selection is an injective function,
        i.e each to_ids will at most be picked once.

        overridden_to_weights is an optional dictionary of {"to": weight}
        that can be used to override the default weights contained in this
        Relationship.
        """

        if overridden_to_weights is not None:
            missing_keys = self.unique_tos() - set(
                overridden_to_weights.keys().values)
            assert len(missing_keys) == 0, \
                "overridden_to_weights is missing those 'to' keys: {}".format(
                    missing_keys)

        if from_ids is None:
            _from_ids = pd.Series(list(self.grouped.keys()))
        elif type(from_ids) == list:
            _from_ids = pd.Series(from_ids)
        else:
            _from_ids = from_ids

        def _results():
            # req_index is the technical index of the table built by the Story,
            # => must be respect to join correctly the result of the select_one
            for req_index, from_id in zip(_from_ids.index, _from_ids):
                if from_id in self.grouped:
                    idx, picked = self.grouped[from_id].pick_one(self.state,
                                                                 overridden_to_weights)
                    if picked is None:
                        if discard_empty:
                            continue
                        else:
                            yield req_index, from_id, -1, None
                    else:
                        yield req_index, from_id, idx, picked

                elif not discard_empty:
                    yield req_index, from_id, -1, None

        output = list(zip(*_results()))
        if len(output) == 0:
            return pd.DataFrame(columns=["from", named_as])

        request_index, from_id, rel_idx, chosen_tos = output
        output = pd.DataFrame({named_as: list(chosen_tos),
                               "idx": list(rel_idx),
                               "from": from_id},
                              index=request_index)

        if one_to_one and output.shape[0] > 0:
            # not de-duplicating the blank results
            blank_idx = output[named_as].isna()
            blanks, present = output[blank_idx], output[~blank_idx]

            present = present.loc[self.state.permutation(present.index)]
            present.drop_duplicates(subset=named_as, keep="first", inplace=True)

            output = pd.concat([present, blanks])

        if remove_selected:

            # we have to remove all the relations of each from in one go since
            # no injective selection might have the same index several times
            g = output[output["idx"] != -1][["from", "idx"]].groupby(by="from")
            for from_id in g.groups:
                group = self.grouped[from_id]
                removed_idx = g.get_group(from_id)["idx"]
                group.remove_inplace(removed_idx)
                if len(group) == 0:
                    del self.grouped[from_id]

        output.drop(["idx"], axis=1, inplace=True)
        return output

    def select_all_horizontal(self, from_ids, named_as="to"):
        """
        Return all the "to" sides starting from each "from",
        as an "horizontal" list, i.e. each "from" is on one row and the set of
        all "to" are all on that row, in one list.

        Any requested from_id that has no relationship is absent is the
        returned dataframe (=> the corresponding rows are dropped in the result)
        """

        rows = self.get_relations(from_ids)
        groups = rows.set_index("to", drop=True).groupby("from", sort=False)
        df = pd.DataFrame(data=list(groups.groups.items()),
                          columns=["from", named_as])
        df[named_as] = df[named_as].apply(lambda s: [el for el in s])
        return df

    def select_many(self, from_ids, named_as, quantities, remove_selected=False,
                    discard_empty=True):
        """

        The result is returned in vertical format and index by the values of the index of from_ids.
        Since we select several values, we return several lines per index value of from_id =>
        during the subsequent join by the Operation, the number of produced rows increases.

        """

        req = pd.DataFrame({"from": from_ids, "qties": quantities})
        req["qties"] = req["qties"].astype(np.int)

        # gathers all requests to the same "from" together, keeping track of
        # the "request index" in the original from_ids so we can merge it later
        def gather(df):

            # shuffles that set of request s.t. in case of capping not the same
            # from_id get "capped" all time
            df2 = df.loc[self.state.permutation(df.index)]
            return pd.Series({"quantities": df2["qties"].tolist(),
                              "req_index": df2.index.tolist()})

        # the same "from" can be requested several times
        all_reqs = req.groupby("from", sort=False).apply(gather)

        def _all_picks_results():
            for _, row in all_reqs.iterrows():

                from_id = row.name

                if from_id in self.grouped:

                    relations = self.grouped[from_id]
                    quantities = utils.cap_to_total(row["quantities"],
                                                    len(relations))

                    # rel_idx is the index of the picked values within the grouped values (i.e. for one from_id)
                    rel_idx, rel_tos = relations.pick_many(self.state,
                                                           np.sum(quantities))

                    # prepares the indices of the resulting vertical format, as a sequence
                    # of index interval where to inject the picked values
                    to_idx = np.cumsum(quantities).tolist()
                    from_idx = [0] + to_idx[:-1]
                    idx_intervals = [(lb, ub) for lb, ub in zip(from_idx, to_idx)]

                    def _one_pick_result():
                        for ((lower_bound, upper_bound), req_index) in zip(
                                idx_intervals, row["req_index"]):
                            size = upper_bound - lower_bound

                            if size == 0:
                                continue

                            yield [
                                req_index,
                                from_id,
                                rel_tos[lower_bound:upper_bound],
                                rel_idx[lower_bound:upper_bound],
                            ]

                    yield list(_one_pick_result())

        all_picks_results = list(_all_picks_results())

        if len(all_picks_results) > 0:
            output = pd.DataFrame(
                data=functools.reduce(lambda l1, l2: l1 + l2, all_picks_results),
                columns=["req_idx", "from", named_as, "rel_idx"])

            if remove_selected:

                # remove all the relations of each from in one go since
                # no injective selection might have the same index several times
                g = output[output["rel_idx"] != -1][
                    ["from", "rel_idx"]].groupby(by="from")
                for from_id in g.groups:
                    group = self.grouped[from_id]
                    removed_idx = g.get_group(from_id)["rel_idx"].values[0]
                    group.remove_inplace(removed_idx)
                    if len(group) == 0:
                        del self.grouped[from_id]

        else:
            output = pd.DataFrame(
                columns=["req_idx", "from", named_as, "rel_idx"])

        output.set_index("req_idx", drop=True, inplace=True)
        output.drop(["rel_idx", "from"], axis=1, inplace=True)

        # "discard_empty" option: return empty result (instead of nothing) for
        # any non existing (i.e. empty) "from" relation
        if not discard_empty and output.shape[0] != len(from_ids):
            missing_index = from_ids.index.difference(output.index)
            missing_values = pd.DataFrame(
                {named_as: pd.Series([[] * missing_index.shape[0]],
                                     index=missing_index)})

            output = pd.concat([output, missing_values], copy=False)

        return output

    ######################
    # IO                 #
    ######################

    def save_to(self, file_path):
        """
        Saves all the relationship as well as the current status of the seed
        as a CSV file
        """
        logging.info("saving relationship to {}".format(file_path))

        # creating a vertical dataframe to store the inner table
        saved_df = pd.DataFrame(self.get_relations().stack(), columns=["value"])

        # we also want to save the seed => added an index level to separate
        # self._table from self.seed in the end result
        saved_df["param"] = "relations"
        saved_df = saved_df.set_index("param", append=True)
        saved_df.index = saved_df.index.reorder_levels([2, 0, 1])

        # then finally added the seed
        saved_df.loc[("seed", 0, 0)] = self.seed
        saved_df.to_csv(file_path)

    @staticmethod
    def load_from(file_path):
        logging.info("loading relationship from {}".format(file_path))

        saved_df = pd.read_csv(file_path, index_col=[0, 1, 2])
        seed = int(saved_df.loc["seed"].values[0][0])

        _all = slice(None)
        relations = saved_df.loc[("relations", _all, _all)].unstack()
        relations.index = relations.index.droplevel(0)
        relations.columns = relations.columns.droplevel(0)

        relationship = Relationship(seed)
        relationship.add_relations(
            from_ids=relations["from"].values,
            to_ids=relations["to"].values,
            weights=relations["weight"].values.astype(float))

        return relationship

    class RelationshipOps(object):
        def __init__(self, relationship):
            self.relationship = relationship

        class AddNeighbourhoodSize(AddColumns):
            def __init__(self, relationship, from_field, named_as):
                AddColumns.__init__(self)

                self.relationship = relationship
                self.from_field = from_field
                self.named_as = named_as

            def build_output(self, story_data):

                requested_froms = story_data[self.from_field]
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

            def build_output(self, story_data):
                selected = self.relationship.select_one(
                    from_ids=story_data[self.from_field],
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
                true, that row is removed from the story_data.

            :param weight: weight to use for the "to" side of the
                relationship. Must be a Series whose index are the "to" values.
                Typical usage would be to plug an attribute of the "to"
                population here.

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

            def transform(self, story_data):

                from_ids = story_data[[self.from_field]].drop_duplicates()
                selected = self.relationship.select_all_horizontal(
                    from_ids=from_ids[self.from_field].values,
                    named_as=self.named_as)

                selected.set_index("from", drop=True, inplace=True)
                return pd.merge(left=story_data, right=selected,
                                left_on=self.from_field, right_index=True)

        def select_all(self, from_field, named_as):
            """
            This simply creates a new story_data field containing all the
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

            def build_output(self, story_data):
                selected = self.relationship.select_many(
                    from_ids=story_data[self.from_field],
                    named_as=self.named_as,
                    quantities=story_data[self.quantity_field],
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

            def side_effect(self, story_data):
                if story_data.shape[0] > 0:
                    self.relationship.add_relations(
                        from_ids=story_data[self.from_field],
                        to_ids=story_data[self.item_field])

        def add(self, from_field, item_field):
            return self.Add(self.relationship, from_field, item_field)

        class AddGrouped(SideEffectOnly):
            def __init__(self, relationship, from_field, grouped_items_field):
                self.relationship = relationship
                self.from_field = from_field
                self.grouped_items_field = grouped_items_field

            def side_effect(self, story_data):
                if story_data.shape[0] > 0:

                    self.relationship.add_grouped_relations(
                        from_ids=story_data[self.from_field],
                        grouped_ids=story_data[self.grouped_items_field])

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

            def side_effect(self, story_data):
                if story_data.shape[0] > 0:
                    self.relationship.remove(
                        from_ids=story_data[self.from_field],
                        to_ids=story_data[self.item_field])

        def remove(self, from_field, item_field):
            return self.Remove(self.relationship, from_field, item_field)
