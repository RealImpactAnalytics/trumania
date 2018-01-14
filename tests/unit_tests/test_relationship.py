import path
import pandas as pd
import logging
import os
import numpy as np
import functools

from trumania.core.util_functions import setup_logging
from trumania.core.util_functions import build_ids
from trumania.core.relationship import Relationship

setup_logging()

oneto1 = Relationship(seed=1)
oneto1.add_relations(from_ids=pd.Series(["a", "b", "c", "d", "e"]),
                     to_ids=pd.Series(["ta", "tb", "tc", "td", "te"]))

four_to_one = Relationship(seed=1)
four_to_one.add_relations(from_ids=pd.Series(["a", "b", "c", "d"]),
                          to_ids=pd.Series(["z", "z", "z", "z"]))

four_to_two = Relationship(seed=1)
four_to_two.add_relations(from_ids=pd.Series(["a", "b", "c", "d"]),
                          to_ids=pd.Series(["y", "y", "y", "y"]))
four_to_two.add_relations(from_ids=pd.Series(["a", "b", "c", "d"]),
                          to_ids=pd.Series(["z", "z", "z", "z"]))

two_per_from = Relationship(seed=1)
two_per_from.add_relations(from_ids=pd.Series(["a", "b", "c", "d"]),
                           to_ids=pd.Series(["ya", "yb", "yc", "yd"]))
two_per_from.add_relations(from_ids=pd.Series(["a", "b", "c", "d"]),
                           to_ids=pd.Series(["za", "zb", "zc", "zd"]))

four_to_plenty = Relationship(seed=123456)
for i in range(100):
    four_to_plenty.add_relations(
        from_ids=["a", "b", "c", "d"],
        to_ids=["a_%d" % i, "b_%d" % i, "c_%d" % i, "d_%d" % i])


def test_get_relations_from_specified_ids_should_be_as_expected():
    relations = oneto1.get_relations(from_ids=["b", "c", "non_existing", "e"])

    assert relations.sort_values("from")["from"].tolist() == ["b", "c", "e"]
    assert relations.sort_values("from")["to"].tolist() == ["tb", "tc", "te"]

    # default weights should have been assigned to the relationships
    assert relations["weight"].tolist() == [1, 1, 1]


def test_get_relations_from_non_existing_ids_should_have_correct_columns():

    relations = oneto1.get_relations(from_ids=["non_existing", "neither"])

    assert relations.shape[0] == 0
    assert relations.columns.tolist() == ["from", "to", "weight"]


def test_get_neighbourhood_size_of_known_ids_should_return_correct_value():

    onetoone_size = oneto1.get_neighbourhood_size(from_ids=["b", "c", "d"])
    assert onetoone_size.equals(pd.Series([1, 1, 1], index=["b", "c", "d"]))

    fourtoone_size = four_to_one.get_neighbourhood_size(from_ids=["b", "c", "d"])
    assert fourtoone_size.equals(pd.Series([1, 1, 1], index=["b", "c", "d"]))

    four_to2size = four_to_two.get_neighbourhood_size(from_ids=["b", "c", "d"])
    assert four_to2size.equals(pd.Series([2, 2, 2], index=["b", "c", "d"]))

    four_to100size = four_to_plenty.get_neighbourhood_size(from_ids=["b", "c"])
    assert four_to100size.equals(pd.Series([100, 100], index=["b", "c"]))


def test_get_neighbourhood_size_of_duplicated_ids_should_return_one_per_value():

    onetoone_size = oneto1.get_neighbourhood_size(from_ids=["b", "b", "b"])
    assert onetoone_size.equals(pd.Series([1], index=["b"]))

    fourtoone_size = four_to_one.get_neighbourhood_size(from_ids=["c", "c"])
    assert fourtoone_size.equals(pd.Series([1], index=["c"]))


def test_get_neighbourhood_size_of_unknown_ids_should_return_0():

    onetoone_size = oneto1.get_neighbourhood_size(from_ids=["b", "c",
                                                            "non_existing"])

    assert onetoone_size.index.tolist() == ["b", "c", "non_existing"]
    assert onetoone_size[["b", "c", "non_existing"]].tolist() == [1, 1, 0]


def test_get_neihgbourhood_size_operation_should_add_expected_column():

    op = four_to_plenty.ops.get_neighbourhood_size(
        from_field="A", named_as="A_NGH_SIZE")

    # with several times a lookup from value a
    data = pd.DataFrame({"A": ["a", "non_existing", "d", "a"]})
    output, logs = op(data)

    # the transformer should have added the "CELL" column to the df
    assert output.columns.values.tolist() == ["A", "A_NGH_SIZE"]

    assert {} == logs

    # no rows should have been dropped
    assert output.shape[0] == data.shape[0]
    assert output["A_NGH_SIZE"].tolist() == [100, 0, 100, 100]


# bug fix: this was simply crashing previously
def test_select_one_from_empty_relationship_should_return_void():
    tested = Relationship(seed=1)
    result = tested.select_one(pd.Series([]))
    assert result.shape[0] == 0
    assert result.columns.tolist() == ["from", "to"]


def test_select_one_from_empty_rel_should_return_empty_if_not_keep_missing():
    empty_relationship = Relationship(seed=1)

    selected = empty_relationship.select_one(from_ids=["non_existing"],
                                             discard_empty=True)
    assert selected.shape == (0, 2)
    assert selected.columns.tolist() == ["from", "to"]


def test_select_one_from_empty_rel_should_return_none_if_keep_missing():
    empty_relationship = Relationship(seed=1)

    selected = empty_relationship.select_one(from_ids=["non_existing"],
                                             discard_empty=False)
    assert selected.shape == (1, 2)
    assert selected.columns.tolist() == ["from", "to"]
    assert selected.iloc[0]["from"] == "non_existing"
    assert selected.iloc[0]["to"] is None


def test_select_one_nonexistingids_should_return_empty_if_not_keep_missing():
    tested = Relationship(seed=1)
    tested.add_relations(from_ids=["a", "b", "b", "c"],
                         to_ids=["b", "c", "a", "b"])

    result = tested.select_one(["non_existing_id", "neither"],
                               discard_empty=True)

    assert result.shape[0] == 0
    assert result.columns.tolist() == ["from", "to"]


def test_select_one_nonexistingids_should_insert_none_if_keep_missing():
    tested = Relationship(seed=1)
    tested.add_relations(from_ids=["a", "b", "b", "c"],
                         to_ids=["a1", "b1", "b2", "c1"])

    result = tested.select_one(["c", "b_non_existing_id", "a", "neither", "a"],
                               discard_empty=False)

    assert result.shape[0] == 5
    assert result.columns.tolist() == ["from", "to"]

    result_s = result.sort_values("from")

    assert result_s["from"].tolist() == ["a", "a", "b_non_existing_id", "c",
                                         "neither"]

    assert result_s["to"].tolist() == ["a1", "a1", None, "c1", None, ]


def test_select_one_from_all_ids_should_return_one_line_per_id():
    tested = Relationship(seed=1)
    tested.add_relations(from_ids=["a", "b", "b", "c"],
                         to_ids=["b", "c", "a", "b"])

    selected = tested.select_one()

    assert set(selected["from"].unique()) == {"a", "b", "c"}


def test_seeded_relationship_should_always_return_same_selection():

    from_ids = ["a", "a", "a",
                "b", "b", "b",
                "c", "c", "c"]
    to_ids = ["af1", "af2", "af3",
              "bf1", "bf2", "bf3",
              "cf1", "cf2", "cf3"]

    # two relationship seeded identically
    tested1 = Relationship(seed=1345)
    tested2 = Relationship(seed=1345)

    tested1.add_relations(from_ids=from_ids, to_ids=to_ids)
    tested2.add_relations(from_ids=from_ids, to_ids=to_ids)

    assert tested1.select_one(from_ids=["a"]).equals(
            tested2.select_one(from_ids=["a"]))

    assert tested1.select_one(from_ids=["b"]).equals(
            tested2.select_one(from_ids=["b"]))

    assert tested1.select_one(from_ids=["a", "b", "d"]).equals(
            tested2.select_one(from_ids=["a", "b", "d"]))


def test_one_to_one_relationship_should_find_unique_counterpart():

    selected = oneto1.select_one()
    assert selected.sort_values("from")["to"].tolist() == ["ta", "tb", "tc",
                                                           "td", "te"]


def test_weighted_relationship_should_take_weights_into_account():

    # a,b and c are all connected to x,y and z, but the weight is 0
    # everywhere except to y
    one_to_three_weighted = Relationship(seed=1234)
    one_to_three_weighted.add_relations(
        from_ids=["a"] * 3 + ["b"] * 3 + ["c"] * 3,
        to_ids=["x", "y", "z"] * 3,
        weights=[0, 1, 0] * 3
    )

    selected = one_to_three_weighted.select_one()

    # => with those weights, only x should should be selected
    assert selected["to"].tolist() == ["y", "y", "y"]
    assert sorted(selected["from"].tolist()) == ["a", "b", "c"]


def test_weighted_relationship_should_take_overridden_weights_into_account():

    # a,b and c are all connected to x,y and z, but the weight is 0
    # everywhere except to y
    one_to_three_weighted = Relationship(seed=1234)
    one_to_three_weighted.add_relations(
        from_ids=["a"] * 3 + ["b"] * 3 + ["c"] * 3,
        to_ids=["x", "y", "z"] * 3,
        weights=[0, 1, 0] * 3
    )

    # if we override the weight, we can only specify one value per "to" value
    overridden_to_weights = pd.Series(
        data=[0, 0, 1],
        index=["x", "y", "z"]
    )
    selected = one_to_three_weighted.select_one(
        overridden_to_weights=overridden_to_weights
    )

    # the initial weights should have been discarded and the one provided as
    # input should have been joined and used as expected
    assert selected["to"].tolist() == ["z", "z", "z"]
    assert sorted(selected["from"].tolist()) == ["a", "b", "c"]


def test_pop_one_relationship_should_remove_element():
    # we're removing relations from this one => working on a copy not to
    # influence other tests
    oneto1_copy = Relationship(seed=1)
    oneto1_copy.add_relations(from_ids=["a", "b", "c", "d", "e"],
                              to_ids=["ta", "tb", "tc", "td", "te"])

    selected = oneto1_copy.select_one(from_ids=["a", "d"], remove_selected=True)

    # unique "to" value should have been taken
    assert selected.sort_values("from")["to"].tolist() == ["ta", "td"]
    assert selected.columns.tolist() == ["from", "to"]

    # and removed form the relationship
    assert set(oneto1_copy.grouped.keys()) == {"b", "c", "e"}

    # selecting the same again should just return nothing
    selected = oneto1_copy.select_one(from_ids=["a", "d"], remove_selected=True)

    assert selected.shape[0] == 0
    assert selected.columns.tolist() == ["from", "to"]

    # and have no impact on the relationship
    assert set(oneto1_copy.grouped.keys()) == {"b", "c", "e"}

    # selecting the same again without discarding empty relationship should
    # now return a size 2 dataframe with Nones
    selected = oneto1_copy.select_one(
        from_ids=["a", "d"], remove_selected=True,
        discard_empty=False)
    assert selected.shape[0] == 2
    assert sorted(selected.columns.tolist()) == ["from", "to"]
    assert selected["to"].tolist() == [None, None]
    assert sorted(selected["from"].tolist()) == ["a", "d"]


def test_one_to_one_relationship_operation_should_find_unique_counterpart():

    op = oneto1.ops.select_one(from_field="A", named_as="CELL")

    # with several times a lookup from value a
    data = pd.DataFrame({"A": ["a", "e", "d", "a"]})
    output, logs = op(data)

    # the transformer should have added the "CELL" column to the df
    assert output.columns.values.tolist() == ["A", "CELL"]

    assert {} == logs

    # no rows should have been dropped
    assert output.shape[0] == data.shape[0]

    # output should correspond to the A column of data, indexed correctly
    # assert output["CELL"].sort_index().equals(
    #     pd.Series(["ta", "td", "te"], index=["a", "d", "e"]))
    assert output.sort_values("A")["CELL"].tolist() == ["ta", "ta", "td", "te"]


def test_select_one_to_one_should_not_return_duplicates_1():

    op = four_to_one.ops.select_one(from_field="A", named_as="B",
                                    one_to_one=True)
    story_data = pd.DataFrame({"A": ["a", "b", "c", "d"]})
    story_data = story_data.set_index("A", drop=False)

    output, logs = op(story_data)

    assert {} == logs
    assert sorted(output["B"].unique()) == sorted(output["B"])

    # with z being unique, only one A can call it with this select_one
    assert output.shape[0] == 1


def test_select_one_to_one_should_not_return_duplicates_2():

    op = four_to_two.ops.select_one("A", "B", one_to_one=True)
    story_data = pd.DataFrame({"A": ["a", "b", "c", "d"]})
    story_data = story_data.set_index("A", drop=False)

    output, logs = op(story_data)

    assert {} == logs
    assert sorted(output["B"].unique()) == sorted(output["B"])

    # with y, z being unique, we can only have 2 "to" sides
    assert 1 <= output.shape[0] <= 2


def test_select_one_to_one_among_no_data_should_return_nothing():
    # (instead of crashing...)

    op = four_to_one.ops.select_one("A", "B", one_to_one=True)
    empty_data = pd.DataFrame(columns=["A", "B"])

    output, logs = op(empty_data)

    assert {} == logs

    # with z being unique, only one A can call it with this select_one
    assert output.shape[0] == 0


def test_select_one_from_many_times_same_id_should_yield_different_results():

    op = four_to_plenty.ops.select_one(from_field="DEALER",
                                       named_as="SIM",
                                       one_to_one=True)

    # Many customer selected the same dealer and want to get a sim from them.
    # We expect each of the 2 selected dealer to sell a different SIM to each
    story_data = pd.DataFrame(
        {
            "DEALER": ["a", "a", "b", "a", "b", "a", "b", "a", "a", "a"],
        },
        index=build_ids(size=10, prefix="c", max_length=2)
    )

    result, logs = op(story_data)
    logging.info("selected")

    assert {} == logs
    assert ["DEALER", "SIM"] == result.columns.tolist()

    # There could be collisions that reduce the same of the resulting index,
    # but there should not be only collisions, leading to only "a" and "b"
    assert result.shape[0] > 3

    g = result.groupby("DEALER")["SIM"]

    assert len(np.unique(g.get_group("a").values)) > 1
    assert len(np.unique(g.get_group("b").values)) > 1


def test_select_all_function_from_empty_relationship_should_return_empty():
    empty_relationship = Relationship(seed=1)

    selected = empty_relationship.select_all_horizontal(from_ids=["non_existing"])

    assert selected.shape == (0, 2)
    assert selected.columns.tolist() == ["from", "to"]


def test_select_all_should_return_all_values_of_requested_ids():

    all_to = two_per_from.select_all_horizontal(from_ids=["a", "b"]).sort_values(by="from").reset_index(drop=True)

    # there is no relationship from "non_existing", so we should have an
    # empty list for it (not an absence of row)
    expected = pd.DataFrame(
        {
            "from": ["a", "b"],
            "to": [["ya", "za"], ["yb", "zb"]]
        }
    ).sort_values(by="from").reset_index(drop=True)

    assert all_to.sort_values(by="from").equals(
        expected.sort_values(by="from")), "dataframe\n {} should equal dataframe:\n {}".format(all_to, expected)


def test_select_all_should_return_lists_even_for_one_to_one():

    all_to = oneto1.select_all_horizontal(from_ids=["a", "b"]).sort_values(by="from").reset_index(drop=True)

    expected = pd.DataFrame(
        {
            "from": ["a", "b"],
            "to": [["ta"], ["tb"]]
        }
    ).sort_values(by="from").reset_index(drop=True)

    assert all_to.sort_values(by="from").equals(expected)


def test_select_all_operation():

    op = two_per_from.ops.select_all(from_field="A", named_as="CELLS")

    story_data = pd.DataFrame({"A": ["a", "d"]})
    story_data = story_data.set_index("A", drop=False)
    output, logs = op(story_data)

    # the transformer should have added the "CELL" column to the df
    assert output.columns.values.tolist() == ["A", "CELLS"]

    assert {} == logs
    assert output.index.to_series().equals(output["A"])

    # output should correspond to the A column of data, indexed correctly
    assert output["CELLS"].sort_index().equals(
        pd.Series([["ya", "za"], ["yd", "zd"]],
                  index=["a", "d"]))


def test_select_many_should_return_subsets_of_relationships():

    story_data_index = build_ids(5, prefix="cl_", max_length=1)

    # cheating with the seed for the second part of the test
    four_to_plenty.state = np.random.RandomState(18)
    selection = four_to_plenty.select_many(
        from_ids=pd.Series(["a", "b", "c", "b", "a"], index=story_data_index),
        named_as="selected_sets",

        # On purpose requesting non-integer quantities => these should be
        # rounded to int. It's very common to have them in practise, typically
        # when generating "bulk size" out of a non-integer distribution
        quantities=[4, 5, 6.5, 7.5, 8],
        remove_selected=False,
        discard_empty=False)

    # this index is expected among other things since it allows a direct
    # merge into the initial request
    assert sorted(selection.index.tolist()) == story_data_index
    assert selection.columns.tolist() == ["selected_sets"]

    # no capping should have occured: four_to_plenty has largely enough
    assert sorted(selection["selected_sets"].apply(len).tolist()) == [4, 5, 6, 7, 8]

    # every chosen elemnt should be persent at most once
    s = functools.reduce(lambda s1, s2: set(s1) | set(s2), selection["selected_sets"])
    assert len(s) == np.sum([4, 5, 6, 7, 8])

    # selecting the same thing => should return the same result since
    # remove_selected is False and the relationship is seeded
    four_to_plenty.state = np.random.RandomState(18)
    selection_again = four_to_plenty.select_many(
        from_ids=pd.Series(["a", "b", "c", "b", "a"], index=story_data_index),
        named_as="selected_sets",
        quantities=[4, 5, 6, 7, 8],
        remove_selected=False,
        discard_empty=False)

    assert selection.sort_index().index.equals(selection_again.sort_index().index)
    for idx in selection.index:
        assert selection.ix[idx]["selected_sets"].tolist() == selection_again.ix[idx]["selected_sets"].tolist()


def test_select_many_with_drop_should_remove_elements():

    story_data_index = build_ids(5, prefix="cl_", max_length=1)

    # makes a copy since we're going to drop some elements
    four_to_plenty_copy = Relationship(seed=1)
    for i in range(100):
        four_to_plenty_copy.add_relations(
            from_ids=["a", "b", "c", "d"],
            to_ids=["a_%d" % i, "b_%d" % i, "c_%d" % i, "d_%d" % i])

    selection = four_to_plenty.select_many(
        from_ids=pd.Series(["a", "b", "c", "b", "a"], index=story_data_index),
        named_as="selected_sets",
        quantities=[4, 5, 6, 7, 8],
        remove_selected=True,
        discard_empty=False)

    # makes sure all selected values have been removed
    for from_id in selection.index:
        for to_id in selection.ix[from_id]["selected_sets"].tolist():
            rels = four_to_plenty_copy.get_relations(from_ids=[from_id])
            assert to_id not in rels["to"]


def test_select_many_several_times_with_pop_should_empty_all_data():

    rel = Relationship(seed=1234)
    froms = ["id1"] * 2500 + ["id2"] * 1500 + ["id3"] * 500
    tos = np.random.choice(a=range(10), size=len(froms))
    rel.add_relations(from_ids=froms, to_ids=tos)

    assert rel.get_relations().shape[0] == 2500 + 1500 + 500

    # we'll be selecting 1000 values from all 3 ids, 3 times

    # first selection: we should be able to get some values out, though id3
    # should already be exhausted
    selection1 = rel.select_many(
        from_ids=pd.Series(["id1", "id2", "id3"],
                           index=["f1", "f2", "f3"]),
        named_as="the_selection",
        quantities=[1000, 1000, 1000],
        remove_selected=True,
        discard_empty=False
    )

    assert selection1.columns.tolist() == ["the_selection"]
    assert sorted(selection1.index.tolist()) == ["f1", "f2", "f3"]

    # only 500 could be obtained from "id3":
    selection_sizes1 = selection1["the_selection"].map(len)
    assert selection_sizes1[["f1", "f2", "f3"]].tolist() == [1000, 1000, 500]

    # remove_selected => size of the relationship should have decreased
    assert rel.get_relations().shape[0] == 1500 + 500 + 0

    # second selection: similar story for id2 as for id3, plus now id3 should
    # just return an empty list (since discard_empty is False)
    selection2 = rel.select_many(
        from_ids=pd.Series(["id1", "id2", "id3"],
                           index=["f1", "f2", "f3"]),
        named_as="the_selection",
        quantities=[1000, 1000, 1000],
        remove_selected=True,
        discard_empty=False
    )

    assert selection2.columns.tolist() == ["the_selection"]
    assert sorted(selection2.index.tolist()) == ["f1", "f2", "f3"]

    # only 500 could be obtained from "id2" and nothing from "id2":
    selection_sizes2 = selection2["the_selection"].map(len)
    assert selection_sizes2[["f1", "f2", "f3"]].tolist() == [1000, 500, 0]

    # remove_selected => size of the relationship should have decreased
    assert rel.get_relations().shape[0] == 500 + 0 + 0

    # third selection: should be very simlar to above
    selection3 = rel.select_many(
        from_ids=pd.Series(["id1", "id2", "id3"],
                           index=["f1", "f2", "f3"]),
        named_as="the_selection",
        quantities=[1000, 1000, 1000],
        remove_selected=True,
        discard_empty=False
    )

    assert selection3.columns.tolist() == ["the_selection"]
    assert sorted(selection3.index.tolist()) == ["f1", "f2", "f3"]

    selection_sizes3 = selection3["the_selection"].map(len)
    assert selection_sizes3[["f1", "f2", "f3"]].tolist() == [500, 0, 0]

    # the relationship should now be empty
    assert rel.get_relations().shape[0] == 0 + 0 + 0

    # one last time: selection from a fully empty relationship
    # third selection: should be very similar to above
    selection4 = rel.select_many(
        from_ids=pd.Series(["id1", "id2", "id3"],
                           index=["f1", "f2", "f3"]),
        named_as="the_selection",
        quantities=[1000, 1000, 1000],
        remove_selected=True,
        discard_empty=False
    )

    assert selection4.columns.tolist() == ["the_selection"]
    assert sorted(selection4.index.tolist()) == ["f1", "f2", "f3"]

    selection_sizes4 = selection4["the_selection"].map(len)
    assert selection_sizes4[["f1", "f2", "f3"]].tolist() == [0, 0, 0]

    # relationship should still be empty
    assert rel.get_relations().shape[0] == 0


def test_select_many_operation_should_join_subsets_of_relationships():
    # same test as above, but from the operation

    story_data = pd.DataFrame({
            "let": ["a", "b", "c", "b", "a"],
            "how_many": [4, 5, 6, 7, 8]
        },
        index=build_ids(5, prefix="wh_", max_length=2)
    )

    select_op = four_to_plenty.ops.select_many(
        from_field="let",
        named_as="found",
        pop=False,
        quantity_field="how_many",
        discard_missing=False,
    )

    selection, logs = select_op(story_data)

    # this index is expected among other things since it allows a direct
    # merge into the initial request
    assert selection.sort_index().index.equals(story_data.sort_index().index)

    assert selection.columns.tolist() == ["how_many", "let", "found"]

    # no capping should have occurred: four_to_plenty has largely enough
    assert selection["found"].apply(len).tolist() == [4, 5, 6, 7, 8]

    # every chosen element should be present at most once
    s = functools.reduce(lambda s1, s2: set(s1) | set(s2), selection["found"])
    assert len(s) == np.sum([4, 5, 6, 7, 8])

    # all relationships in wh00 must come from a
    a_tos = four_to_plenty.get_relations(["a"])["to"]
    for f in selection.loc["wh_00", "found"]:
        assert f in a_tos.values
    for f in selection.loc["wh_04", "found"]:
        assert f in a_tos.values

    b_tos = four_to_plenty.get_relations(["b"])["to"]
    for f in selection.loc["wh_01", "found"]:
        assert f in b_tos.values
    for f in selection.loc["wh_03", "found"]:
        assert f in b_tos.values

    c_tos = four_to_plenty.get_relations(["c"])["to"]
    for f in selection.loc["wh_02", "found"]:
        assert f in c_tos.values


def test_add_grouped():
    story_data = pd.DataFrame({"boxes": ["b1", "b2"],
                               "fruits": [["f11", "f12", "f13", "f14"],
                                          ["f21", "f22", "f23", "f24"]],

                               })

    rel = Relationship(seed=1)
    ag = rel.ops.add_grouped(from_field="boxes", grouped_items_field="fruits")

    ag(story_data)

    # we should have 4 relationships from b1 and from b2
    assert rel.get_relations(from_ids=["b1"])["from"].tolist() == [
        "b1", "b1", "b1", "b1"]

    assert rel.get_relations(from_ids=["b2"])["from"].tolist() == [
        "b2", "b2", "b2", "b2"]

    # pointing to each of the values above
    assert rel.get_relations(from_ids=["b1"])["to"].tolist() == [
        "f11", "f12", "f13", "f14"]
    assert rel.get_relations(from_ids=["b2"])["to"].tolist() == [
        "f21", "f22", "f23", "f24"]


def test_io_round_trip():

    with path.tempdir() as p:
        full_path = os.path.join(p, "relationship.csv")
        four_to_plenty.save_to(full_path)

        retrieved = Relationship.load_from(full_path)

        assert four_to_plenty.seed == retrieved.seed
        assert four_to_plenty.unique_tos() == retrieved.unique_tos()
        assert four_to_plenty.grouped.keys() == retrieved.grouped.keys()

        expected_relations = four_to_plenty.get_relations().sort_values([
            "from", "to"]).reset_index()
        actual_relations = retrieved.get_relations().sort_values(["from", "to"]).reset_index()

        assert expected_relations["from"].equals(actual_relations["from"])
        assert expected_relations["to"].equals(actual_relations["to"])
        assert expected_relations["weight"].equals(actual_relations["weight"])
