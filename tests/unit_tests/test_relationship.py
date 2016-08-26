from datagenerator.relationship import Relationship
from datagenerator.util_functions import *
setup_logging()


oneto1 = Relationship(seed=1)
oneto1.add_relations(from_ids=["a", "b", "c", "d", "e"],
                     to_ids=["ta", "tb", "tc", "td", "te"])

four_to_one = Relationship(seed=1)
four_to_one.add_relations(from_ids=["a", "b", "c", "d"],
                          to_ids=["z", "z", "z", "z"])

four_to_two = Relationship(seed=1)
four_to_two.add_relations(from_ids=["a", "b", "c", "d"],
                          to_ids=["y", "y", "y", "y"])
four_to_two.add_relations(from_ids=["a", "b", "c", "d"],
                          to_ids=["z", "z", "z", "z"])

two_per_from = Relationship(seed=1)
two_per_from.add_relations(from_ids=["a", "b", "c", "d"],
                           to_ids=["ya", "yb", "yc", "yd"])
two_per_from.add_relations(from_ids=["a", "b", "c", "d"],
                           to_ids=["za", "zb", "zc", "zd"])


four_to_plenty = Relationship(seed=1)
for i in range(100):
    four_to_plenty.add_relations(
        from_ids=["a", "b", "c", "d"],
        to_ids=["a_%d" % i, "b_%d" % i, "c_%d" % i, "d_%d" % i])


def test_missing_ids_should_return_empty_for_nothing_missing():
    r = oneto1.missing_ids(from_ids=["a", "b"])
    assert r == set([])


def test_missing_ids_should_identify_missing_correctly():
    r = oneto1.missing_ids(
        from_ids=["a", "b", "NON_EXISTING_1", "NON_EXISTING_2"])
    assert r == {"NON_EXISTING_1", "NON_EXISTING_2"}


# bug fix: this was simply crashing previously
def test_select_one_from_empty_relationship_should_return_void():
    tested = Relationship(seed=1)
    assert tested.select_one(pd.Series([])).shape[0] == 0


# bug fix: this was simply crashing previously
def test_select_one_from_non_existing_ids_should_return_void():
    tested = Relationship(seed=1)
    tested.add_relations(from_ids=["a", "b", "b", "c"],
                         to_ids=["b", "c", "a", "b"])

    assert tested.select_one(["non_existing_id", "neither"]).shape[0] == 0


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
              "cf1", "cf2", "cf3", ]

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
    action_data = pd.DataFrame({"A": ["a", "b", "c", "d"]})
    action_data = action_data.set_index("A", drop=False)

    output, logs = op(action_data)

    assert {} == logs
    assert sorted(output["B"].unique()) == sorted(output["B"])

    # with z being unique, only one A can call it with this select_one
    assert output.shape[0] == 1


def test_select_one_to_one_should_not_return_duplicates_2():

    op = four_to_two.ops.select_one("A", "B", one_to_one=True)
    action_data = pd.DataFrame({"A": ["a", "b", "c", "d"]})
    action_data = action_data.set_index("A", drop=False)

    output, logs = op(action_data)

    assert {} == logs
    assert sorted(output["B"].unique()) == sorted(output["B"])

    # with y, z being unique, we can only have 2 "to" sides
    assert 1 <= output.shape[0] <= 2


def test_select_one_to_one_among_no_data_should_return_nothing():
    #(instead of crashing...)

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
    action_data = pd.DataFrame({
        "DEALER": ["a", "a", "b", "a", "b", "a", "b", "a", "a", "a"],
        },
        index=build_ids(size=10, prefix="c", max_length=2)
    )

    result, logs = op(action_data)

    assert {} == logs
    assert ["DEALER", "SIM"] == result.columns.tolist()

    # There could be collisions that reduce the same of the resulting index,
    # but there should not be only collisions, leading to only "a" and "b"
    assert result.shape[0] > 3

    g = result.groupby("DEALER")["SIM"]

    assert len(np.unique(g.get_group("a").values)) > 1
    assert len(np.unique(g.get_group("b").values)) > 1


def test_select_all_should_return_all_values_of_requested_ids():

    all_to = two_per_from.select_all(from_ids=["a", "b", "non_existing"])

    # there is no relationship from "non_existing", so we should have an
    # empty list for it (not an absence of row)
    expected = pd.DataFrame({
        "from": ["a", "b", "non_existing"],
        "to": [["ya", "za"], ["yb", "zb"], []]
        }
    )
    assert all_to.equals(expected)


def test_select_all_should_return_lists_even_for_one_to_one():

    all_to = oneto1.select_all(from_ids=["a", "b", "non_existing"])

    expected = pd.DataFrame({
        "from": ["a", "b", "non_existing"],
        "to": [["ta"], ["tb"], []]
        }
    )
    assert all_to.equals(expected)


def test_select_all_operation():

    op = two_per_from.ops.select_all(from_field="A", named_as="CELLS")

    data = pd.DataFrame({"A": ["a",  "d", "non_existing"]})
    data = data.set_index("A", drop=False)
    output, logs = op(data)

    # the transformer should have added the "CELL" column to the df
    assert output.columns.values.tolist() == ["A", "CELLS"]

    assert {} == logs
    assert output.index.to_series().equals(output["A"])

    # output should correspond to the A column of data, indexed correctly
    assert output["CELLS"].sort_index().equals(
        pd.Series([["ya", "za"], ["yd", "zd"], []],
                  index=["a", "d", "non_existing"]))
