import pandas as pd
from datagenerator.relationship import Relationship


# bug fix: this was simply crashing previously
def test_select_one_from_empty_relationship_should_return_void():
    tested = Relationship(seed=1)

    assert tested.select_one([]).shape[0] == 0


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

    oneto1= Relationship(seed=1)
    oneto1.add_relations(from_ids=["a", "b", "c", "d", "e"],
                         to_ids=["ta", "tb", "tc", "td", "te"])

    selected = oneto1.select_one()

    assert selected.sort_values("from")["to"].equals(
        pd.Series(["ta", "tb", "tc", "td", "te"]))


def test_one_to_one_relationship_operation_should_find_unique_counterpart():

    oneto1= Relationship(seed=1)
    oneto1.add_relations(from_ids=["a", "b", "c", "d", "e"],
                         to_ids=["ta", "tb", "tc", "td", "te"])

    op = oneto1.ops.select_one("A", "CELL")

    data = pd.DataFrame({"A": ["a", "e", "d"]}).set_index("A", drop=False)
    output, logs = op(data)

    # the transformer should have added the "CELL" column to the df
    assert output.columns.values.tolist() == ["A", "CELL"]

    assert {} == logs
    assert output.index.to_series().equals(output["A"])

    # output should correspond to the A column of data, in the correct order
    assert output["CELL"].values.tolist() == ["ta", "te", "td"]


def test_select_one_to_one_should_not_return_duplicates_1():

    four_to_one = Relationship(seed=1)
    four_to_one.add_relations(from_ids=["a", "b", "c", "d"],
                              to_ids=["z", "z", "z", "z"])

    op = four_to_one.ops.select_one("A", "B", one_to_one=True)
    action_data = pd.DataFrame({"A": ["a", "b", "c", "d"]})
    action_data = action_data.set_index("A", drop=False)

    output, logs = op(action_data)

    assert {} == logs
    assert sorted(output["B"].unique()) == sorted(output["B"])

    # with z being unique, only one A can call it with this select_one
    assert output.shape[0] == 1


def test_select_one_to_one_should_not_return_duplicates_2():

    four_to_two = Relationship(seed=1)
    four_to_two.add_relations(from_ids=["a", "b", "c", "d"],
                              to_ids=["y", "y", "y", "y"])
    four_to_two.add_relations(from_ids=["a", "b", "c", "d"],
                              to_ids=["z", "z", "z", "z"])

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

    four_to_one = Relationship(seed=1)
    four_to_one.add_relations(from_ids=["a", "b", "c", "d"],
                              to_ids=["z", "z", "z", "z"])

    op = four_to_one.ops.select_one("A", "B", one_to_one=True)
    empty_data = pd.DataFrame(columns=["A", "B"])

    output, logs = op(empty_data)

    assert {} == logs

    # with z being unique, only one A can call it with this select_one
    assert output.shape[0] == 0


