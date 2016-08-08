import pandas as pd
from datagenerator.relationship import Relationship


# bug fix: this was simply crashing previously
def test_select_one_from_empty_relationship_should_return_void():
    tested = Relationship(name="tested", seed=1)

    assert tested.select_one([]).shape[0] == 0


# bug fix: this was simply crashing previously
def test_select_one_from_non_existing_ids_should_return_void():
    tested = Relationship(name="tested", seed=1)
    tested.add_relations(from_ids=["a", "b", "b", "c"],
                         to_ids=["b", "c", "a", "b"])

    assert tested.select_one(["non_existing_id", "neither"]).shape[0] == 0


def test_select_one_from_all_ids_should_return_one_line_per_id():
    tested = Relationship(name="tested", seed=1)
    tested.add_relations(from_ids=["a", "b", "b", "c"],
                         to_ids=["b", "c", "a", "b"])

    selected = tested.select_one()

    assert set(selected["from"].unique()) == {"a", "b", "c"}


def test_one_to_one_relationship_should_find_unique_counterpart():

    oneto1= Relationship(name="tested", seed=1)
    oneto1.add_relations(from_ids=["a", "b", "c", "d", "e"],
                         to_ids=["ta", "tb", "tc", "td", "te"])

    selected = oneto1.select_one()

    assert selected.sort_values("from")["to"].equals(
        pd.Series(["ta", "tb", "tc", "td", "te"]))


def test_one_to_one_relationship_operation_should_find_unique_counterpart():

    oneto1= Relationship(name="tested", seed=1)
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

    four_to_one = Relationship(name="tested", seed=1)
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

    four_to_two = Relationship(name="tested", seed=1)
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
    assert output.shape[0] == 2


