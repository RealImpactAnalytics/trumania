from datagenerator.actor import Actor
import pandas as pd

dummy_actor = Actor(size=10, max_length=1)

ages = [10, 20, 40, 10, 100, 98, 12, 39, 76, 23]
dummy_actor.create_attribute("age", init_values=ages)

city = ["a", "b", "b", "a", "d", "e", "r", "a", "z", "c"]
dummy_actor.create_attribute("city", init_values=city)

# some fake action data with an index corresponding to another actor
# => simulates an action triggered by that other actor
# the column "NEIGHBOUR" contains value that point to the dummy actor, with
# a duplication (id2)
action_data = pd.DataFrame({
        "A": ["a1", "a2", "a3", "a4"],
        "B": ["b1", "b2", "b3", "b4"],
        "NEIGHBOUR": ["id_2", "id_4", "id_7", "id_2"],
        "COUSINS": [
            ["id_2", "id_4", "id_7", "id_2"],
            ["id_3"],
            ["id_4", "id_5", "id_8"],
            ["id_0", "id_4"],
        ],
    },
    index=["cust_1", "cust_2", "cust_3", "cust_4"]
)


def test_resulting_size_should_be_as_expected():
    assert dummy_actor.size == 10
    assert len(dummy_actor.ids) == 10


def test_transforming_actor_to_dataframe_should_provide_all_data():
    df = dummy_actor.to_dataframe()

    # order of the columns in the resulting dataframe is currently not
    # deterministic
    assert sorted(df.columns) == ["age", "city"]

    assert df["age"].values.tolist() == ages
    assert df["city"].values.tolist() == city


def test_lookup_values_by_scalar_should_return_correct_values():

    lookup = dummy_actor.ops.lookup(
        actor_id_field="NEIGHBOUR",
        select={
            "age": "neighbour_age",
            "city": "neighbour_city",
        }
    )

    result, logs = lookup(action_data)
    expected_cols = ["A", "B", "COUSINS", "NEIGHBOUR", "neighbour_age",
                     "neighbour_city"]
    assert logs == {}

    assert sorted(result.columns) == expected_cols

    # values of the age corresponding to the neighbour id
    assert [40, 100, 39, 40] == result["neighbour_age"].tolist()

    # values of the age corresponding to the neighbour id
    assert ["b", "d", "a", "b"] == result["neighbour_city"].tolist()


def test_lookup_values_by_array_should_return_correct_values():

    lookup = dummy_actor.ops.lookup(
        actor_id_field="COUSINS",
        select={
            "age": "cousin_age",
            "city": "cousin_city",
        }
    )

    result, logs = lookup(action_data)
    expected_cols = ["A", "B", "COUSINS", "NEIGHBOUR", "cousin_age",
                     "cousin_city"]
    assert logs == {}

    assert sorted(result.columns) == expected_cols

    # list of values of the age corresponding to the coussin id, in the correct
    # order
    assert [
               [40, 100, 39, 40],
               [10],
               [100, 98, 76],
               [10, 100]
           ] == result["cousin_age"].tolist()

    assert [
               ["b", "d", "a", "b"],
               ["a"],
               ["d", "e", "z"],
               ["a", "d"]
           ] == result["cousin_city"].tolist()
