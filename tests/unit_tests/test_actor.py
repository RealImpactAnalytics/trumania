import path
import pandas as pd
import os

from trumania.core.random_generators import SequencialGenerator
from trumania.core.actor import Actor

dummy_actor = Actor(circus=None,
                    size=10,
                    ids_gen=SequencialGenerator(max_length=1, prefix="id_"))

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


def test_lookup_operation_from_empty_action_data_should_return_empty_df_with_all_columns():

    lookup = dummy_actor.ops.lookup(
        actor_id_field="NEIGHBOUR",
        select={
            "age": "neighbour_age",
            "city": "neighbour_city",
        }
    )

    empty_action_data = pd.DataFrame(columns=["A", "B", "COUSINS", "NEIGHBOUR"])

    result, logs = lookup(empty_action_data)
    expected_cols = ["A", "B", "COUSINS", "NEIGHBOUR", "neighbour_age",
                     "neighbour_city"]
    assert logs == {}
    assert sorted(result.columns) == expected_cols
    assert result.shape[0] == 0


def test_lookup_values_by_array_should_return_correct_values():

    lookup = dummy_actor.ops.lookup(
        actor_id_field="COUSINS",
        select={
            "age": "cousins_age",
            "city": "cousins_city",
        }
    )

    result, logs = lookup(action_data)
    expected_cols = ["A", "B", "COUSINS", "NEIGHBOUR", "cousins_age",
                     "cousins_city"]
    assert logs == {}

    assert sorted(result.columns) == expected_cols

    # list of values of the age corresponding to the coussin id, in the correct
    # order
    assert [
               [40, 100, 39, 40],
               [10],
               [100, 98, 76],
               [10, 100]
           ] == result["cousins_age"].tolist()

    assert [
               ["b", "d", "a", "b"],
               ["a"],
               ["d", "e", "z"],
               ["a", "d"]
           ] == result["cousins_city"].tolist()


def test_insert_actor_value_for_existing_actors_should_update_all_values():

    # copy of dummy actor that will be updated
    tested_actor = Actor(
        circus=None,
        size=10,
        ids_gen=SequencialGenerator(max_length=1, prefix="a_")
    )
    ages = [10, 20, 40, 10, 100, 98, 12, 39, 76, 23]
    tested_actor.create_attribute("age", init_values=ages)
    city = ["a", "b", "b", "a", "d", "e", "r", "a", "z", "c"]
    tested_actor.create_attribute("city", init_values=city)

    current = tested_actor.get_attribute_values("age", ["a_0", "a_7", "a_9"])
    assert current.tolist() == [10, 39, 23]

    update = pd.DataFrame({
            "age": [139, 123],
            "city": ["city_7", "city_9"]
            },
        index=["a_7", "a_9"])

    tested_actor.update(update)

    # we should have the same number of actors
    assert tested_actor.ids.shape[0] == 10

    updated_age = tested_actor.get_attribute_values("age", ["a_0", "a_7", "a_9"])
    updated_city = tested_actor.get_attribute_values("city", ["a_0", "a_7", "a_9"])

    assert updated_age.tolist() == [10, 139, 123]
    assert updated_city.tolist() == ["a", "city_7", "city_9"]


def test_insert_actor_value_for_existing_and_new_actors_should_update_and_add_values():

    # copy of dummy actor that will be updated
    tested_actor = Actor(
        circus=None, size=10,
        ids_gen=SequencialGenerator(max_length=1, prefix="a_"))
    ages = [10, 20, 40, 10, 100, 98, 12, 39, 76, 23]
    tested_actor.create_attribute("age", init_values=ages)
    city = ["a", "b", "b", "a", "d", "e", "r", "a", "z", "c"]
    tested_actor.create_attribute("city", init_values=city)

    current = tested_actor.get_attribute_values("age", ["a_0", "a_7", "a_9"])
    assert current.tolist() == [10, 39, 23]

    update = pd.DataFrame({
            "age": [139, 123, 54, 25],
            "city": ["city_7", "city_9", "city_11", "city_10"]
            },
        index=["a_7", "a_9", "a_11", "a_10"])

    tested_actor.update(update)

    # we should have 2 new actors
    assert tested_actor.ids.shape[0] == 12

    updated_age = tested_actor.get_attribute_values("age", ["a_0", "a_7", "a_9", "a_10", "a_11"])
    updated_city = tested_actor.get_attribute_values("city", ["a_0", "a_7", "a_9", "a_10", "a_11"])

    assert updated_age.tolist() == [10, 139, 123, 25, 54]
    assert updated_city.tolist() == ["a", "city_7", "city_9", "city_10", "city_11"]


def test_insert_op_actor_value_for_existing_actors_should_update_all_values():
    # same as test above but triggered as an Operation on action data

    # copy of dummy actor that will be updated
    tested_actor = Actor(
        circus=None, size=10,
        ids_gen=SequencialGenerator(max_length=1, prefix="a_"))
    ages = [10, 20, 40, 10, 100, 98, 12, 39, 76, 23]
    tested_actor.create_attribute("age", init_values=ages)
    city = ["a", "b", "b", "a", "d", "e", "r", "a", "z", "c"]
    tested_actor.create_attribute("city", init_values=city)

    action_data = pd.DataFrame({
            "the_new_age": [139, 123, 1, 2],
            "location": ["city_7", "city_9", "city_11", "city_10"],
            "updated_actors": ["a_7", "a_9", "a_11", "a_10"]
            },
        index=["d_1", "d_2", "d_4", "d_3"])

    update_op = tested_actor.ops.update(
        actor_id_field="updated_actors",
        copy_attributes_from_fields={
            "age": "the_new_age",
            "city": "location"
        }
    )

    action_data_2, logs = update_op(action_data)

    # there should be no impact on the action data
    assert action_data_2.shape == (4, 3)
    assert sorted(action_data_2.columns.tolist()) == ["location", "the_new_age", "updated_actors"]

    # we should have 2 new actors
    assert tested_actor.ids.shape[0] == 12

    updated_age = tested_actor.get_attribute_values("age", ["a_0", "a_7", "a_9", "a_10", "a_11"])
    updated_city = tested_actor.get_attribute_values("city", ["a_0", "a_7", "a_9", "a_10", "a_11"])

    assert updated_age.tolist() == [10, 139, 123, 2, 1]
    assert updated_city.tolist() == ["a", "city_7", "city_9", "city_10", "city_11"]


def test_creating_an_empty_actor_and_adding_attributes_later_should_be_possible():

    # empty actor
    a = Actor(circus=None, size=0)
    assert a.ids.shape[0] == 0

    # empty attributes
    a.create_attribute("att1")
    a.create_attribute("att2")

    dynamically_created = pd.DataFrame({
            "att1": [1, 2, 3],
            "att2": [11, 12, 13],
        }, index=["ac1", "ac2", "ac3"]
    )

    a.update(dynamically_created)

    assert a.ids.tolist() == ["ac1", "ac2", "ac3"]
    assert a.get_attribute_values("att1", ["ac1", "ac2", "ac3"]).tolist() == [1, 2, 3]
    assert a.get_attribute_values("att2", ["ac1", "ac2", "ac3"]).tolist() == [11, 12, 13]


def test_io_round_trip():

    with path.tempdir() as p:

        actor_path = os.path.join(p, "test_location")
        dummy_actor.save_to(actor_path)
        retrieved = Actor.load_from(circus=None, actor_dir=actor_path)

        assert dummy_actor.size == retrieved.size
        assert dummy_actor.ids.tolist() == retrieved.ids.tolist()

        ids = dummy_actor.ids.tolist()

        for att_name in dummy_actor.attribute_names():
            assert dummy_actor.get_attribute_values(att_name, ids).equals(
                retrieved.get_attribute_values(att_name, ids)
            )

        for rel_name in dummy_actor.relationship_names():
            assert dummy_actor.get_relationship(rel_name)._table.equals(
                retrieved.get_relationship(rel_name)._table
            )
