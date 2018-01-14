import path
import pandas as pd
import os

from trumania.core.random_generators import SequencialGenerator
from trumania.core.circus import Circus
from trumania.core.population import Population, Attribute

tc = Circus("c", master_seed=1234, start=pd.Timestamp("1 Jan 2011"),
            step_duration=pd.Timedelta("1h"))


def test_set_and_read_values_in_attribute_should_be_equal():

    population = Population(circus=None, size=5,
                            ids_gen=SequencialGenerator(prefix="abc", max_length=1))

    tested = Attribute(population, init_values=[10, 20, 30, 40, 50])

    assert tested.get_values(["abc0"]).tolist() == [10]
    assert tested.get_values(["abc0", "abc3", "abc1"]).tolist() == [10, 40, 20]

    # getting no id should return empty list
    assert tested.get_values([]).tolist() == []


def test_updated_and_read_values_in_attribute_should_be_equal():
    population = Population(circus=tc, size=5, ids_gen=SequencialGenerator(
        prefix="abc", max_length=1))
    tested = Attribute(population, init_values=[10, 20, 30, 40, 50])

    tested.update(pd.Series([22, 44], index=["abc1", "abc3"]))

    # value of a should untouched
    assert tested.get_values(["abc0"]).tolist() == [10]

    # arbitrary order should not be impacted
    assert tested.get_values(["abc0", "abc3", "abc1"]).tolist() == [10, 44, 22]


def test_updating_non_existing_population_ids_should_add_them():
    population = Population(circus=tc, size=5, ids_gen=SequencialGenerator(
        prefix="abc", max_length=1))
    tested = Attribute(population, init_values=[10, 20, 30, 40, 50])

    tested.update(pd.Series([22, 1000, 44], index=["abc1", "not_yet_there", "abc3"]))

    assert tested.get_values(["not_yet_there", "abc0", "abc3", "abc4"]).tolist() == [1000, 10, 44, 50]


def test_initializing_attribute_from_relationship_must_have_a_value_for_all():

    population = Population(circus=tc, size=5, ids_gen=SequencialGenerator(
        prefix="abc", max_length=1))
    oneto1 = population.create_relationship("rel")
    oneto1.add_relations(from_ids=["abc0", "abc1", "abc2", "abc3", "abc4"],
                         to_ids=["ta", "tb", "tc", "td", "te"])

    attr = Attribute(population, init_relationship="rel")

    expected = pd.DataFrame({"value": ["ta", "tb", "tc", "td", "te"]},
                            index=["abc0", "abc1", "abc2", "abc3", "abc4"])

    assert attr._table.sort_index().equals(expected)


def test_overwrite_attribute():

    population = Population(circus=tc, size=10,
                            ids_gen=SequencialGenerator(prefix="u_", max_length=1))

    ages = [10, 20, 40, 10, 100, 98, 12, 39, 76, 23]
    age_attr = population.create_attribute("age", init_values=ages)

    # before modification
    ages = age_attr.get_values(["u_0", "u_4", "u_9"]).tolist()
    assert ages == [10, 100, 23]

    story_data = pd.DataFrame({
        # id of the populations to update
        "A_ID": ["u_4", "u_0"],

        # new values to copy
        "new_ages": [34, 30]},

        # index of the story data has, in general, nothing to do with the
        # updated population
        index=["cust_1", "cust_2"]
    )

    update = age_attr.ops.update(
        member_id_field="A_ID",
        copy_from_field="new_ages"
    )

    _, logs = update(story_data)

    assert logs == {}
    # before modification
    ages = age_attr.get_values(["u_0", "u_4", "u_9"]).tolist()
    assert ages == [30, 34, 23]


def test_added_and_read_values_in_attribute_should_be_equal():
    population = Population(circus=tc, size=5,
                            ids_gen=SequencialGenerator(prefix="abc", max_length=1))
    tested = Attribute(population, init_values=[10, 20, 30, 40, 50])

    tested.add(["abc1", "abc3"], [22, 44])

    assert tested.get_values(["abc0", "abc1", "abc2", "abc3", "abc4"]).tolist() == [10, 20 + 22, 30, 40 + 44, 50]


def test_adding_several_times_to_the_same_from_should_pile_up():
    population = Population(circus=tc, size=5,
                            ids_gen=SequencialGenerator(prefix="abc", max_length=1))
    tested = Attribute(population, init_values=[10, 20, 30, 40, 50])

    tested.add(["abc1", "abc3", "abc1"], [22, 44, 10])

    assert tested.get_values(["abc0", "abc1", "abc2", "abc3", "abc4"]).tolist() == [10, 20 + 22 + 10, 30, 40 + 44, 50]


def test_io_round_trip():

    with path.tempdir() as root_dir:

        population = Population(circus=tc, size=5,
                                ids_gen=SequencialGenerator(prefix="abc", max_length=1))
        orig = Attribute(population, init_values=[10, 20, 30, 40, 50])

        full_path = os.path.join(root_dir, "attribute.csv")

        orig.save_to(full_path)
        retrieved = Attribute.load_from(full_path)

        assert orig._table.equals(retrieved._table)
