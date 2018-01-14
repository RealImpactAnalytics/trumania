import os
import path
import functools
from itertools import islice
import pandas as pd
import numpy as np

from trumania.core.random_generators import SequencialGenerator, NumpyRandomGenerator, ConstantGenerator, seed_provider
from trumania.core.random_generators import DependentTriggerGenerator, FakerGenerator, Generator


def test_constant_generator_should_produce_constant_values():
    tested = ConstantGenerator(value="c")

    assert [] == tested.generate(size=0)
    assert ["c"] == tested.generate(size=1)
    assert ["c", "c", "c", "c", "c"] == tested.generate(size=5)


def test_numpy_random_generator_should_delegate_to_numpy_correctly():

    # basic "smoke" test, if it does not crash it at least proves it's able
    # to load the appropriate method
    tested = NumpyRandomGenerator(method="normal", loc=10, scale=4, seed=1)
    assert len(tested.generate(size=10)) == 10


def test_seeder_should_be_deterministic():
    """
    makes sure the seeds always provides the same sequence of seeds
    """

    master_seed = 12345

    seeder1 = seed_provider(master_seed)
    seeder2 = seed_provider(master_seed)

    assert list(islice(seeder1, 1000)) == list(islice(seeder2, 1000))


def test_depend_trigger_should_trigger_given_constant_value():

    # returns 6 hard-coded 1 and zero
    def fake_mapper(x):
        return [1, 1, 0, 0, 1, 0]

    g = DependentTriggerGenerator(value_to_proba_mapper=fake_mapper)

    triggers = g.generate(observations=pd.Series([10, 20, 30, 0, 1, 2]))

    # because the fake_mapper returns fake values, we should always have the
    # following triggers, no matter what the internal uniform distro provided
    assert triggers.tolist() == [True, True, False, False, True, False]


def test_sequencial_generator_should_create_unique_values():

    tested = SequencialGenerator(start=10, prefix="test_p_", max_length=10)

    sizes = [100, 200, 300, 400, 500]
    sets = [set(tested.generate(size)) for size in sizes]

    # generated values should be unique within each set
    all_values = functools.reduce(lambda s1, s2: s1 | s2, sets)

    assert len(all_values) == np.sum(sizes)


def test_random_generator_should_provide_correct_amount_of_single_values():

    tested = NumpyRandomGenerator(method="gamma", scale=10, shape=1.8, seed=1)

    genops = tested.ops.generate(named_as="rand")

    story_data = pd.DataFrame(
        np.random.rand(10, 5), columns=["A", "B", "C", "D", "E"])

    result, logs = genops(story_data)

    assert result.columns.tolist() == ["A", "B", "C", "D", "E", "rand"]

    # should be float and not list of values
    assert result["rand"].dtype == float


def test_random_generator_should_provide_correct_amount_of_list_of_values():

    tested = NumpyRandomGenerator(method="gamma", scale=10, shape=1.8, seed=1)

    story_data = pd.DataFrame(
        np.random.rand(10, 5), columns=["A", "B", "C", "D", "E"],
    )
    story_data["how_many"] = pd.Series([10, 20, 30, 40, 50, 60, 70, 80, 90, 100])

    genops = tested.ops.generate(named_as="rand", quantity_field="how_many")

    result, logs = genops(story_data)

    assert result.columns.tolist() == ["A", "B", "C", "D", "E", "how_many", "rand"]

    # should be list of the expected sizes
    assert result["rand"].dtype == list
    assert result["rand"].apply(len).tolist() == [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]


def test_faker_generator_should_delegate_to_faker_correct():

    tested_name = FakerGenerator(seed=1234, method="name")
    some_names = tested_name.generate(10)
    assert len(some_names) == 10

    tested_text = FakerGenerator(seed=1234, method="text")
    some_text = tested_text.generate(20)
    assert len(some_text) == 20

    tested_address = FakerGenerator(seed=1234, method="address")
    some_addresses = tested_address.generate(30)
    assert len(some_addresses) == 30


def test_sequencial_generator_read_from_disk_should_continue_sequence():

    with path.tempdir() as p:

        tested = SequencialGenerator(start=10, prefix="o_", max_length=2)

        list_1 = tested.generate(size=4)
        assert list_1 == ["o_10", "o_11", "o_12", "o_13"]

        gen_file = os.path.join(p, "tested.json")
        tested.save_to(gen_file)

        tested2 = Generator.load_generator(gen_type="SequencialGenerator",
                                           input_file=gen_file)

        list_2 = tested2.generate(size=4)
        assert list_2 == ["o_14", "o_15", "o_16", "o_17"]

        # loading it again => we should have the same result
        tested3 = Generator.load_generator(gen_type="SequencialGenerator",
                                           input_file=gen_file)

        list_3 = tested3.generate(size=4)
        assert list_3 == ["o_14", "o_15", "o_16", "o_17"]


def numpy_generators_read_from_disk_should_generate_same_sequence_as_original():

    with path.tempdir() as p:

        # making sure we're not using the default seed
        tested = NumpyRandomGenerator(method="normal", loc=10, scale=4,
                                      seed=123456)

        gen_file = os.path.join(p, "tested2.json")
        tested.save_to(gen_file)

        reloaded = Generator.load_generator(gen_type="NumpyRandomGenerator",
                                            input_file=gen_file)

        assert tested.generate(size=10000) == reloaded.generate(size=10000)


def test_sequencial_generator_must_not_change_format_when_size_is_float():

    seq = SequencialGenerator(prefix="sq", max_length=2)

    # bugfix: this was previously generating "sq00.0", "sq01.0",...
    assert ["sq00", "sq01", "sq02"] == seq.generate(size=3.3)
    assert ["sq03", "sq04", "sq05"] == seq.generate(size=3.3)
