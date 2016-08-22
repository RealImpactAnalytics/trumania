from datagenerator.random_generators import *
from itertools import *


def test_constant_generator_should_produce_constant_values():
    tested = ConstantGenerator(value="c")

    assert [] == tested.generate(size=0)
    assert ["c"] == tested.generate(size=1)
    assert ["c", "c", "c", "c", "c"] == tested.generate(size=5)


def test_numpy_random_generator_should_delegate_to_numpy_correctly():

    # basic "smoke" test, if it does not crash it at least proves it's able
    # to load the appropriate method
    tested = NumpyRandomGenerator(method="normal", loc=10, scale=4)
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
        return [1,1,0,0,1,0]

    g = DependentTriggerGenerator(value_mapper=fake_mapper)

    triggers = g.generate(observations=pd.Series([10, 20, 30, 0, 1, 2]))

    # because the fake_mapper returns fake values, we should always have the
    # following triggers, no matter what the internal uniform distro provided
    assert triggers.tolist() == [True, True, False, False, True, False]
