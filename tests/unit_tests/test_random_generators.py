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
