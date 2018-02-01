from itertools import islice
from faker import Faker
from bson.objectid import ObjectId
import json
import pandas as pd
import logging
from abc import ABCMeta, abstractmethod
import numpy as np
from numpy.random import RandomState

from trumania.core.operations import AddColumns, identity
from trumania.core.util_functions import merge_2_dicts, build_ids


def seed_provider(master_seed):
    """
    :param master_seed: master seed
    :return: a generator of seeds, deterministically depending on the master one
    """
    state = RandomState(master_seed)
    max_int_32 = 2**31 - 1
    while True:
        yield state.randint(1, max_int_32)


class Generator(object):
    """
    Independent parameterized random value generator.
    Abstract class
    """
    __metaclass__ = ABCMeta

    file_loaders = {}

    def __init__(self):
        self.ops = self.GeneratorOps(self)

    @abstractmethod
    def generate(self, size):
        """
        "Independent" random value generation: do not depend on any previous
        observation, we just want to sample the random variable `size` times

        :param size: the number of random value to produce
        :return: an array of generated random values
        """
        pass

    def map(self, f=None, f_vect=None):
        """
        Creates a new generator that transforms the generated values with the
        provided function.

        # TODO: do we really need "non vectorialized" f and vectorialized
        f_vect? Have a look at numpy ufunc...
        """

        assert (f is not None) ^ (f_vect is not None)
        parent = self

        class Transformed(Generator):
            def generate(self, size):

                samples = parent.generate(size=size)

                if f_vect is not None:
                    # makes sure the result type is array and not a Series
                    # (which may cause index mis-alignments)
                    return [i for i in f_vect(samples)]

                elif f is not None:
                    return [f(sample) for sample in samples]

        return Transformed()

    def flatmap(self, dependent_generator):
        """
        Not _really_ a flatmap but close enough on concept (I guess): this
        chains self with a DependentGenerator by feeding our output
        values as observations to the DependentGenerator at the moment of
        generation.

        :param dependent_generator: must be an instance of DependentGenerator,
         i.e. have a .generate(observations=...) method

        :return: an instance of Generator whose .generate(size=...) method
            provides the combination of the above

        """
        return self.map(f_vect=dependent_generator.generate)

    def save_to(self, output_file):
        raise NotImplemented("must be implemented in sub-class")

    def description(self):
        return {}

    @staticmethod
    def load_generator(gen_type, input_file):
        if gen_type in Generator.file_loaders:
            return Generator.file_loaders[gen_type](input_file)
        else:
            raise ValueError("does not know how to parse generator of type "
                             "{}".format(gen_type))

    class GeneratorOps(object):
        def __init__(self, generator):
            self.generator = generator

        class RandomValues(AddColumns):
            """
            Operation that produces one single column generated randomly.
            """

            def __init__(self, generator, named_as, quantity_field):
                AddColumns.__init__(self)
                self.generator = generator
                self.named_as = named_as
                self.quantity_field = quantity_field

            def build_output(self, story_data):

                # if quantity_field is not specified, we assume 1 and return
                # the "bare" result (i.e. not in a list of 1 element)
                if self.quantity_field is None:
                    values = self.generator.generate(size=story_data.shape[0])

                # otherwise, provides a columns with list of generated values
                else:
                    qties = story_data[self.quantity_field]

                    # slices groups of generated values of appropriate size
                    flat_vals = iter(self.generator.generate(size=qties.sum()))
                    values = [list(islice(flat_vals, size)) for size in qties]

                return pd.DataFrame({self.named_as: values},
                                    index=story_data.index)

        def generate(self, named_as, quantity_field=None):
            return self.RandomValues(self.generator, named_as=named_as,
                                     quantity_field=quantity_field)


class ConstantGenerator(Generator):
    def __init__(self, value):
        Generator.__init__(self)
        self.value = value

    def generate(self, size):
        return [self.value] * size


class FixedValuesGenerator(Generator):
    def __init__(self, values):
        Generator.__init__(self)
        self.values = values

    def generate(self, size):
        assert len(self.values) == size
        return self.values


class NumpyRandomGenerator(Generator):
    """
        Generator wrapping any numpy.Random method.
    """

    def __init__(self, method, seed, **numpy_parameters):
        """Initialise a random number generator

        :param method: string: must be a valid numpy.Randomstate method that
            accept the "size" parameter

        :param numpy_parameters: dict, see descriptions below
        :param seed: int, seed of the generator
        :return: create a random number generator of type "gen_type", with its parameters and seeded.
        """
        Generator.__init__(self)
        self.method = method
        self.numpy_parameters = numpy_parameters
        self.state = RandomState(seed)
        self.numpy_method = getattr(self.state, method)

    def generate(self, size):
        all_params = merge_2_dicts({"size": size}, self.numpy_parameters)
        return self.numpy_method(**all_params)

    def description(self):
        return {
            "type": "NumpyRandomGenerator",
            "method": self.method,
            "numpy_parameters": self.numpy_parameters
        }

    def save_to(self, output_file):

        logging.info("saving generator to {}".format(output_file))

        # saving the numpy RandomState instance, converting the numpy array
        # to enable json serialization
        np_state = self.state.get_state()
        state = {
            "method": self.method,
            "numpy_parameters": self.numpy_parameters,
            "numpy_state": (np_state[0], np_state[1].tolist(), np_state[2],
                            np_state[3], np_state[4])
        }
        with open(output_file, "w") as outf:
            json.dump(state, outf, indent=4)

    @staticmethod
    def load_from(input_file):

        logging.info("loading numpy generator from {}".format(input_file))

        with open(input_file, "r") as inf:
            json_payload = json.load(inf)

            # Initializing the generator with an incorrect seed just to make
            # the constructor happy, then setting the state
            gen = NumpyRandomGenerator(
                method=json_payload["method"],
                seed=1234,
                **json_payload["numpy_parameters"])

            # retrieving the numpy state + converting list to np.array as needed
            state_raw_ = json_payload["numpy_state"]
            np_state = (state_raw_[0], np.array(state_raw_[1]), state_raw_[2],
                        state_raw_[3], state_raw_[4])

            gen.state = np.random.RandomState(seed=1234)
            gen.state.set_state(np_state)
            return gen


Generator.file_loaders["NumpyRandomGenerator"] = NumpyRandomGenerator.load_from


class ParetoGenerator(Generator):
    """
    Builds a pareto having xmin as lower bound for the sampled values and a
     as power parameter, i.e.:

     p(x|a) = (x/xmin)^a  if x >= xmin
            = 0           otherwise

     The higher the value of a, the closer pareto gets to dirac's delta.

    force_int allows to round each value to integers (handy to generate
     counts distributed as a power law)
    """
    def __init__(self, xmin, seed=None, force_int=False, **np_params):
        Generator.__init__(self)

        self.force_int = force_int
        self.xmin = xmin
        self.lomax = NumpyRandomGenerator(method="pareto", seed=seed,
                                          **np_params)

    def generate(self, size):
        values = (self.lomax.generate(size) + 1) * self.xmin

        if self.force_int:
            values = [int(v) for v in values]

        return values


class SequencialGenerator(Generator):
    """
    Generator of sequencial unique values
    """
    def __init__(self, start=0, prefix="id_", max_length=10):
        Generator.__init__(self)
        self.counter = int(start)
        self.prefix = prefix
        self.max_length = max_length

    def generate(self, size):
        # forcing size as int, also making sure we never get floating point
        # values in ids (can happen if size results from some scaling)
        size_i = int(size)
#        size_i = size
        values = build_ids(size_i, self.counter, self.prefix, self.max_length)
        self.counter += size_i
        return values

    def description(self):
        return {
            "type": "SequencialGenerator",
            "prefix": self.prefix,
            "max_length": self.max_length
        }

    def save_to(self, output_file):

        logging.info("saving sequencial generator to {}".format(output_file))

        state = {
            "counter": int(self.counter),
            "prefix": self.prefix,
            "max_length": self.max_length
        }
        with open(output_file, "w") as outf:
            json.dump(state, outf, indent=4)

    @staticmethod
    def load_from(input_file):

        logging.info("loading generator from {}".format(input_file))

        with open(input_file, "r") as inf:
            state = json.load(inf)

            return SequencialGenerator(
                start=state["counter"],
                prefix=state["prefix"],
                max_length=state["max_length"])


Generator.file_loaders["SequencialGenerator"] = SequencialGenerator.load_from


class FakerGenerator(Generator):
    """
    Generator wrapping Faker factory
    """

    def __init__(self, seed, method, **fakerKwargs):
        Generator.__init__(self)
        fake = Faker()
        fake.seed(seed)

        self.method = getattr(fake, method)
        self.fakerKwargs = fakerKwargs

    def generate(self, size):
        return [self.method(**self.fakerKwargs) for _ in range(size)]


class MSISDNGenerator(Generator):
    """

    """

    def __init__(self, countrycode, prefix_list, length, seed=None):
        """

        :param name: string
        :param countrycode: string
        :param prefix_list: list of strings
        :param length: int
        :param seed: int
        :return:
        """
        Generator.__init__(self)
        self.__cc = countrycode
        self.__pref = prefix_list
        self.__length = length
        self.seed = seed

        maxnumber = 10 ** length - 1
        self.__available = np.empty([maxnumber * len(prefix_list), 2],
                                    dtype=int)
        for i in range(len(prefix_list)):
            self.__available[i * maxnumber:(i + 1) * maxnumber, 0] = np.arange(0, maxnumber, dtype=int)
            self.__available[i * maxnumber:(i + 1) * maxnumber, 1] = i

    def generate(self, size):
        """returns a list of size randomly generated msisdns.
        Those msisdns cannot be generated again from this generator

        :param size: int
        :return: numpy array
        """

        available_idx = np.arange(0, self.__available.shape[0], dtype=int)
        generator = NumpyRandomGenerator(
            method="choice", a=available_idx, replace=False, seed=self.seed)

        generated_entries = generator.generate(size)
        msisdns = np.array(
            [self.__cc + self.__pref[self.__available[i, 1]] +
                str(self.__available[i, 0]).zfill(self.__length)
             for i in generated_entries])

        self.__available = np.delete(self.__available, generated_entries,
                                     axis=0)

        return msisdns


class MongoIdGenerator(Generator):
    """
    Generates a random ObjectId for MongoDB, from bson.objectid.ObjectID,
    See http://api.mongodb.com/python/current/api/bson/objectid.html
    """

    def generate(self, size):
        """returns a list of generated ObjectIds for a MongoDB.
        Those ObjectIds cannot be generated again from this generator

        :param size: int
        :return: array of strings
        """

        return [ObjectId().__str__() for i in range(size)]


class DependentGenerator(object):
    """
    Generator providing random values depending on some live observation
    among the fields of the story or attributes of the populations.

    This opens the door to "probability given" distributions
    """

    # TODO: observations is limited to one single column ("weights")

    __metaclass__ = ABCMeta

    def __init__(self):
        self.ops = self.DependentGeneratorOps(self)

    @abstractmethod
    def generate(self, observations):
        """
        Generation of random values after observing the input events.

        :param observations: one list of "previous observations", coming from
        upstream operation in the Story upstream random variables in
        this graph.

        :return: an array of generated random values
        """

        pass

    class DependentGeneratorOps(object):
        def __init__(self, generator):
            self.generator = generator

        class RandomValuesFromField(AddColumns):
            """
            Operation that produces one single column generated randomly.
            """

            def __init__(self, generator, named_as, observations_field):
                AddColumns.__init__(self)

                self.generator = generator
                self.named_as = named_as
                self.observations_field = observations_field

            def build_output(self, story_data):
                # observing either a field or an attribute
                obs = story_data[self.observations_field]
                values = self.generator.generate(observations=obs).values
                return pd.DataFrame({self.named_as: values},
                                    index=story_data.index)

        def generate(self, named_as, observed_field):
            """
            :param named_as: the name of the supplementary field inserted in
              the story_data
            :param observed_field: the name of the story_data field whose
              content is used as observed input by this DependentGenerator
            :return:
            """
            return self.RandomValuesFromField(self.generator, named_as,
                                              observed_field)


class ConstantDependentGenerator(ConstantGenerator, DependentGenerator):
    """
    Dependent generator ignoring the observations and producing a constant
    value.
    """

    def __init__(self, value):
        ConstantGenerator.__init__(self, value=value)
        DependentGenerator.__init__(self)

    def generate(self, observations):
        vals = ConstantGenerator.generate(self, size=len(observations))
        return pd.Series(vals, index=observations.index)


class DependentTrigger(object):
    """
    A trigger is a boolean Generator.

    A dependent trigger transforms, with the specified function, the value of
    the depended on story field or population attribute into the [0,1] range
    and uses that as the probability of triggering (i.e. of returning True)
    """

    def __init__(self, value_to_proba_mapper=identity, seed=None):

        # random baseline to compare to each the activation
        self.base_line = NumpyRandomGenerator(method="uniform",
                                              low=0.0, high=1.0,
                                              seed=seed)
        self.value_to_proba_mapper = value_to_proba_mapper

    def generate(self, observations):
        draws = self.base_line.generate(size=observations.shape[0])
        triggers_proba = self.value_to_proba_mapper(observations)

        return draws < triggers_proba


class DependentTriggerGenerator(DependentTrigger, DependentGenerator):
    """
    Composition of the two mixin above:
        DependentGenerator: , with the ability to build operation that generate
         random values
        DependentTrigger: to specify that the the generation actually
        produces booleans with a value_mapper
    """
    def __init__(self, value_to_proba_mapper=identity, seed=None):
        DependentTrigger.__init__(self, value_to_proba_mapper, seed)
        DependentGenerator.__init__(self)


class DependentBulkGenerator(DependentGenerator):
    """
    Dependent Generator that transforms that observations into a list of
    observation elements that are generated through element_generator.
    """
    def __init__(self, element_generator):
        DependentGenerator.__init__(self)
        self.element_generator = element_generator

    def generate(self, observations):

        def f(bulk_size):
            return self.element_generator.generate(bulk_size)

        return pd.Series([f(observation) for observation in observations])
