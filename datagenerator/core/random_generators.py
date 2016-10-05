from itertools import islice
from faker import Faker

from datagenerator.core.operations import *


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

    def flatmap(self, dependent_generator):
        """
        chains self with this other generator by feeding "our" output values as
        observations to the dependent_generator

        :param dependent_generator: must be an instance of DependentGenerator,
         i.e. have a .generate(observations=...) method

        :return: an instance of Generator whose .generate(size=...) method
            provides the combination of the above

        """
        return TransformedGenerator(upstream_gen=self,
                                    vect_f=dependent_generator.generate)

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

            def build_output(self, action_data):

                # if quantity_field is not specified, we assume 1 and return
                # the "bare" result (i.e. not in a list of 1 element)
                if self.quantity_field is None:
                    values = self.generator.generate(size=action_data.shape[0])

                # otherwise, provides a columns with list of generated values
                else:
                    qties = action_data[self.quantity_field]

                    # slices groups of generated values of appropriate size
                    flat_vals = iter(self.generator.generate(size=qties.sum()))
                    values = [list(islice(flat_vals, size)) for size in qties]

                return pd.DataFrame({self.named_as: values},
                                     index=action_data.index)

        def generate(self, named_as, quantity_field=None):
            return self.RandomValues(self.generator, named_as=named_as,
                                     quantity_field=quantity_field)


class ConstantGenerator(Generator):
    def __init__(self, value):
        Generator.__init__(self)
        self.value = value

    def generate(self, size):
        return [self.value] * size


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
        self.numpy_parameters = numpy_parameters
        self.numpy_method = getattr(RandomState(seed), method)

    def generate(self, size):
        all_params = merge_2_dicts({"size": size}, self.numpy_parameters)
        return self.numpy_method(**all_params)


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
        self.counter=start
        self.prefix = prefix
        self.max_length = max_length

    def generate(self, size):
        values = build_ids(size, self.counter, self.prefix, self.max_length)
        self.counter += size
        return values


class FakerGenerator(Generator):
    """
    Generator wrapping Faker factory
    """

    def __init__(self, seed, method):
        Generator.__init__(self)
        fake = Faker()
        fake.seed(seed)

        self.method = getattr(fake, method)

    def generate(self, size):
        return [self.method() for _ in range(size)]


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


class TransformedGenerator(Generator):
    # TODO: this could become a .map() operation on the Generator, just beside
    # the existing flatmap()
    """
    Generators which maps the output of another generator with a
    deterministic function.

    I.e., if the upstream generator provides samples from X, this provides
    samples from f(X).

    TODO: refactor: ParetoGenerator is now a specific case of this class
    """

    def __init__(self, upstream_gen, f=None, vect_f=None):
        """
        :param upstream_gen: upstream generator
        :param f: item per item transformation function
        :param vect_f: "vectorialized" transformation function, i.e. able to
         transform everything in one go
        """

        assert (f is not None) ^ (vect_f is not None)

        Generator.__init__(self)
        self.upstream_gen = upstream_gen
        self.f = f
        self.vect_f = vect_f

    def generate(self, size):
        samples = self.upstream_gen.generate(size=size)

        if self.vect_f is not None:
            return self.vect_f(samples)

        elif self.f is not None:
            return [self.f(sample) for sample in samples]


class BoundedGenerator(TransformedGenerator):
    """
    TransformedGenerator that bounds the generated values to a min and a max
    value.

    TODO: this simplistic implementation is just replacing out of bound
    values with the bounds themselves. We could re-draw them instead  (though
    we'd then need to prevent infinite recursions somehow...)
    """

    def __init__(self, upstream_gen, lb=None, ub=None):

        def bound_value(value):
            bounded = max(lb, value)
            if ub is not None:
                bounded = min(ub, bounded)
            return bounded

        TransformedGenerator.__init__(self, upstream_gen, bound_value)


class DependentGenerator(object):
    """
    Generator providing random values depending on some live observation
    among the fields of the action or attributes of the actors.

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
        upstream operation in the Action or upstream random variables in this
        graph.

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

            def build_output(self, action_data):
                # observing either a field or an attribute
                obs = action_data[self.observations_field]
                values = self.generator.generate(observations=obs).values
                return pd.DataFrame({self.named_as: values},
                                    index=action_data.index)

        def generate(self, named_as, observed_field):
            """
            :param named_as: the name of the supplementary field inserted in
              the action_data
            :param observed_field: the name of the action_data field whose
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

    A dependent trigger transforms, with the specified function, the value
    of the depended on action field or actor attribute into the [0, 1] range
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

        return [f(observation) for observation in observations]

