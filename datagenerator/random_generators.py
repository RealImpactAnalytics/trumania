import numpy as np
from util_functions import *
from abc import ABCMeta, abstractmethod
from datagenerator.operations import *


class Parameterizable(object):
    """
    Mixin providing the ability to contain and update parameters.
    """

    def __init__(self, parameters):
        self.parameters = parameters

    def update_parameters(self, **kwargs):
        # TODO: ultimately, this can evolve into an Action operation => the
        # random generations characteristics evole over time

        # TODO2: cf discussion from Gautier: those parameters could become
        # columns of parameters => 1 value per actor
        self.parameters.update(kwargs)


class Generator(Parameterizable):
    """
    Independent parameterized random value generator.
    Abstract class
    """
    __metaclass__ = ABCMeta

    def __init__(self, name, parameters):
        Parameterizable.__init__(self, parameters)
        self.name = name
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

    class GeneratorOps(object):
        def __init__(self, generator):
            self.generator = generator

        class RandomValues(AddColumns):
            """
            Operation that produces one single column generated randomly.
            """

            def __init__(self, generator, named_as):
                AddColumns.__init__(self)
                self.generator = generator
                self.named_as = named_as

            def build_output(self, data):
                values = self.generator.generate(size=data.shape[0])
                return pd.DataFrame({self.named_as: values}, index=data.index)

        def generate(self, named_as):
            return self.RandomValues(self.generator, named_as=named_as)


class ConstantGenerator(Generator):
    def __init__(self, name, value):
        Generator.__init__(self, name, {})
        self.value = value

    def generate(self, size):
        return [self.value] * size


class NumpyRandomGenerator(Generator):
    """
        Generator wrapping any numpy.Random method.
    """

    def __init__(self, name, method, seed=None, **numpy_parameters):
        """Initialise a random number generator

        :param name: string, the name (is this useful?)
        :param method: string: must be a valid numpy.Randomstate method that
            accept the "size" parameter

        :param numpy_parameters: dict, see descriptions below
        :param seed: int, seed of the generator
        :return: create a random number generator of type "gen_type", with its parameters and seeded.
        """
        Generator.__init__(self, name, numpy_parameters)
        self.numpy_method = getattr(RandomState(seed), method)

    def generate(self, size):
        all_params = merge_2_dicts({"size": size}, self.parameters)
        return self.numpy_method(**all_params)


class ScaledParetoGenerator(Generator):
    def __init__(self, name, m, seed=None, **numpy_parameters):
        Generator.__init__(self, name, numpy_parameters)

        # TODO: bug here (and elsewhere: we're actually not being impacted by
        # the "update parameter" call ^^
        self.stock_pareto = NumpyRandomGenerator(name="p",
                                                 method="pareto",
                                                 seed=seed,
                                                 **numpy_parameters)
        self.m = m

    def generate(self, size):
        stock_obs = self.stock_pareto.generate(size)
        return (stock_obs + 1) * self.m


class MSISDNGenerator(Generator):
    """

    """

    def __init__(self, name, countrycode, prefix_list, length, seed=None):
        """

        :param name: string
        :param countrycode: string
        :param prefix_list: list of strings
        :param length: int
        :param seed: int
        :return:
        """
        Generator.__init__(self, name, {})
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
        generator = NumpyRandomGenerator(name="inner", method="choice",
                                         seed=self.seed, a=available_idx,
                                         replace=False)

        generated_entries = generator.generate(size)
        msisdns = np.array(
            [self.__cc + self.__pref[self.__available[i, 1]] + str(self.__available[i, 0]).zfill(self.__length)
             for i in generated_entries])

        self.__available = np.delete(self.__available, generated_entries, axis=0)

        return msisdns


class DependentGenerator(Parameterizable):
    """
        Generator sampling values from a random variable that depends on
        previous observations.
    """

    # TODO: observations is limited to one single column ("weights")

    __metaclass__ = ABCMeta

    def __init__(self, name, parameters):
        Parameterizable.__init__(self, parameters)
        self.name = name
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

        class RandomValues(AddColumns):
            """
            Operation that produces one single column generated randomly.
            """

            def __init__(self, generator, named_as, observations_field):
                AddColumns.__init__(self)
                self.generator = generator
                self.named_as = named_as
                self.observations_field = observations_field

            def build_output(self, data):
                obs = data[self.observations_field]
                values = self.generator.generate(observations=obs)
                return pd.DataFrame({self.named_as: values}, index=data.index)

        def generate(self, named_as, observations_field):
            return self.RandomValues(self.generator, named_as, observations_field)


class TriggerGenerator(DependentGenerator):
    """
    A trigger generator takes some observed values and returns a vector of
    Booleans, depending if the trigger has been released or not
    """

    def __init__(self, name, trigger_type, seed=None):

        # random baseline to compare to each the activation
        self.base_line = NumpyRandomGenerator(name="inner", method="uniform",
                                              seed=seed)

        if trigger_type == "logistic":
            def logistic(x, a, b):
                """returns the value of the logistic function 1/(1+e^-(ax+b))
                """
                the_exp = np.minimum(-(a*x+b),10.)
                return 1./(1.+np.exp(the_exp))
            self.triggering_function = logistic

            # TODO: we could allow the API to provide those parameters
            DependentGenerator.__init__(self, name, {"a": -0.01, "b": 10.})
            self.triggering_function = logistic

        else:
            raise NotImplemented("unknown trigger type: {}".format(trigger_type))

    def generate(self, observations):
        probs = self.base_line.generate(size=observations.shape[0])

        params = merge_2_dicts({"x": observations}, self.parameters)
        trigger = self.triggering_function(**params)

        # BUG here? ideally we'd trigger the high probabilities => should we
        # "flip" the sigmoid ?
        return probs < trigger
