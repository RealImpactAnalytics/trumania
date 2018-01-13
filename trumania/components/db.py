"""
This is just the provider of the IO methods save and retrieve various
simulation components to/from persistence.

A namespace defines a place where to put objects that belong together
(typically, from the same scenario or component, e.g. "Uganda").

"""

# TODO: we should store this elsewhere than in the git repo...

# TODO: would be cool to also be able to store empirical probability
# distribution here, for the random generators...

import pandas as pd
import os

from trumania.core.util_functions import ensure_folder_exists, ensure_non_existing_dir
from trumania.core.population import Population
import trumania.core.clock as clock
from trumania.core.random_generators import Generator, NumpyRandomGenerator


def save_population(population, namespace, population_id):
    population.save_to(population_folder(namespace, population_id))


def load_population(namespace, population_id, circus):
    return Population.load_from(population_folder(namespace, population_id), circus)


def list_populations(namespace):
    folder = _population_folder(namespace)
    return [d for d in os.listdir(folder)
            if os.path.isdir(os.path.join(folder, d))]


def save_generator(generator, namespace, gen_id):

    output_folder = _gen_folder(namespace=namespace,
                                gen_type=generator.__class__.__name__,)

    ensure_folder_exists(output_folder)
    generator.save_to(json_item_path(output_folder, gen_id))


def list_generators(namespace):
    folder = _generators_folder(namespace)
    if not os.path.exists(folder):
        return []

    def _list():
        for gen_type in os.listdir(folder):
            for gen_file in os.listdir(os.path.join(folder, gen_type)):
                gen_id = gen_file.split(".")[0]
                yield [gen_type, gen_id]

    return list(_list())


def load_generator(namespace, gen_type, gen_id):

    input_file = json_item_path(
        _gen_folder(namespace=namespace, gen_type=gen_type), gen_id)

    return Generator.load_generator(gen_type, input_file)


# TODO: this can now be refactored to save as NumpyGenerator, togheter with
# its state
def save_timer_gen(timer_gen, namespace, timer_gen_id):

    timer_gen_folder = _timer_gens_root_folder(namespace)
    ensure_folder_exists(timer_gen_folder)
    timer_gen.save_to(csv_item_path(timer_gen_folder, timer_gen_id))


def load_timer_gen_config(namespace, timer_gen_id):
    timer_gen_folder = _timer_gens_root_folder(namespace)

    return clock.CyclicTimerProfile.load_from(
        csv_item_path(timer_gen_folder, timer_gen_id))


def save_empirical_discrete_generator(distribution, values, namespace, gen_id):
    assert distribution.sum() - 1 < 1e-6

    root_folder = _empirical_discrete_gen_folder(namespace)
    ensure_folder_exists(root_folder)
    gen_file_path = csv_item_path(root_folder, gen_id)

    df = pd.DataFrame({
        "px": distribution,
      }, index=pd.Series(values, name="x"))

    df.to_csv(gen_file_path, index=True)


def load_empirical_discrete_generator(namespace, gen_id, seed):
    root_folder = _empirical_discrete_gen_folder(namespace)
    gen_file_path = os.path.join(root_folder, "%s.csv" % gen_id)
    df = pd.read_csv(gen_file_path)

    gen = NumpyRandomGenerator(
        method="choice",
        a=df["x"].tolist(),
        p=df["px"].tolist(),
        seed=seed)

    return gen


def is_namespace_existing(namespace):
    return os.path.exists(namespace_folder(namespace))


def namespace_folder(namespace):
    return os.path.join(_db_folder(), namespace)


def create_namespace(namespace):
    folder = namespace_folder(namespace)
    if not os.path.exists(folder):
        os.makedirs(folder)
    return folder


def remove_namespace(namespace):
    ensure_non_existing_dir(namespace_folder(namespace))


def _population_folder(namespace):
    return os.path.join(namespace_folder(namespace), "populations")


def population_folder(namespace, population_id):
    return os.path.join(_population_folder(namespace), population_id)


def _generators_folder(namespace):
    return os.path.join(
        namespace_folder(namespace),
        "generators")


def _gen_folder(namespace, gen_type):
    return os.path.join(_generators_folder(namespace), gen_type)


def csv_item_path(folder, item_id):
    return os.path.join(folder, "{}.csv".format(item_id))


def json_item_path(folder, item_id):
    return os.path.join(folder, "{}.json".format(item_id))


def _timer_gens_root_folder(namespace):
    return os.path.join(
        _generators_folder(namespace),
        "timer_gens")


def _empirical_discrete_gen_folder(namespace):
    return os.path.join(
        _generators_folder(namespace),
        "empirical_discrete_gens")


def _db_folder():
    this_folder = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(this_folder, "_DB")
