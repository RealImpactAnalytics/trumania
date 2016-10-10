"""
This is just the provider of the IO methods save and retrieve various
simulation components to/from persistence.

A namespace defines a place where to put objects that belong together
(typically, from the same scenario or component, e.g. "Uganda").

"""

# TODO: we should store this elsewhere than in the git repo...

# TODO: would be cool to also be able to store empirical probability
# distribution here, for the random generators...

from datagenerator.core.actor import Actor
import datagenerator.core.clock as clock
from datagenerator.core.random_generators import *


def save_actor(actor, namespace, actor_id):
    actor.save_to(_actor_folder(namespace, actor_id))


def load_actor(namespace, actor_id):
    return Actor.load_from(_actor_folder(namespace, actor_id))


def list_actors(namespace):
    folder = namespace_folder(namespace)
    return [d for d in os.listdir(folder) if os.path.isdir(d)]


def save_timer_gen(timer_gen, namespace, timer_gen_id):

    timer_gen_folder = _timer_gens_root_folder(namespace)
    ensure_folder_exists(timer_gen_folder)
    timer_gen_file_path = os.path.join(timer_gen_folder,
                                       "%s.csv" % timer_gen_id)
    timer_gen.save_to(timer_gen_file_path)


def load_timer_gen_config(namespace, timer_gen_id):
    timer_gen_folder = _timer_gens_root_folder(namespace)
    timer_gen_file_path = os.path.join(timer_gen_folder,
                                       "%s.csv" % timer_gen_id)

    return clock.CyclicTimerProfile.load_from(timer_gen_file_path)


def save_empirical_discrete_generator(distribution, values, namespace, gen_id):
    assert distribution.sum() - 1 < 1e-6

    root_folder = _empirical_discrete_gen_folder(namespace)
    ensure_folder_exists(root_folder)
    gen_file_path = os.path.join(root_folder, "%s.csv" % gen_id)

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
        a=df["x"],
        p=df["px"],
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


def _actor_folder(namespace, actor_id):
    return os.path.join(
        namespace_folder(namespace),
        "actors",
        actor_id)


def _generators_folder(namespace):
    return os.path.join(
        namespace_folder(namespace),
        "generators")


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
