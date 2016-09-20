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
from datagenerator.core.util_functions import *
import datagenerator.core.clock as clock


def save_actor(actor, namespace, actor_id):
    actor.save_to(_actor_folder(namespace, actor_id))


def load_actor(namespace, actor_id):
    return Actor.load_from(_actor_folder(namespace, actor_id))


def save_timer_gen(timer_gen, namespace, timer_gen_id):

    timer_gen_folder = _timer_gen_folder(namespace)
    if not os._exists(timer_gen_folder):
        os.makedirs(timer_gen_folder)

    timer_gen_file_path = os.path.join(timer_gen_folder,
                                       "%s.csv" % timer_gen_id)
    timer_gen.save_to(timer_gen_file_path)


def load_timer_gen_config(namespace, timer_gen_id):
    timer_gen_folder = _timer_gen_folder(namespace)
    timer_gen_file_path = os.path.join(timer_gen_folder,
                                       "%s.csv" % timer_gen_id)

    return clock.CyclicTimerProfile.load_from(timer_gen_file_path)


def remove_namespace(namespace):
    ensure_non_existing_dir(_namespace_folder(namespace))


def _actor_folder(namespace, actor_id):
    return os.path.join(
        _namespace_folder(namespace),
        "actors",
        actor_id)


def _timer_gen_folder(namespace):
    return os.path.join(
        _namespace_folder(namespace),
        "timer_gens")


def _namespace_folder(namespace):
    return os.path.join(_db_folder(), namespace)


def _db_folder():
    this_folder = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(this_folder, "_DB")
