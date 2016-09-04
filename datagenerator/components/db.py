"""
This is just the provider of the IO methods save and re



A namespace defines a place where to put objects that belong together
(typically, from the same scenario or component)

"""

# TODO: we should store this elsewhere than in the git repo...

# TODO: would be cool to also be able to store empirical probability
# distribution here...

from datagenerator.core.actor import Actor
from datagenerator.core.util_functions import *


def save_actor(actor, namespace, actor_id):
    actor.save_to(_actor_folder(namespace, actor_id))


def load_actor(namespace, actor_id):
    return Actor.load_from(_actor_folder(namespace, actor_id))


def remove_namespace(namespace):
    ensure_non_existing_dir(_namespace_folder(namespace))


def _actor_folder(namespace, actor_id):
    return os.path.join(
        _namespace_folder(namespace),
        actor_id)


def _namespace_folder(namespace):
    return os.path.join(_db_folder(), namespace)


def _db_folder():
    this_folder = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(this_folder, "_DB")
