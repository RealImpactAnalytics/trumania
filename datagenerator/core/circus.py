from datagenerator.core.action import *
from datagenerator.core import actor
import logging
import os
import json
from datagenerator.components import db


class Circus(object):
    """
    A Circus is just a container of a lot of objects that are required to make the simulation
    It is also the object that will execute the actions required for 1 iteration

    The different objects contained in the Circus are:
    - Actors, which do actions during the simumation
    - Items, that contain descriptive data
    - a Clock object, to manage overall time
    - Actions, a list of actions to perform on the actors
    - Incrementors, similarly to a list of actions, it's storing which generators need to be incremented at each step

    """

    def __init__(self, name, master_seed, **clock_params):
        """Create a new Circus object

        :param master_seed: seed used to initialized random generatof of
        other seeds
        :type master_seed: int

        :rtype: Circus
        :return: a new Circus object, with the clock, is created
        """
        self.name = name

        self.master_seed = master_seed
        self.clock_params = clock_params

        self.seeder = seed_provider(master_seed=master_seed)
        self.clock = Clock(seed=self.seeder.next(), **clock_params)
        self.actions = []
        self.actors = {}

    def create_actor(self, name, **actor_params):
        """
        Creates an actor with the specifed parameters and attach it to this
        circus.
        """
        if name in self.actors:
            raise ValueError("refusing to overwrite existing actor: {} "
                             "".format(name))

        self.actors[name] = actor.Actor(**actor_params)

    def load_actor(self, namespace, actor_id):
        actor = db.load_actor(namespace=namespace, actor_id=actor_id)
        self.actors[actor_id] = actor
        return actor

    def create_action(self, name, **action_params):
        existing = self.get_action(name)

        if existing is None:
            action = Action(name=name, **action_params)
            self.actions.append(action)
            return action

        else:
            raise ValueError("Cannot add action {}: another action with "
                             "identical name is already in the circus".format(name))

    def get_action(self, action_name):
        found = filter(lambda a: a.name == action_name, self.actions)
        if len(found) == 0:
            logging.warn("action not found: {}".format(action_name))
            return None
        else:
            return found[0]

    def get_actor_of(self, action_name):
        return self.get_action(action_name).triggering_actor

    def save_logs(self, log_id, logs, log_output_folder):
        """
        Appends those logs to the corresponding output file, creating it if
        it does not exist or appending lines to it otherwise.
        """

        output_file = os.path.join(log_output_folder, "{}.csv".format(log_id))

        if not os.path.exists(log_output_folder):
            os.makedirs(log_output_folder)

        if len(logs) > 0:
            if not os.path.exists(output_file):
                # If these are this first persisted logs, we create the file
                # and include the field names as column header.
                logs.to_csv(output_file, index=False, header=True)

            else:
                # Otherwise, open the existing log file in append mode and add
                # the new logs at the end, this time without columns headers.
                with open(output_file, "a") as out_f:
                    logs.to_csv(out_f, index=False, header=False)

    def run(self, duration, log_output_folder, delete_existing_logs=False):
        """
        Executes all actions in the circus for as long as requested.

        :param duration: duration of the desired simulation (start date is
        dictated by the clock)
        :type duration: pd.TimeDelta

        :param output_folder: folder where to write the logs.
        :type output_folder: string

        :param delete_existing_logs:
        """

        n_iterations = self.clock.n_iterations(duration)
        logging.info("Starting circus for {} iterations of {} for a "
                     "total duration of {}".format(
            n_iterations, self.clock.step_duration, duration))

        if os.path.exists(log_output_folder):
            if delete_existing_logs:
                ensure_non_existing_dir(log_output_folder)
            else:
                raise EnvironmentError("{} exists and delete_existing_logs is "
                                       "False => refusing to start and "
                                       "overwrite logs".format(log_output_folder))

        for step_number in range(n_iterations):
            logging.info("step : {}".format(step_number))

            for action in self.actions:
                for log_id, logs in action.execute().iteritems():
                    self.save_logs(log_id, logs, log_output_folder)

            self.clock.increment()

    @staticmethod
    def load_from_db(circus_name):

        logging.info("loading circus {}".format(circus_name))

        namespace_folder = db.namespace_folder(namespace=circus_name)
        config_file = os.path.join(namespace_folder, "circus_config.json")

        with open(config_file, "r") as config_h:
            config = json.load(config_h)

            clock_config = {
                "start": pd.Timestamp(config["clock_config"]["start"]),
                "step_duration": pd.Timedelta(
                    str(config["clock_config"]["step_duration"]))
            }

            circus = Circus(name=circus_name, master_seed=config["master_seed"],
                            **clock_config)

            for actor_id in db.list_actors(namespace=circus_name):
                circus.actions[actor_id] = db.load_actor(
                    namespace=circus_name, actor_id=actor_id)

            return circus

    def save_to_db(self, overwrite=False):
        """
        Create a db namespace named after this circus and saves all the
        actors there.

        Only static data is saved, not the actions.
        """

        logging.info("saving circus {}".format(self.name))

        if db.is_namespace_existing(namespace=self.name):
            if overwrite:
                logging.warning(
                    "overwriting existing circus {}".format(self.name))
                db.remove_namespace(namespace=self.name)

            else:
                raise IOError("refusing to remove existing {} namespace since "
                              "overwrite parameter is False".format(self.name))

        namespace_folder = db.create_namespace(namespace=self.name)
        config_file = os.path.join(namespace_folder, "circus_config.json")
        with open (config_file, "w") as o:
            config = {"master_seed": self.master_seed,
                      "clock_config": {
                          "start": self.clock_params["start"].isoformat(),
                          "step_duration":
                              str(self.clock_params["step_duration"])
                        }
                      }
            json.dump(config, o, indent=4)

        for actor_id, actor in self.actors.iteritems():
            db.save_actor(actor, namespace=self.name, actor_id=actor_id)

        logging.info("circus saved")

