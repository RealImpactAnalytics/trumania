import logging
import os
import json
import pandas as pd

from trumania.core import population
from trumania.components import db
from trumania.core.random_generators import seed_provider
from trumania.core.util_functions import ensure_non_existing_dir
from trumania.core.clock import Clock
from trumania.core.story import Story


class Circus(object):
    """
    A Circus is just a container of a lot of objects that are required to make the simulation
    It is also the object that will execute the stories required for 1 iteration
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
        self.clock = Clock(seed=next(self.seeder), **clock_params)
        self.stories = []
        self.populations = {}
        self.generators = {}

    def create_population(self, name, **population_params):
        """
        Creates a population with the specifed parameters and attach it to this
        circus.
        """
        if name in self.populations:
            raise ValueError("refusing to overwrite existing population: {} "
                             "".format(name))

        self.populations[name] = population.Population(circus=self, **population_params)
        return self.populations[name]

    def load_population(self, population_id, namespace=None):
        """
        Load this population definition add attach it to this circus
        """

        # Defaulting to the namespace associated to this circus if none
        # specified
        if namespace is None:
            namespace = self.name

        loaded = db.load_population(namespace=namespace,
                                    population_id=population_id, circus=self)
        self.populations[population_id] = loaded
        return loaded

    def create_story(self, name, **story_params):
        """
        Creates a story with the provided parameters and attach it to this
        circus.
        """

        existing = self.get_story(name)

        if existing is None:
            story = Story(name=name, **story_params)
            self.stories.append(story)
            return story

        else:
            raise ValueError("Cannot add story {}: another story with "
                             "identical name is already in the circus".format(name))

    def get_story(self, story_name):
        """
        Looks up and story by name in this circus and returns it. Returns none
        if not found.
        """
        remaining_stories = filter(lambda a: a.name == story_name, self.stories)
        try:
            return next(remaining_stories)
        except StopIteration:
            logging.warn("story not found: {}".format(story_name))
            return None

    def get_population_of(self, story_name):
        """
        Looks up the initiating population associated to this story
        """
        return self.get_story(story_name).triggering_population

    def attach_generator(self, gen_id, generator):
        """
        "attach" a random generator to this circus, s.t. it gets persisted
        with the rest
        """
        if gen_id in self.generators:
            raise ValueError("refusing to replace existing generator: {} "
                             "".format(gen_id))

        self.generators[gen_id] = generator

    def load_generator(self, gen_type, gen_id):
        """
        Load this generator definition add attach it to this circus
        """
        gen = db.load_generator(
            namespace=self.name, gen_type=gen_type, gen_id=gen_id)

        self.attach_generator(gen_id, gen)
        return gen

    @staticmethod
    def save_logs(log_id, logs, log_output_folder):
        """
        Appends those logs to the corresponding output file, creating it if
        it does not exist or appending lines to it otherwise.
        """

        output_file = os.path.join(log_output_folder, "{}.csv".format(log_id))

        if not os.path.exists(log_output_folder):
            os.makedirs(log_output_folder)

        if logs.shape[0] > 0:
            logging.info("appending {} rows to {}".format(logs.shape[0], output_file))

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
        Executes all stories in the circus for as long as requested.

        :param duration: duration of the desired simulation (start date is
        dictated by the clock)
        :type duration: pd.TimeDelta

        :param log_output_folder: folder where to write the logs.
        :type log_output_folder: string

        :param delete_existing_logs:
        """

        n_iterations = self.clock.n_iterations(duration)
        logging.info("Starting circus for {} iterations of {} for a "
                     "total duration of {}".format(
                        n_iterations, self.clock.step_duration, duration
                     ))

        if os.path.exists(log_output_folder):
            if delete_existing_logs:
                ensure_non_existing_dir(log_output_folder)
            else:
                raise EnvironmentError("{} exists and delete_existing_logs is "
                                       "False => refusing to start and "
                                       "overwrite logs".format(log_output_folder))

        for step_number in range(n_iterations):
            logging.info("step : {}".format(step_number))

            for story in self.stories:
                for log_id, logs in story.execute().items():
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

            for population_id in db.list_populations(namespace=circus_name):
                circus.load_population(population_id)

            for gen_type, gen_id in db.list_generators(namespace=circus_name):
                circus.load_generator(gen_type=gen_type, gen_id=gen_id)

            return circus

    def save_to_db(self, overwrite=False):
        """
        Create a db namespace named after this circus and saves all the
        populations there.

        Only static data is saved, not the stories.
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
        with open(config_file, "w") as o:
            config = {"master_seed": self.master_seed,
                      "clock_config": {
                          "start": self.clock_params["start"].isoformat(),
                          "step_duration": str(self.clock_params["step_duration"])}
                      }
            json.dump(config, o, indent=4)

        logging.info("saving all populations")
        for population_id, ac in self.populations.items():
            db.save_population(ac, namespace=self.name,
                               population_id=population_id)

        logging.info("saving all generators")
        for gen_id, generator in self.generators.items():
            db.save_generator(generator, namespace=self.name, gen_id=gen_id)

        logging.info("circus saved")

    def save_params_to_db(self, params_type, params):
        """
        Saves the params object to the circus folder in the DB for future reference
        :param params_type: "build", "run" or "target"
        :param params: the params object
        """
        target_file = os.path.join(db.namespace_folder(self.name),
                                   "params_{}.json".format(params_type))

        with open(target_file, "w") as outfile:
            json.dump(params, outfile)

    def description(self):

        return {
            "circus_name": self.name,
            "master_seed": self.master_seed,
            "populations": {id: population.description()
                            for id, population in self.populations.items()
                            },
            "generators": {gen_id: gen.description()
                           for gen_id, gen in self.generators.items()
                           },
        }

    def __str__(self):
        return json.dumps(self.description(), indent=4)
