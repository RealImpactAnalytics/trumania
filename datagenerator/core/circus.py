from datagenerator.core.action import *
import os


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

    def __init__(self, master_seed, output_folder, **clock_params):
        """Create a new Circus object

        :param master_seed: seed used to initialized random generatof of
        other seeds
        :type master_seed: int

        :param output_folder: folder where to write the logs.
        :type output_folder: string

        :rtype: Circus
        :return: a new Circus object, with the clock, is created
        """
        self.seeder = seed_provider(master_seed=master_seed)
        self.output_folder = output_folder
        self.clock = Clock(seed=self.seeder.next(), **clock_params)
        self.__actions = []

    def create_action(self, name, **action_params):
        existing = self.get_action(name)

        if existing is None:
            action = Action(name=name, **action_params)
            self.__actions.append(action)
            return action

        else:
            raise ValueError("Cannot add action {}: another action with "
                             "identical name is already in the circus".format(name))

    def get_action(self, action_name):
        found = filter(lambda a: a.name == action_name, self.__actions)
        if len(found) == 0:
            logging.warn("action not found: {}".format(action_name))
            return None
        else:
            return found[0]

    def get_actor_of(self, action_name):
        return self.get_action(action_name).triggering_actor

    def save_logs(self, log_id, logs):
        """
        Appends those logs to the corresponding output file, creating it if
        it does not exist or appending lines to it otherwise.
        """

        output_file = os.path.join(self.output_folder, "{}.csv".format(log_id))

        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

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

    def run(self, duration, delete_existing_logs=False):
        """
        Executes all actions in the circus for as long as requested.

        :param duration: duration of the desired simulation (start date is
        dictated by the clock)
        :type duration: pd.TimeDelta

        :param delete_existing_logs:
        """

        n_iterations = self.clock.n_iterations(duration)
        logging.info("Starting circus for {} iterations of {}".format(
            n_iterations, self.clock.step_duration))

        if os.path.exists(self.output_folder):
            if delete_existing_logs:
                ensure_non_existing_dir(self.output_folder)
            else:
                raise EnvironmentError("{} exists and delete_existing_logs "
                                       "is False => refusing to start and "
                                       "overwrite logs".format(self.output_folder))

        for step_number in range(n_iterations):
            logging.info("step : {}".format(step_number))

            for action in self.__actions:
                for log_id, logs in action.execute().iteritems():
                    self.save_logs(log_id, logs)

            self.clock.increment()
