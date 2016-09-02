from datagenerator.action import *
import logging


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

    def __init__(self, clock):
        """Create a new Circus object

        :type clock: Clock object
        :param clock:
        :rtype: Circus
        :return: a new Circus object, with the clock, is created
        """
        self.__items = {}
        self.clock = clock
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

    def one_step(self, round_number):
        """
        Performs one round of all actions
        """

        logging.info("step : {}".format(round_number))

        # puts the logs of all actions into one grand dictionary.
        logs = merge_dicts((action.execute() for action in self.__actions),
                           df_concat)

        self.clock.increment()

        return logs

    def run(self, n_iterations):
        """
        Executes all actions for as many iteration as specified.

        :param n_iterations:
        :return: a list of as many dataframes as there are actions configured in
        this circus, each gathering all rows produces throughout all iterations
        """

        logging.info("starting circus")
        all_actions_logs = (self.one_step(r) for r in range(n_iterations))

        # merging logs from all actions
        return merge_dicts(all_actions_logs, df_concat)
