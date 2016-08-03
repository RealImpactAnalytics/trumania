import pandas as pd


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
        self.__clock = clock
        self.__actions = []
        self.__incrementors = []

    def add_item(self, name, item):
        """Add an Item object to the list of items

        :type name: str
        :param name: the name to reference the Item
        :type item: Item object
        :param item: the Item object to add
        :return: None
        """
        if self.__items.has_key(name):
            raise Exception("Already having items named %s" % name)
        self.__items[name] = item

    def add_action(self, action):
        """Add an action to perform

        :type action: Action
        :param action: the action to execute
        :type supp_fields: dict
        :param supp_fields: dictionary of additional fields to complete for the action logs
        Currently, the dictionary can have 2 entries:
        - "timestamp": {True, False} if a timestamp needs to be added
        - "join": [list of tuples with ("field to join on",
                                        object to join on (Actor or Item),
                                        "object field to join on",
                                        "field name in output table")]
        :return:
        """
        self.__actions.append(action)

    def add_increment(self, to_increment):
        """Add an object to be incremented at each step (such as a TimeProfiler)

        :param to_increment:
        :return:
        """
        self.__incrementors.append(to_increment)

    def __execute_action(self, action):
        """
            executes this action and adds a timestamp to the result
        """
        action_values = action.execute()
        action_values["datetime"] = self.__clock.get_timestamp(
            action_values.shape[0]).values

        return action_values

    def one_round(self):
        """
        Performs one round of actions

        :return:
        """

        result_tables = [self.__execute_action(action)
                         for action in self.__actions]

        for i in self.__incrementors:
            i.increment()

        self.__clock.increment()

        return result_tables

    def run(self, n_iterations):
        """
        Executes all actions for as many iteration as specified.

        :param n_iterations:
        :return: a list of as many dataframes as there are actions configured in
        this circus, each gathering all rows produces throughout all iterations
        """

        tables_list = zip(*(self.one_round() for _ in range(n_iterations)))
        return [pd.concat(table, ignore_index=True) for table in tables_list]

    def get_contents(self):
        return (self.__clock, self.__items)
