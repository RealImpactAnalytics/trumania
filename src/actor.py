import pandas as pd
import numpy as np


class Actor(object):
    """

    """

    def __init__(self, size, id_start=0):
        """

        :param size:
        :param id_start:
        :return:
        """
        IDs = np.arange(id_start, id_start + size)
        self._table = pd.DataFrame({"ID": IDs, "clock": 0})

    def add_attribute(self, name, generator):
        """Adds a column named "name" to the inner table of the actor, randomly generated from the generator.

        :param name: string, will be used as name for the column in the table
        :param generator: class from the random_generator series. needs to have a generate function that works
        :return: none
        """
        self._table[name] = generator.generate(len(self._table.index))

    def who_acts_now(self):
        """

        :return:
        """
        return self._table[self._table["clock"] == 0]

    def update_clock(self, decrease=1):
        """

        :param decrease:
        :return:
        """
        self._table["clock"] -= 1

    def update_attribute(self, name, generator):
        """

        :param name:
        :param generator:
        :return:
        """
        self._table[name] = generator.generate(len(self._table.index))

    def make_actions(self, new_time_generator):
        """

        :param new_time_generator:
        :return:
        """
        act_now = self.who_acts_now()
        out = pd.DataFrame(columns=["ID", "action"])
        if len(act_now.index) > 0:
            out["ID"] = act_now["ID"]
            out["action"] = "ping"
            self._table.loc[act_now.index, "clock"] = new_time_generator.generate(len(act_now.index))
        self.update_clock()
        return out

    def __repr__(self):
        return self._table


class CustomerActor(Actor):
    def __init__(self, size, id_start=0):
        """

        :param size:
        :param id_start:
        :return:
        """
        Actor.__init__(self, size, id_start)

    def make_calls(self, network, chooser, new_time_generator):
        """

        :param network:
        :param chooser:
        :param new_time_generator:
        :return:
        """
        act_now = self.who_acts_now()
        out = pd.DataFrame(columns=["A", "B"])
        if len(act_now.index) > 0:
            out = network.select_one("A", act_now["ID"].values)
            self._table.loc[act_now.index, "clock"] = new_time_generator.generate(len(act_now.index))
            # TODO next generate time with different generators depending on customers
        self.update_clock()
        return out