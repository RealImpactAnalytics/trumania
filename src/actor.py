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
        self._table = pd.DataFrame({"ID": IDs, "clock": 0, "activity":1.})
        self._transient_attributes = {}

    def add_attribute(self, name, generator,weights=None,weight_field=None):
        """Adds a column named "name" to the inner table of the actor, randomly generated from the generator.

        :param name: string, will be used as name for the column in the table
        :param generator: class from the random_generator series. needs to have a generate function that works
        :param weights: Pandas Series
        :return: none
        """
        if weights is not None:
            self._table[name] = generator.generate(weights)
        elif weight_field is not None:
            self._table[name] = generator.generate(self._table[weight_field])
        else:
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

    def update_attribute(self, name, generator,weights=None,weight_field=None):
        """

        :param name:
        :param generator:
        :param weights:
        :return:
        """
        if weights is not None:
            self._table[name] = generator.generate(weights)
        elif weight_field is not None:
            self._table[name] = generator.generate(self._table[weight_field])
        else:
            self._table[name] = generator.generate(len(self._table.index))

    def make_actions(self, new_time_generator, relationship=None):
        """

        :param new_time_generator:
        :return:
        """
        act_now = self.who_acts_now()
        out = pd.DataFrame(columns=["ID", "action"])
        if len(act_now.index) > 0:
            out["ID"] = act_now["ID"]
            out["action"] = "ping"
            self._table.loc[act_now.index, "clock"] = new_time_generator.generate(act_now["activity"])+1
        self.update_clock()
        return out

    def __repr__(self):
        return self._table


class CallerActor(Actor):
    """
    A CallerActor is an actor that makes calls
    """
    def __init__(self, size, id_start=0):
        """

        :param size:
        :param id_start:
        :return:
        """
        Actor.__init__(self, size, id_start)

    def make_calls(self, new_time_generator, relationship):
        """

        :param relationship:
        :param new_time_generator:
        :return:
        """
        act_now = self.who_acts_now()
        out = pd.DataFrame(columns=["A", "B"])
        if len(act_now.index) > 0:
            calls = relationship.select_one("A", act_now["ID"].values)
            if len(calls.index) > 0:
                out=calls[["A","B"]]

            self._table.loc[act_now.index, "clock"] = new_time_generator.generate(act_now["activity"])+1
        self.update_clock()
        return out


class TransientAttribute(object):
    """

    """
    def __init__(self, ids):
        """

        :param ids:
        :return:
        """
        self._table = pd.DataFrame({"ID": ids, "value": np.NaN, "clock": 0})

    def update(self, ids_to_update, values):
        """

        :param values:
        :param ids_to_update:
        :return:
        """
        self._table[self._table["ID"].isin(ids_to_update)]["value"] = values

    def who_acts_now(self):
        """

        :return:
        """
        return self._table[self._table["clock"] == 0]

    def make_actions(self, new_time_generator, relationship, id1, id2):
        """

        :param new_time_generator:
        :param relationship:
        :param id1:
        :param id2:
        :return:
        """
        act_now = self.who_acts_now()
        out = pd.DataFrame(columns=["ID", "new"])
        if len(act_now.index) > 0:
            out = relationship.select_one(id1,act_now["ID"].values).rename(columns={id1:"ID",id2:"new"})
            self._table[self._table["ID"].isin(out[id1].values)]["value"] = out["new"]
            self._table[self._table["ID"].isin(out[id1].values)]["clock"] = new_time_generator.generate(len(act_now.index))
        return out
