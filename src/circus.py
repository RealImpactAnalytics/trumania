import pandas as pd
import numpy as np


class Circus(object):
    """

    """

    def __init__(self, clock):
        """

        :type clock: Clock object
        :param clock:
        :return:
        """
        self.__actors = {}
        self.__items = {}
        self.__relationships = {}
        self.__generators = {}
        self.__clock = clock
        self.__actions = []
        self.__incrementors = []

    def add_actor(self, name, actor):
        """

        :param name:
        :param actor:
        :return:
        """
        if self.__actors.has_key(name):
            raise Exception("Already having actors named %s" % name)
        self.__actors[name] = actor

    def add_item(self, name, item):
        """

        :param name:
        :param item:
        :return:
        """
        if self.__items.has_key(name):
            raise Exception("Already having items named %s" % name)
        self.__items[name] = item

    def add_relationship(self, r1, r2, rel):
        """

        :param r1:
        :param r2:
        :param rel:
        :return:
        """
        if self.__relationships.has_key((r1, r2)):
            raise Exception("Already having relationship between %s and %s" % (r1, r2))
        self.__relationships[(r1, r2)] = rel

    def add_generator(self, name, gen):
        """

        :param name:
        :param gen:
        :return:
        """
        if self.__generators.has_key(name):
            raise Exception("Already having generator named %s" % name)

        self.__generators[name] = gen

    def add_action(self, actor, func, param, add_info):
        """

        :type actor: string
        :param actor: name of an actor object
        :type func: function that applies to actor
        :param func: function to apply to actor
        :type param: dictionary
        :param param: keyworded arguments of func
        :type add_info: dict
        :param add_info:
        :return:
        """
        self.__actions.append((actor, func, param, add_info))

    def add_increment(self, to_increment):
        """

        :param to_increment:
        :return:
        """
        self.__incrementors.append(to_increment)

    def one_round(self):
        """
        Performs one round of actions

        :return:
        """
        out_tables = []
        for a in self.__actions:
            out = getattr(self.__actors[a[0]], a[1])(**a[2])
            for j in a[3]:
                if j == "timestamp":
                    if a[3][j]:
                        out["datetime"] = self.__clock.get_timestamp(len(out.index))
                if j == "join":
                    for j_info in a[3][j]:
                        # entry is then field in out, actor or item name, actor or item field, new name
                        out_field, obj_to_join, obj_field, new_name = j_info
                        out[new_name] = obj_to_join.get_join(obj_field, out[out_field])

            out_tables.append(out)

        for i in self.__incrementors:
            i.increment()
        self.__clock.increment()

        return out_tables

    def get_contents(self):
        return (self.__clock,
                self.__actors,
                self.__items,
                self.__relationships,
                self.__generators)
