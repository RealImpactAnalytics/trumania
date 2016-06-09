from datetime import timedelta
from numpy.random import RandomState
import pandas as pd


class Clock(object):
    """

    """

    def __init__(self, start, step, format_for_out, seed):
        """

        :type start: DateTime object
        :param start: instant of start of the generation
        :type step: int
        :param step: number of seconds between each iteration
        :type format_for_out: string
        :param format_for_out: format string to return timestamps (need to be accepted by the strftime function)
        :type seed: int
        :param seed: seed for timestamp generator (if steps are more than 1 sec)
        :return: a new Clock object, initialised
        """

        self.__current = start
        self.__step = step
        self.__format_for_out = format_for_out
        self.__state = RandomState(seed)

    def increment(self):
        """

        :return:
        """
        self.__current += timedelta(seconds=self.__step)

    def get_timestamp(self,size=1):
        """

        :type size: int
        :param size: number of timestamps to generate, default 1
        :return:
        """
        def make_ts(x):
            return (self.__current + timedelta(seconds=x)).strftime(self.__format_for_out)

        return pd.Series(self.__state.choice(self.__step, size)).apply(make_ts)
