from datetime import timedelta
import numpy as np
from numpy.random import RandomState
import pandas as pd
import itertools


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

    def get_week_index(self):
        return (self.__current.weekday()*24*3600 + self.__current.hour*3600 + self.__current.minute*60+self.__current.second)/self.__step

    def get_day_index(self):
        return (self.__current.hour*3600 + self.__current.minute*60+self.__current.second)/self.__step


class TimeProfiler(object):
    """

    """

    def __init__(self, step, profile, seed=None):
        """
        This should not be used, only child classes

        :type step: int
        :param step: number of steps between each item of the profile, needs to be a common divisor of the width of
        the bins
        :type profile: Pandas Series, index is a timedelta object (minimum 1 sec and last is defined by the type of profile)
        values are floats
        :param profile: Weight of each period. The index indicate the right bound of the bin (left bound is 0 for the
        first bin and the previous index for all others)
        :type seed: int
        :param seed: seed for random number generator, default None
        :return:
        """
        self._step = step
        self._state = RandomState(seed)

        totalweight = profile.sum()
        rightbounds = profile.index.values

        leftbounds = np.append(np.array([np.timedelta64(-1,"s")]),profile.index.values[:-1])
        widths = rightbounds-leftbounds
        n_subbins = widths/np.timedelta64(step,"s")

        norm_prof = list(itertools.chain(*[n_subbins[i]*[profile.iloc[i]/float(n_subbins[i]*totalweight),] for i in range(len(n_subbins))]))

        self._profile = pd.DataFrame({"weight":norm_prof,"next_prob":np.nan,"timeframe":np.arange(len(norm_prof))})

    def get_profile(self):
        return self._profile

    def increment(self):
        """

        :return: None
        """
        old_end_prob = self._profile["next_prob"].iloc[len(self._profile.index)-1]
        self._profile["next_prob"] -= self._profile["next_prob"].iloc[0]
        self._profile = pd.concat([self._profile.iloc[1:len(self._profile.index)],self._profile.iloc[0:1]],ignore_index=True)
        self._profile.loc[self._profile.index[-1],"next_prob"] = old_end_prob

    def generate(self,weights):
        """

        :type weights: Pandas Series
        :param weights: contains an array of floats
        :return: Pandas Series
        """
        p = self._state.rand(len(weights.index))/weights.values
        return pd.Series(self._profile["next_prob"].searchsorted(p),index=weights.index)


class WeekProfiler(TimeProfiler):
    """

    """

    def __init__(self,step,profile,seed=None):
        """

        :type step: int
        :param step: number of steps between each item of the profile, needs to be a common divisor of the width of
        the bins
        :type profile: Pandas Series, index is a timedelta object (minimum 1 sec and last has to be 6 days, 23h 59 min 59 secs)
        values are floats
        :param profile: Weight of each period. The index indicate the right bound of the bin (left bound is 0 for the
        first bin and the previous index for all others)
        :type seed: int
        :param seed: seed for random number generator, default None
        :return:
        """
        assert profile.index.values[-1] == np.timedelta64(604799,"s")

        TimeProfiler.__init__(self,step,profile,seed)

    def initialise(self,clock):
        """

        :param clock: a Clock object
        :return: None
        """
        start = clock.get_week_index()
        self._profile = pd.concat([self._profile.iloc[start:len(self._profile.index)],self._profile.iloc[0:start]],ignore_index=True)
        self._profile["next_prob"] = self._profile["weight"].cumsum()


class DayProfiler(TimeProfiler):
    """

    """

    def __init__(self,step,profile,seed=None):
        """

        :type step: int
        :param step: number of steps between each item of the profile, needs to be a common divisor of the width of
        the bins
        :type profile: Pandas Series, index is a timedelta object (minimum 1 sec and last has to be 23h 59 min 59 secs)
        values are floats
        :param profile: Weight of each period. The index indicate the right bound of the bin (left bound is 0 for the
        first bin and the previous index for all others)
        :type seed: int
        :param seed: seed for random number generator, default None
        :return:
        """
        assert profile.index.values[-1] == np.timedelta64(86399,"s")

        TimeProfiler.__init__(self,step,profile,seed)

    def initialise(self,clock):
        """

        :param clock: a Clock object
        :return: None
        """
        start = clock.get_day_index()
        self._profile = pd.concat([self._profile.iloc[start:len(self._profile.index)],self._profile.iloc[0:start]],ignore_index=True)
        self._profile["next_prob"] = self._profile["weight"].cumsum()