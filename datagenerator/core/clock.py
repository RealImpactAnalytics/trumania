from __future__ import division

import itertools
from datetime import timedelta

from datagenerator.core.random_generators import *


class Clock(object):
    """
    A Clock is the central object managing the evolution of time of the whole circus.
    It's generating timestamps on demand, and provides information for TimeProfiler objects.
    """

    def __init__(self, start, step_s, format_for_out, seed):
        """Create a Clock object.

        :type start: DateTime object
        :param start: instant of start of the generation
        :type step_s: int
        :param step_s: number of seconds between each iteration
        :type format_for_out: string
        :param format_for_out: format string to return timestamps (need to be accepted by the strftime function)
        :type seed: int
        :param seed: seed for timestamp generator (if steps are more than 1 sec)
        :return: a new Clock object, initialised
        """

        self.__current = start
        self.step_s = step_s
        self.step_delta = timedelta(seconds=step_s)
        self.number_of_ticks_per_day = int(24 * 60 * 60 / step_s)
        self.number_of_ticks_per_week = 7 * self.number_of_ticks_per_day

        self.day_index = int((start.hour * 3600 + start.minute * 60
                              + start.second) / step_s)
        self.week_index = int((start.weekday() * 24 * 3600) /
                              step_s) + self.day_index

        self.__format_for_out = format_for_out
        self.__state = RandomState(seed)
        self.ops = self.ClockOps(self)

        self.__increment_listeners = []

    def register_increment_listener(self, listener):
        """Add an object to be incremented at each step (such as a TimeProfiler)
        """
        self.__increment_listeners.append(listener)

    def increment(self):
        """Increments the clock by 1 step

        :rtype: NoneType
        :return: None
        """
        self.__current += self.step_delta
        self.day_index = (self.day_index + 1) % self.number_of_ticks_per_day
        self.week_index = (self.week_index + 1) % self.number_of_ticks_per_week

        for listener in self.__increment_listeners:
            listener.increment()

    def get_timestamp(self, size=1):
        """Returns random timestamps within the current value of the clock and the next step

        :type size: int
        :param size: number of timestamps to generate, default 1
        :rtype: Pandas Series
        :return: random timestamps in the form of strings, formatted as defined in the Clock.
        """

        def make_ts(x):
            return (self.__current + timedelta(seconds=x)).strftime(self.__format_for_out)

        return pd.Series(self.__state.choice(self.step_s, size)).apply(make_ts)

    def get_week_index(self):
        """Return the number of steps in which the Clock is from the start of the week.
        For example, if the clock has a step of 3600s, Monday 12:00:00 returns 12, Tuesday 23:00:00 returns 47, etc...

        :rtype: int
        :return: the count of number of steps already taken since the start of the week
        """
        return self.week_index

    def get_day_index(self):
        """Return the number of steps in which the Clock is from the start of the day.
        For example, if the clock has a step of 3600s, 12:00:00 returns 12,  23:00:00 returns 23, etc...

        :rtype: int
        :return: the count of number of steps already taken since the start of the day
        """
        return self.day_index

    class ClockOps(object):
        def __init__(self, clock):
            self.clock = clock

        class Timestamp(AddColumns):
            def __init__(self, clock, named_as):
                AddColumns.__init__(self)
                self.clock = clock
                self.named_as = named_as

            def build_output(self, action_data):
                values = self.clock.get_timestamp(action_data.shape[0]).values
                df = pd.DataFrame({self.named_as: values},
                                  index=action_data.index)
                return df

        def timestamp(self, named_as):
            """
            Generates a random timestamp within the current time slice
            """
            return self.Timestamp(self.clock, named_as)


class TimerGenerator(DependentGenerator):
    """A TimeProfiler contains an activity profile over a defined time range.
    It's mostly a super class, normally only its child classes should be used.

    The goal of a TimeProfiler is to keep a track of the expected level of activity of users over a cyclic time range
    It will store a vector with probabilities of activity per time step, as well as a cumulative sum of the probabilities
    starting with the current time step.

    This allows to quickly produce random waiting times until the next event for the users

    """
    def __init__(self, clock, profile, seed=None):
        """
        This should not be used, only child classes

        :type clock: Clock
        :param clock: the master clock driving this simulator
        :type profile: Pandas Series, index is a timedelta object (minimum 1 sec and last is defined by the type of profile)
        values are floats
        :param profile: Weight of each period. The index indicate the right bound of the bin (left bound is 0 for the
        first bin and the previous index for all others)
        :type seed: int
        :param seed: seed for random number generator, default None
        :return: A new TimeProfiler is created
        """
        DependentGenerator.__init__(self)
        self._state = RandomState(seed)

        totalweight = profile.sum()
        rightbounds = profile.index.values

        leftbounds = np.append(np.array([np.timedelta64(-1, "s")]), profile.index.values[:-1])
        widths = rightbounds - leftbounds
        n_subbins = widths / np.timedelta64(clock.step_s, "s")

        norm_prof = list(itertools.chain(
            *[n_subbins[i] * [profile.iloc[i] / float(n_subbins[i] * totalweight), ]
              for i in range(len(n_subbins))]))

        self._profile = pd.DataFrame({"weight": norm_prof,
                                      "next_prob": np.nan,
                                      "timeframe": np.arange(len(norm_prof))})

        # makes sure we'll get notified when the clock goes forward
        clock.register_increment_listener(self)

    def get_profile(self):
        """Returns the profile, for debugging mostly

        :rtype: Pandas DataFrame
        :return: the profile of the TimeProfiler, it has 3 fields:
        - "weight" : the pdf
        - "next_prob": the cdf
        - "timeframe": the time bin that this represents
        """
        return self._profile

    def increment(self):
        """Increment the profiler of 1 step. This as as effect to move the cdf of 1 step to the right, decrease all values
        by the value of the original first entry, and placing the previous first entry at the end of the cdf, with value
         1.

        :return: None
        """
        old_end_prob = self._profile["next_prob"].iloc[len(self._profile.index) - 1]
        self._profile["next_prob"] -= self._profile["next_prob"].iloc[0]
        self._profile = pd.concat([self._profile.iloc[1:len(self._profile.index)], self._profile.iloc[0:1]],
                                  ignore_index=True)
        self._profile.loc[self._profile.index[-1], "next_prob"] = old_end_prob

    def generate(self, observations):
        """Generate random waiting times, based on some observed activity
        levels. The higher the level of activity, the shorter the waiting
        times will be

        :type observations: Pandas Series
        :param observations: contains an array of floats
        :return: Pandas Series
        """
        if sum(np.isnan(self._profile["next_prob"])) > 0:
            raise Exception("Time profiler is not initialised!")
        p = self._state.rand(len(observations.index)) / observations.values
        return pd.Series(self._profile["next_prob"].searchsorted(p),
                         index=observations.index)
