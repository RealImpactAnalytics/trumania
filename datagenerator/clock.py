from __future__ import division
from datetime import timedelta
from numpy.random import RandomState
import itertools
from datagenerator.operations import *


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

        :param to_increment:
        :return:
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


class TimeProfiler(object):
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

    def generate(self, weights):
        """Generate random waiting times, based on activity levels in weights. The higher the weights, the shorter the
        waiting times will be

        :type weights: Pandas Series
        :param weights: contains an array of floats
        :return: Pandas Series
        """
        if sum(np.isnan(self._profile["next_prob"])) > 0:
            raise Exception("Time profiler is not initialised!")
        p = self._state.rand(len(weights.index)) / weights.values
        return pd.Series(self._profile["next_prob"].searchsorted(p),
                         index=weights.index)


class WeekProfiler(TimeProfiler):
    """WeekProfiler is a TimeProfiler on a period of a week. This imposes that the last time bin in the profile
    of the constructor needs to be equal to 6 days, 23h, 59m and 59s.

    """

    def __init__(self, clock, profile, seed=None):
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
        assert profile.index.values[-1] == np.timedelta64(604799, "s")

        TimeProfiler.__init__(self, clock, profile, seed)

        # Initialisation of the profiler
        # It sets the first bin of the cdf to the current timestamp of the clock
        start = clock.get_week_index()
        self._profile = pd.concat(
            [self._profile.iloc[start:len(self._profile.index)],
             self._profile.iloc[0:start]],
            ignore_index=True)
        self._profile["next_prob"] = self._profile["weight"].cumsum()


class DayProfiler(TimeProfiler):
    """DayProfiler is a TimeProfiler on a period of a day. This imposes that the last time bin in the profile
    of the constructor needs to be equal to 23h, 59m and 59s.

    """

    # naked eye guesstimation from
    # https://github.com/RealImpactAnalytics/lab-home-work-detection/blob/3bacb58a53f69824102437a27218149f75d322e2/pub/chimayblue/01%20basic%20exploration.ipynb
    default_profile = pd.Series(
        [1, .5, .2, .15, .2, .4, 3.8, 7.2, 8.4, 9.1, 9.0, 8.3, 8.1, 7.7, 7.4,
         7.8, 8.0, 7.9, 9.7, 10.4, 10.5, 8.8, 5.7, 2.8],
        index=[timedelta(hours=h, minutes=59, seconds=59) for h in range(24)])

    def __init__(self, clock, profile=default_profile, seed=None):
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
        assert profile.index.values[-1] == np.timedelta64(86399, "s")

        TimeProfiler.__init__(self, clock, profile, seed)

        # Initialisation of the profiler
        # It sets the first bin of the cdf to the current timestamp of the clock
        start = clock.get_day_index()
        self._profile = pd.concat(
            [self._profile.iloc[start:len(self._profile.index)],
             self._profile.iloc[0:start]],
            ignore_index=True)
        self._profile["next_prob"] = self._profile["weight"].cumsum()


class ConstantProfiler(object):
    """ConstantProfiler is a TimeProfiler that always returns the same value.
    It's a bit stupid but handy.

    """

    def __init__(self, return_value):
        """

        :type return_value: int
        :param step: value to return
        :return:
        """
        self._val = return_value

    def get_profile(self):
        """Returns the profile, for debugging mostly

        :rtype: NoneType
        :return: None
        """
        return None

    def increment(self):
        """Doesn't do anything since it's a constant profiler.

        :return: None
        """
        pass

    def generate(self, weights):
        """Generate constant values of the same size as weights. To have the same signature as other profilers.

        :type weights: Pandas Series
        :param weights: contains an array of floats
        :return: Pandas Series
        """
        return pd.Series(self._val, index=weights.index)
