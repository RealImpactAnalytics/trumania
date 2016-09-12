from datagenerator.core.clock import *


class WeeklyTimerGenerator(TimerGenerator):
    """WeekProfiler is a TimeProfiler on a period of a week.
        This imposes that the last time bin in the profile
        of the constructor needs to be equal to 6 days, 23h, 59m and 59s.

    """

    def __init__(self, clock, week_profile, seed=None):
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

        # TODO: clean this up: Svend messed up that one... ^^
        # the condition is actually that the time index is within one week (day for day profiler)
        if len(week_profile) != 7:
            raise ValueError("Week profile must be initialized with 7 daily "
                             "weights")

        profile = pd.Series(
            week_profile,
            index=[timedelta(days=d, hours=23, minutes=59, seconds=59)
                   for d in range(7)])

        TimerGenerator.__init__(self, clock, profile, seed)

        # Initialisation of the profiler
        # It sets the first bin of the cdf to the current timestamp of the clock
        start = clock.get_week_index()
        self._profile = pd.concat(
            [self._profile.iloc[start:len(self._profile.index)],
             self._profile.iloc[0:start]],
            ignore_index=True)
        self._profile["next_prob"] = self._profile["weight"].cumsum()


class DailyTimerGenerator(TimerGenerator):
    """DayProfiler is a TimeProfiler on a period of a day. This imposes that the last time bin in the profile
    of the constructor needs to be equal to 23h, 59m and 59s.

    """

    # naked eye guesstimation from
    # https://github.com/RealImpactAnalytics/lab-home-work-detection/blob/3bacb58a53f69824102437a27218149f75d322e2/pub/chimayblue/01%20basic%20exploration.ipynb
    default_profile = [1, .5, .2, .15, .2, .4, 3.8, 7.2, 8.4, 9.1, 9.0, 8.3,
                       8.1, 7.7, 7.4, 7.8, 8.0, 7.9, 9.7, 10.4, 10.5, 8.8,
                       5.7, 2.8]

    def __init__(self, clock, day_profile=default_profile, seed=None):
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

        if len(day_profile) != 24:
            raise ValueError("day profile must be initialized with 24 hourly"
                             "weights")

        profile = pd.Series(
            day_profile,
            index=[timedelta(hours=h, minutes=59, seconds=59)
                   for h in range(24)])

        TimerGenerator.__init__(self, clock, profile, seed)

        # Initialisation of the profiler
        # It sets the first bin of the cdf to the current timestamp of the clock
        start = clock.get_day_index()
        self._profile = pd.concat(
            [self._profile.iloc[start:len(self._profile.index)],
             self._profile.iloc[0:start]],
            ignore_index=True)
        self._profile["next_prob"] = self._profile["weight"].cumsum()
