import pandas as pd

from trumania.core.clock import CyclicTimerGenerator, CyclicTimerProfile


class HighWeekDaysTimerGenerator(CyclicTimerGenerator):
    """
    Basic CyclicTimerGenerator with a one week period that allocates higher
    probabilities to week-day vs week-ends
    """
    def __init__(self, clock, seed):

        start_date = pd.Timestamp("6 June 2016 00:00:00")
        CyclicTimerGenerator.__init__(self,
                                      clock=clock,
                                      seed=seed,
                                      config=CyclicTimerProfile(
                                        profile=[5., 5., 5., 5., 5., 3., 3.],
                                        profile_time_steps="1D",
                                        start_date=start_date),
                                      )


class WorkHoursTimerGenerator(CyclicTimerGenerator):
    """
    Basic CyclicTimerGenerator with a one week period that allocates uniform
    probabilities to work hours.

    Work hours happen during week days (Monday to Friday),
    and between start_hour and end_hour, both included

    """
    def __init__(self, clock, seed, start_hour=9, end_hour=17):

        assert start_hour >= 0
        assert end_hour < 24
        assert start_hour <= end_hour

        # if start_hour = 0, before_work is empty
        before_work = [0] * start_hour
        during_work = [1.] * (end_hour - start_hour + 1)
        # if end_hour = 23, after_work is empty
        after_work = [0] * (23 - end_hour)

        # the sum of before_work, during_work and after_work is always 24
        week_day_profile = before_work + during_work + after_work
        weekend_day_profile = [0] * 24

        week_profile = week_day_profile * 5 + weekend_day_profile * 2

        start_date = pd.Timestamp("6 June 2016 00:00:00")
        CyclicTimerGenerator.__init__(self,
                                      clock=clock,
                                      seed=seed,
                                      config=CyclicTimerProfile(
                                          profile=week_profile,
                                          profile_time_steps="1h",
                                          start_date=start_date))


class DefaultDailyTimerGenerator(CyclicTimerGenerator):
    """
    Basic CyclicTimerGenerator with a one dat period with hourly weights
    vaguely inspired from

    https://github.com/RealImpactAnalytics/lab-home-work-detection/blob/3bacb58a53f69824102437a27218149f75d322e2/pub/chimayblue/01%20basic%20exploration.ipynb

    """
    def __init__(self, clock, seed):
        # any date starting at midnight is ok...
        start_date = pd.Timestamp("6 June 2016 00:00:00")
        CyclicTimerGenerator.__init__(self,
                                      clock=clock,
                                      seed=seed,
                                      config=CyclicTimerProfile(
                                                  profile=[1, .5, .2, .15, .2, .4, 3.8,
                                                           7.2, 8.4, 9.1, 9.0, 8.3, 8.1,
                                                           7.7, 7.4, 7.8, 8.0, 7.9, 9.7,
                                                           10.4, 10.5, 8.8, 5.7, 2.8],
                                                  profile_time_steps="1h",
                                                  start_date=start_date,
                                              ),
                                      )
