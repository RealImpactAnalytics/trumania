from datagenerator.core.clock import *


class HighWeekDaysTimerGenerator(CyclicTimerGenerator):
    """
    Basic CyclicTimerGenerator with a one week period that allocates higher
    probabilities to week-day vs week-ends
    """
    def __init__(self, clock, seed):

        start_date = pd.Timestamp("6 June 2016 00:00:00")
        CyclicTimerGenerator.__init__(self,
                                      clock=clock,
                                      profile=[5., 5., 5., 5., 5., 3., 3.],
                                      profile_time_steps="1D",
                                      start_date=start_date,
                                      seed=seed)


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
                                      profile=[1, .5, .2, .15, .2, .4, 3.8,
                                               7.2, 8.4, 9.1, 9.0, 8.3, 8.1,
                                               7.7, 7.4, 7.8, 8.0, 7.9, 9.7,
                                               10.4, 10.5, 8.8, 5.7, 2.8],
                                      profile_time_steps="1h",
                                      start_date=start_date,
                                      seed=seed)
