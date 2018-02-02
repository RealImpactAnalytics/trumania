from __future__ import division

import pandas as pd
import logging
import numpy as np
from numpy.random import RandomState

from trumania.core.operations import AddColumns
from trumania.core.random_generators import DependentGenerator
from trumania.core.util_functions import latest_date_before


class Clock(object):
    """
    A Clock is the central object managing the evolution of time of the whole circus.
    It's generating timestamps on demand, and provides information for TimeProfiler objects.
    """

    def __init__(self, start, step_duration, seed):
        """Create a Clock object.

        :type start: pd.Timestamp
        :param start: instant of start of the generation

        :type step_duration: pd.Timedelta
        :param step_duration: duration of a clock step

        :type seed: int
        :param seed: seed for timestamp generator (if steps are more than 1 sec)

        :return: a new Clock object, initialised
        """

        self.current_date = start
        self.step_duration = step_duration

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
        self.current_date += self.step_duration

        for listener in self.__increment_listeners:
            listener.increment()

    def get_timestamp(self, size=1, random=True, log_format=None):
        """
        Returns timestamps formatted as string

        :type size: int
        :param size: number of timestamps to generate, default 1

        :type random: boolean
        :param random: if True, the timestamps are randomly generated in [
        self.current_date, self.current_date+self.step_duration]

        :type log_format: string
        :param log_format: string format of the generated timestamps

        :rtype: Pandas Series
        :return: random timestamps in the form of strings
        """

        if log_format is None:
            log_format = "%Y-%m-%d %H:%M:%S"

        def make_ts(delta_secs):
            date = self.current_date + pd.Timedelta(seconds=delta_secs)
            return date.strftime(log_format)

        if random:
            step_secs = int(self.step_duration.total_seconds())
            return pd.Series(self.__state.choice(step_secs, size)).apply(make_ts)
        else:
            return pd.Series([self.current_date.strftime(log_format)] * size)

    def n_iterations(self, duration):
        """
        :type duration: pd.Timedelta

        :return: the smallest number of iteration of this clock s.t. the
        corresponding duration is >= duration
        """
        step_secs = self.step_duration.total_seconds()
        return int(np.ceil(duration.total_seconds() / step_secs))

    class ClockOps(object):
        def __init__(self, clock):
            self.clock = clock

        class Timestamp(AddColumns):
            def __init__(self, clock, named_as, random, log_format):
                AddColumns.__init__(self)
                self.clock = clock
                self.named_as = named_as
                self.random = random
                self.log_format = log_format

            def build_output(self, story_data):
                values = self.clock.get_timestamp(
                    size=story_data.shape[0], random=self.random,
                    log_format=self.log_format).values

                df = pd.DataFrame({self.named_as: values},
                                  index=story_data.index)
                return df

        def timestamp(self, named_as, random=True, log_format=None):
            """
            Generates a random timestamp within the current time slice
            """
            return self.Timestamp(self.clock, named_as, random, log_format)


class CyclicTimerGenerator(DependentGenerator):
    """A TimeProfiler contains an activity profile over a defined time range.
    It's mostly a super class, normally only its child classes should be used.

    The goal of a TimeProfiler is to keep a track of the expected level of activity of users over a cyclic time range
    It will store a vector with probabilities of activity per time step, as well as a cumulative sum of the
    probabilities starting with the current time step.

    This allows to quickly produce random waiting times until the next event for the users

    """
    def __init__(self, clock, seed, config):
        """
        This should not be used, only child classes

        :type clock: Clock
        :param clock: the master clock driving this simulator

        :type seed: int
        :param seed: seed for random number generator, default None
        :return: A new TimeProfiler is created
        """
        DependentGenerator.__init__(self)
        self._state = RandomState(seed)
        self.config = config
        self.clock = clock

        # "macro" time shift: we shift the whole profile n times in the future
        # or the past until it overlaps with the current clock date
        init_date = latest_date_before(
            starting_date=config.start_date,
            upper_bound=clock.current_date,
            time_step=pd.Timedelta(config.profile_time_steps) * len(
                config.profile))

        # Un-scaled weight profile. We artificially adds a nan to force the
        # up-sclaling to multiply the last element
        profile_idx = pd.date_range(start=init_date,
                                    freq=config.profile_time_steps,
                                    periods=len(config.profile) + 1)
        profile_ser = pd.Series(data=config.profile + [np.nan],
                                index=profile_idx)

        # scaled weight profile, s.t. one clock step == one profile value
        profile_ser = profile_ser.resample(rule=clock.step_duration).pad()[:-1]

        self.n_time_bin = profile_ser.shape[0]

        profile_cdf = (profile_ser / profile_ser.sum()).cumsum()
        self.profile = pd.DataFrame({"cdf": profile_cdf,

                                     # for debugging
                                     "timeframe": np.arange(len(profile_cdf))})

        # "micro" time shift,: we step forward along the profile until it is
        # align with the current date
        while self.profile.index[0] < clock.current_date:
            self.increment()

        # makes sure we'll get notified when the clock goes forward
        clock.register_increment_listener(self)

    def increment(self):
        """
        Increment the time generator by 1 step.

        This has as effect to move the cdf of one step to the left, decrease
        all values by the value of the original first entry, and placing the
        previous first entry at the end of the cdf, with value 1.
        """

        self.profile["cdf"] -= self.profile["cdf"].iloc[0]

        self.profile = pd.concat([self.profile.iloc[1:], self.profile.iloc[:1]])
        self.profile.loc[self.profile.index[-1], "cdf"] = 1

    def generate(self, observations):
        """Generate random waiting times, based on some observed activity
        levels. The higher the level of activity, the shorter the waiting
        times will be

        :type observations: Pandas Series
        :param observations: contains an array of floats
        :return: Pandas Series
        """

        activities = observations

        # activities less often than once per cycle length
        low_activities = activities.where((activities <= 2) & (activities > 0)).dropna()
        if low_activities.shape[0] > 0:

            draw = self._state.uniform(size=low_activities.shape[0])

            # A uniform [0, 2/activity] yields an expected freqs == 1/activity
            # == average period between story.
            # => n_cycles is the number of full timer cycles from now until
            # next story. It's typically not an integer and possibly be > 1
            # since we have on average less han 1 activity per cycle of this
            # timer.
            n_cycles = 2 * draw / low_activities.values

            timer_slots = n_cycles % 1
            n_cycles_int = n_cycles - timer_slots

            timers = self.profile["cdf"].searchsorted(timer_slots) + \
                self.n_time_bin * n_cycles_int

            low_activity_timer = pd.Series(timers, index=low_activities.index)

        else:
            low_activity_timer = pd.Series()

        high_activities = activities.where(activities > 2).dropna()
        if high_activities.shape[0] > 0:

            # A beta(1, activity-1) will yield expected frequencies of
            # 1/(1+activity-1) == 1/activity == average period between story.
            # This just stops to work for activities < 1, or even close to one
            # => we use the uniform mechanism above for activities <= 2 and
            # rely on betas here for expected frequencies of 2 per cycle or
            # higher
            timer_slots = high_activities.apply(
                lambda activity: self._state.beta(1, activity - 1))

            timers = self.profile["cdf"].searchsorted(timer_slots, side="left")
            high_activity_timer = pd.Series(timers, index=high_activities.index)

        else:
            high_activity_timer = pd.Series()

        all_timers = pd.concat([low_activity_timer, high_activity_timer])

        # Not sure about that one, there seem to be a bias somewhere that
        # systematically generates too large timer. Maybe it's a rounding
        # effect of searchsorted() or so. Or a bug elsewhere ?
        all_timers = all_timers.apply(lambda d: max(0, d - 1))

        # makes sure all_timers is in the same order and with the same index
        # as input observations, even in case of duplicate index values
        all_timers = all_timers.reindex_like(observations)
        return all_timers

    def activity(self, n, per):
        """

        :param n: number of stories
        :param per: time period for that number of stories
        :type per: pd.Timedelta
        :return: the activity level corresponding to the specified number of n
        executions per time period
        """

        scale = self.config.duration().total_seconds() / per.total_seconds()
        activity = n * scale

        requested_period = pd.Timedelta(seconds=per.total_seconds() / n)
        if requested_period < self.clock.step_duration:
            logging.warning(
                "Warning: Creating activity level for {} stories per "
                "{} =>  activity is {} but period is {}, which is "
                "shorter  than the clock period ({}). This clock "
                "cannot keep up with such rate and less events will be"
                " produced".format(n, per, activity, requested_period,
                                   self.clock.step_duration)
            )

        return activity


class CyclicTimerProfile(object):
    """
    Static parameters of the Timer profile. Separated from the timer gen
    itself to facilitate persistence.

    :type profile: python array
    :param profile: Weight of each period

    :type profile_time_steps: string
    :param profile_time_steps: duration of the time-steps in the profile
    (e.g. "15min")

    :type start_date: pd.Timestamp
    :param start_date: date of the origin of the specified profile =>
    this is used to align with the values of the clock

    """
    def __init__(self, profile, profile_time_steps, start_date):
        self.start_date = start_date
        self.profile = profile
        self.profile_time_steps = profile_time_steps

    def save_to(self, file_path):

        logging.info("saving timer generator to {}".format(file_path))

        saved_df = pd.DataFrame({("value", "profile"): self.profile},
                                dtype=str).stack()
        saved_df.index = saved_df.index.reorder_levels([1, 0])
        saved_df.loc[("start_date", 0)] = self.start_date
        saved_df.loc[("profile_time_steps", 0)] = self.profile_time_steps
        saved_df.to_csv(file_path)

    @staticmethod
    def load_from(file_path):
        saved_df = pd.read_csv(file_path, index_col=[0, 1])

        profile = saved_df.loc[("profile", slice(None))]\
            .unstack()\
            .astype(float)\
            .tolist()

        profile_time_steps = saved_df.loc["profile_time_steps"].values[0][0]
        start_date = pd.Timestamp(saved_df.loc["start_date"].values[0][0])

        return CyclicTimerProfile(profile, profile_time_steps, start_date)

    def duration(self):
        """
        :return: the total duration corresponding to this time profile
        """

        return len(self.profile) * pd.Timedelta(self.profile_time_steps)
