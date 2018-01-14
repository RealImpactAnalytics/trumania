import pandas as pd

from trumania.core.clock import CyclicTimerProfile, CyclicTimerGenerator
from trumania.core.clock import Clock
from trumania.components.time_patterns.profilers import DefaultDailyTimerGenerator


def test_clock_tick_per_day():

    clock = Clock(start=pd.Timestamp("10 June 2016 5:45pm"),
                  step_duration=pd.Timedelta("15 min"),
                  seed=1234)

    # time steps is 900 s, i.e 15 min
    assert clock.n_iterations(pd.Timedelta("7D")) == 7 * 24 * 4
    assert clock.n_iterations(pd.Timedelta("1D")) == 24 * 4

    # 47 min should be rounded up to 4 quarters
    assert clock.n_iterations(pd.Timedelta("47min")) == 4


def test_init_cyclictimergenerator():

    # say we have a clock at 5.45pm on 10th June
    clock = Clock(start=pd.Timestamp("10 June 2016 5:45pm"),
                  # time steps by 15min
                  step_duration=pd.Timedelta("15 min"),
                  seed=1234)

    # 1 to 12 then 12 to 1, from midnight to midnight
    timer_gen = CyclicTimerGenerator(
        clock=clock,
        config=CyclicTimerProfile(
            profile=list(range(1, 13)) + list(range(12, 0, -1)),
            profile_time_steps="1H",
            start_date=pd.Timestamp("1 January 2014 00:00:00"),
        ),
        seed=1234
    )

    # after the initialization, the 1h time delta of the profile should have
    # been aligned to the 15min of the clock
    assert timer_gen.profile.index.shape[0] == 24 * 4

    # the first index should be shifted to the time of the clock
    assert timer_gen.profile.index[0] == pd.Timestamp("10 June 2016 5:45pm")


def test_DefaultDailyTimerGenerator_should_be_initialized_correctly():

    clock = Clock(start=pd.Timestamp("12 Sept 2016"),
                  step_duration=pd.Timedelta("60 s"),
                  seed=1234)

    daily = DefaultDailyTimerGenerator(clock=clock, seed=1234)

    assert daily.profile.index[0] == pd.Timestamp("12 Sept 2016")


def test_cyclic_timer_profile_should_compute_duration_correct():

    tested = CyclicTimerProfile(
        profile=[10, 20, 10, 40],
        profile_time_steps="2h",
        start_date=pd.Timestamp("21 March 1956")
    )

    assert tested.duration() == pd.Timedelta("8h")


def test_activity_level_should_be_scaled_according_to_profile_duration():

    clock = Clock(start=pd.Timestamp("10 June 2016 5:45pm"),
                  # time steps by 15min
                  step_duration=pd.Timedelta("1 h"),
                  seed=1234)

    # 1 to 12 then 12 to 1, from midnight to midnight
    one_day_timer = CyclicTimerGenerator(
        clock=clock,
        config=CyclicTimerProfile(
            profile=list(range(24)),
            profile_time_steps="1H",
            start_date=pd.Timestamp("1 January 2014 00:00:00"),
        ),
        seed=1234
    )

    # 14 actions/week should be scaled to activity 2 since profile lasts 1 day
    assert 2 == one_day_timer.activity(n=14, per=pd.Timedelta("7 days"))

    # this one should generate a warning log since the corresponding freq
    # is shorter than the clock step
    assert 48 == one_day_timer.activity(n=4, per=pd.Timedelta("2h"))

    assert .5 == one_day_timer.activity(n=1, per=pd.Timedelta("2 days"))

    assert .5 == one_day_timer.activity(n=.25, per=pd.Timedelta("12h"))

    assert 1. / 360 - one_day_timer.activity(
        n=1, per=pd.Timedelta("360 days")) < 1e-10
