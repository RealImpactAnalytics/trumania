from datagenerator.components.time_patterns.profilers import *


def test_init_cyclictimergenerator():
    """
    from a

    """

    # say we have a clock at 5.45pm on 10th June
    clock = Clock(start=pd.Timestamp("10 June 2016 5:45pm"),

                  # time steps by 15min
                  step_s=900,

                  seed=1234)

    # 1 to 12 then 12 to 1, from midnight to midnight
    timer_gen = CyclicTimerGenerator(
        clock=clock,

        profile=range(1, 13) + range(12, 0, -1),
        profile_time_steps="1H",
        start_date=pd.Timestamp("1 January 2014 00:00:00"),
        seed=1234
    )

    # after the initialization, the 1h time delta of the profile should have
    # been aligned to the 15min of the clock
    assert timer_gen.profile.index.shape[0] == 24*4

    # the first index should be shifted to the time of the clock
    assert timer_gen.profile.index[0] == pd.Timestamp("10 June 2016 5:45pm")


def test_DefaultDailyTimerGenerator_should_be_initialized_correctly():

    clock = Clock(start=pd.Timestamp("12 Sept 2016"),

                  # time steps by 15min
                  step_s=60,

                  seed=1234)

    daily = DefaultDailyTimerGenerator(clock=clock, seed=1234)

    assert daily.profile.index[0] == pd.Timestamp("12 Sept 2016")
