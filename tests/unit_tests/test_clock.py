from datetime import datetime

from datagenerator.core import clock


def test_default_clock_should_have_0_day_and_week_index():

    # this is a monday
    start = datetime(year=2016, month=8, day=15)
    tested = clock.Clock(start, step_s=60, format_for_out="%d%m%Y %H:%M:%S",
                         seed=123)

    # without any increment, the clock should be at index 0
    assert 0 == tested.get_day_index()
    assert 0 == tested.get_week_index()


def test_default_clock_should_have_n_day_and_week_index_with_different_start():

    # this is a wednesday
    start = datetime(year=2016, month=8, day=17, hour=18, minute=21)
    tested = clock.Clock(start, step_s=60, format_for_out="%d%m%Y %H:%M:%S",
                         seed=123)

    # the index have been initialized correctly
    assert 21 + 18*60 == tested.get_day_index()
    assert 21 + 18*60 + 2 * 24 * 60 == tested.get_week_index()


def test_clock_should_have_day_and_week_index_n_after_n_increments():

    # this is a monday
    start = datetime(year=2016, month=8, day=15)
    tested = clock.Clock(start, step_s=60, format_for_out="%d%m%Y %H:%M:%S",
                         seed=123)

    # progressing by 45 minutes:
    for i in range(45):
        tested.increment()

    # both index should now be at 45 steps (of 1 minute), from 0
    assert 45 == tested.get_day_index()
    assert 45 == tested.get_week_index()

    # incrementing again
    for i in range(45):
        tested.increment()

    # both index should now be at 90 steps (of 1 minute), from 0
    assert 2*45 == tested.get_day_index()
    assert 2*45 == tested.get_week_index()


def test_large_number_of_increment_should_go_around_the_next_day():

    # this is a monday
    start = datetime(year=2016, month=8, day=15)

    tested = clock.Clock(start, step_s=60, format_for_out="%d%m%Y %H:%M:%S",
                         seed=123)

    # progressing by 25 hours:
    for i in range(25*60):
        tested.increment()

    # day index should be back to 60 minutes
    assert 60 == tested.get_day_index()

    # week index should be at the second day
    assert 25*60 == tested.get_week_index()


def test_very_large_number_of_increment_should_go_around_the_next_week():

    # this is a monday
    start = datetime(year=2016, month=8, day=15)

    tested = clock.Clock(start, step_s=60, format_for_out="%d%m%Y %H:%M:%S",
                         seed=123)

    # progressing by 9 days and 3 hours:
    for i in range(24*9*60 + 3*60):
        tested.increment()

    # day index should be back to 180 minutes
    assert 3*60 == tested.get_day_index()

    # week index should at 2 days and 3 hours
    assert 2*24*60 + 3*60 == tested.get_week_index()
