from __future__ import division
from datagenerator.core.clock import Clock
from datagenerator.core.actor import Actor
from datagenerator.core.circus import Circus
from datagenerator.components.time_patterns.profilers import DayProfiler
from datetime import datetime, timedelta
import pytest
import pandas as pd


def test_create_action_get_action_should_work_as_expected():

    customers = Actor(100)

    flying = Circus(master_seed=1, start=datetime(year=2016, month=6, day=8),
                    step_s=60, format_for_out="%d%m%Y %H:%M:%S")

    mov_prof = pd.Series(
        [1., 1., 1., 1., 1., 1., 1., 1., 5., 10., 5., 1., 1., 1., 1., 1., 1.,
         5., 10., 5., 1., 1., 1., 1.],
        index=[timedelta(hours=h, minutes=59, seconds=59) for h in range(24)])
    mobility_time_gen = DayProfiler(flying.clock, mov_prof, seed=1)

    mobility_action = flying.create_action(
        name="mobility",

        initiating_actor=customers,
        actorid_field="A_ID",

        timer_gen=mobility_time_gen,
    )

    # add and get action by name should work as expected
    result = flying.get_action("mobility")

    assert result.name == "mobility"
    assert result.actorid_field_name == mobility_action.actorid_field_name


def test_get_non_existing_action_should_return_None():

    flying = Circus(master_seed=1, start=datetime(year=2016, month=6, day=8),
                    step_s=60, format_for_out="%d%m%Y %H:%M:%S")

    assert flying.get_action("non_existing_name") is None


def test_adding_a_second_action_with_same_name_should_be_refused():

    flying = Circus(master_seed=1, start=datetime(year=2016, month=6, day=8),
                    step_s=60, format_for_out="%d%m%Y %H:%M:%S")

    flying.create_action(name="the_action",
                               initiating_actor=Actor(100),
                               actorid_field="actor_id")

    with pytest.raises(ValueError):
        flying.create_action(name="the_action",
                               initiating_actor=Actor(100),
                               actorid_field="actor_id")



