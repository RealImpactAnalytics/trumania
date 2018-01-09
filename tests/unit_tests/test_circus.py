from __future__ import division
import pytest
import pandas as pd

from trumania.core.random_generators import SequencialGenerator
from trumania.core.circus import Circus
from trumania.components.time_patterns.profilers import DefaultDailyTimerGenerator


def test_create_action_get_action_should_work_as_expected():

    flying = Circus(name="tested_circus",
                    master_seed=1,
                    start=pd.Timestamp("8 June 2016"),
                    step_duration=pd.Timedelta("60s"))

    customers = flying.create_actor(
        "teste", size=100,
        ids_gen=SequencialGenerator(prefix="a"))

    mobility_time_gen = DefaultDailyTimerGenerator(flying.clock, seed=1)

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


def test_get_non_existing_action_should_return_none():

    flying = Circus(name="tested_circus",
                    master_seed=1,
                    start=pd.Timestamp("8 June 2016"),
                    step_duration=pd.Timedelta("60s"))

    assert flying.get_action("non_existing_name") is None


def test_adding_a_second_action_with_same_name_should_be_refused():

    flying = Circus(name="tested_circus",
                    master_seed=1,
                    start=pd.Timestamp("8 June 2016"),
                    step_duration=pd.Timedelta("60s"))

    customers = flying.create_actor(
        name="tested", size=100,
        ids_gen=SequencialGenerator(prefix="a"))

    flying.create_action(name="the_action",
                         initiating_actor=customers,
                         actorid_field="actor_id")

    with pytest.raises(ValueError):
        flying.create_action(name="the_action",
                             initiating_actor=customers,
                             actorid_field="actor_id")
