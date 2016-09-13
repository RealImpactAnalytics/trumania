from __future__ import division
from datagenerator.core.actor import Actor
from datagenerator.core.circus import Circus
from datagenerator.components.time_patterns.profilers import *
from datetime import datetime, timedelta
import pytest
from datagenerator.core.random_generators import *


def test_create_action_get_action_should_work_as_expected():

    customers = Actor(size=100,
                      ids_gen=SequencialGenerator(prefix="a"))

    flying = Circus(master_seed=1,
                    start=pd.Timestamp("8 June 2016"),
                    step_s=60)

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

    flying = Circus(master_seed=1,
                    start=pd.Timestamp("8 June 2016"),
                    step_s=60)

    assert flying.get_action("non_existing_name") is None


def test_adding_a_second_action_with_same_name_should_be_refused():

    flying = Circus(master_seed=1,
                    start=pd.Timestamp("8 June 2016"),
                    step_s=60)

    customers = Actor(size=100,
                      ids_gen=SequencialGenerator(prefix="a"))

    flying.create_action(name="the_action",
                               initiating_actor=customers,
                               actorid_field="actor_id")

    with pytest.raises(ValueError):
        flying.create_action(name="the_action",
                               initiating_actor=customers,
                               actorid_field="actor_id")



