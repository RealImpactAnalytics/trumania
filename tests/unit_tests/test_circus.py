from __future__ import division
import pytest
import pandas as pd

from trumania.core.random_generators import SequencialGenerator
from trumania.core.circus import Circus
from trumania.components.time_patterns.profilers import DefaultDailyTimerGenerator


def test_create_story_get_story_should_work_as_expected():

    flying = Circus(name="tested_circus",
                    master_seed=1,
                    start=pd.Timestamp("8 June 2016"),
                    step_duration=pd.Timedelta("60s"))

    customers = flying.create_population(
        "the_customers", size=100,
        ids_gen=SequencialGenerator(prefix="a"))

    mobility_time_gen = DefaultDailyTimerGenerator(flying.clock, seed=1)

    mobility_story = flying.create_story(
        name="mobility",

        initiating_population=customers,
        member_id_field="A_ID",

        timer_gen=mobility_time_gen,
    )

    # add and get story by name should work as expected
    result = flying.get_story("mobility")

    assert result.name == "mobility"
    assert result.member_id_field == mobility_story.member_id_field

    # also retrieving this initiating population of that population

    retrieved_pop = flying.get_population_of("mobility")

    assert retrieved_pop == customers


def test_get_non_existing_story_should_return_none():

    flying = Circus(name="tested_circus",
                    master_seed=1,
                    start=pd.Timestamp("8 June 2016"),
                    step_duration=pd.Timedelta("60s"))

    assert flying.get_story("non_existing_name") is None


def test_adding_a_second_story_with_same_name_should_be_refused():

    flying = Circus(name="tested_circus",
                    master_seed=1,
                    start=pd.Timestamp("8 June 2016"),
                    step_duration=pd.Timedelta("60s"))

    customers = flying.create_population(
        name="tested", size=100,
        ids_gen=SequencialGenerator(prefix="a"))

    flying.create_story(name="the_story",
                        initiating_population=customers,
                        member_id_field="population_id")

    with pytest.raises(ValueError):
        flying.create_story(name="the_story",
                            initiating_population=customers,
                            member_id_field="population_id")
