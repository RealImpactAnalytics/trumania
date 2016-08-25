from __future__ import division

from datetime import datetime

from datagenerator.action import *
from datagenerator.actor import *

from datagenerator.circus import *
from datagenerator.clock import *
from datagenerator.random_generators import *
from datagenerator.relationship import *


def test_add_action_get_action_should_work_as_expected():

    customers = Actor(100)

    the_clock = Clock(datetime(year=2016, month=6, day=8), 60,
                      "%d%m%Y %H:%M:%S", 1)

    flying = Circus(the_clock)

    mov_prof = pd.Series(
        [1., 1., 1., 1., 1., 1., 1., 1., 5., 10., 5., 1., 1., 1., 1., 1., 1.,
         5., 10., 5., 1., 1., 1., 1.],
        index=[timedelta(hours=h, minutes=59, seconds=59) for h in range(24)])
    mobility_time_gen = DayProfiler(the_clock, mov_prof, seed=1)

    mobility_action = Action(
        name="mobility",

        initiating_actor=customers,
        actorid_field="A_ID",

        timer_gen=mobility_time_gen,
    )

    # add and get action by name should work as expected
    flying.add_action(mobility_action)
    result = flying.get_action("mobility")

    assert result.name == "mobility"
    assert result.actorid_field_name == mobility_action.actorid_field_name
