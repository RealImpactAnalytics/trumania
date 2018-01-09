import pandas as pd
import logging
import numpy as np

from trumania.core.util_functions import make_random_bipartite_data
from trumania.core.random_generators import SequencialGenerator, NumpyRandomGenerator, ConstantGenerator
from trumania.core.operations import FieldLogger, bound_value
from trumania.components.time_patterns.profilers import WorkHoursTimerGenerator


def create_field_agents(circus, params):

    logging.info(" adding {} field agents".format(params["n_field_agents"]))

    field_agents = circus.create_actor(name="field_agents", size=params["n_field_agents"],
                                       ids_gen=SequencialGenerator(prefix="FA_"))

    logging.info(" adding mobility relationships to field agents")

    mobility_rel = field_agents.create_relationship(
        "POSSIBLE_SITES")

    # TODO: make sure the number of sites per field agent is "reasonable"
    mobility_df = pd.DataFrame.from_records(
        make_random_bipartite_data(field_agents.ids,
                                   circus.actors["sites"].ids,
                                   0.4,
                                   seed=next(circus.seeder)),
        columns=["FA_ID", "SID"])

    mobility_weight_gen = NumpyRandomGenerator(
        method="exponential", scale=1., seed=next(circus.seeder))

    mobility_rel.add_relations(
        from_ids=mobility_df["FA_ID"],
        to_ids=mobility_df["SID"],
        weights=mobility_weight_gen.generate(mobility_df.shape[0]))

    # Initialize the mobility by allocating one first random site to each
    # field agent among its network
    field_agents.create_attribute(name="CURRENT_SITE",
                                  init_relationship="POSSIBLE_SITES")

    return field_agents


def add_mobility_action(circus, params):

    logging.info(" creating field agent mobility action")

    # Field agents move only during the work hours
    mobility_time_gen = WorkHoursTimerGenerator(clock=circus.clock,
                                                seed=next(circus.seeder))

    fa_mean_weekly_activity = mobility_time_gen.activity(
        n_actions=params["mean_daily_fa_mobility_activity"],
        per=pd.Timedelta("1day"))

    fa_weekly_std = mobility_time_gen.activity(
        n_actions=params["std_daily_fa_mobility_activity"],
        per=pd.Timedelta("1day"))

    gaussian_activity = NumpyRandomGenerator(
        method="normal", loc=fa_mean_weekly_activity, scale=fa_weekly_std,
        seed=next(circus.seeder))

    mobility_activity_gen = gaussian_activity.map(f=bound_value(lb=1))

    field_agents = circus.actors["field_agents"]

    mobility_action = circus.create_action(
        name="field_agent_mobility",

        initiating_actor=field_agents,
        actorid_field="FA_ID",

        timer_gen=mobility_time_gen,
        activity_gen=mobility_activity_gen)

    logging.info(" adding operations")

    mobility_action.set_operations(
        field_agents.ops.lookup(
            actor_id_field="FA_ID",
            select={"CURRENT_SITE": "PREV_SITE"}),

        # selects a destination site (or maybe the same as current... ^^)

        field_agents \
            .get_relationship("POSSIBLE_SITES") \
            .ops.select_one(from_field="FA_ID", named_as="NEW_SITE"),

        # update the SITE attribute of the field agents accordingly
        field_agents \
            .get_attribute("CURRENT_SITE") \
            .ops.update(
                actor_id_field="FA_ID",
                copy_from_field="NEW_SITE"),

        circus.clock.ops.timestamp(named_as="TIME"),

        # create mobility logs
        FieldLogger(log_id="field_agent_mobility_logs",
                    cols=["TIME", "FA_ID", "PREV_SITE",
                          "NEW_SITE"]),
    )


def add_survey_action(circus):

    logging.info(" creating field agent survey action")

    field_agents = circus.actors["field_agents"]

    # Surveys only happen during work hours
    survey_timer_gen = WorkHoursTimerGenerator(clock=circus.clock,
                                               seed=next(circus.seeder))

    min_activity = survey_timer_gen.activity(
        n_actions=10, per=pd.Timedelta("7 days"),)
    max_activity = survey_timer_gen.activity(
        n_actions=100, per=pd.Timedelta("7 days"),)

    survey_activity_gen = NumpyRandomGenerator(
        method="choice", a=np.arange(min_activity, max_activity),
        seed=next(circus.seeder))

    survey_action = circus.create_action(
        name="pos_surveys",
        initiating_actor=field_agents,
        actorid_field="FA_ID",
        timer_gen=survey_timer_gen,
        activity_gen=survey_activity_gen
    )

    survey_action.set_operations(

        field_agents.ops.lookup(
            actor_id_field="FA_ID",
            select={"CURRENT_SITE": "SITE"}
        ),

        # TODO: We should select a POS irrespectively of the relationship weight
        circus.actors["sites"].get_relationship("POS").ops.select_one(
            from_field="SITE",
            named_as="POS_ID",

            # a field agent in a location without a POS won't serve any
            discard_empty=True
        ),

        circus.actors["pos"].ops.lookup(
            actor_id_field="POS_ID",
            select={
                "LATITUDE": "POS_LATITUDE",
                "LONGITUDE": "POS_LONGITUDE",
                "AGENT_NAME": "POS_NAME",
            }
        ),

        SequencialGenerator(prefix="TASK").ops.generate(named_as="TASK_ID"),
        ConstantGenerator(value="Done").ops.generate(named_as="STATUS"),

        circus.clock.ops.timestamp(named_as="TIME"),

        FieldLogger(log_id="pos_surveys",
                    cols=["TASK_ID", "FA_ID", "POS_ID", "POS_NAME",
                          "POS_LATITUDE", "POS_LONGITUDE", "TIME", "STATUS"])
    )
