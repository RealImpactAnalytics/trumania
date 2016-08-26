from __future__ import division

from datetime import datetime

from datagenerator.action import *
from datagenerator.actor import *
from datagenerator.circus import *
from datagenerator.clock import *
from datagenerator.util_functions import *


# AgentA: has stock of SIMs
# AgentB: has stock of SIMs
# SIMs: has ID
# AgentA buys stock to AgentB

params = {
    "n_agents": 1000,
    "n_dealers": 100,
    "average_degree": 20,
    "n_sims": 500000
}


def create_agents_with_sims(seeder):
    """
    Create the AGENT actor (i.e. customer) together with its "SIM" labeled
    stock, to keep track of which SIMs are own by which agent
    """
    logging.info("Creating agents ")

    agents = Actor(size=params["n_agents"], prefix="AGENT_", max_length=3)
    agents.create_relationship(name="SIM", seed=seeder.next())

    # note: the SIM multi-attribute is not initialized with any SIM: agents
    # start with no SIM

    return agents


def create_dealers_with_sims(seeder):
    """
    Create the DEALER actor together with its "SIM" labeled stock, to keep
     track of which SIMs are available at which agents
    """
    logging.info("Creating dealer and their SIM stock  ")

    dealers = Actor(size=params["n_dealers"], prefix="DEALER_", max_length=3)

    sims = dealers.create_relationship(name="SIM", seed=seeder.next())
    sim_ids = ["SIM_%s" % (str(i).zfill(6)) for i in range(params["n_sims"])]
    sims_dealer = make_random_assign("SIM", "DEALER",
                                     sim_ids, dealers.ids,
                                     seed=seeder.next())

    sims.add_relations(from_ids=sims_dealer["DEALER"],
                       to_ids=sims_dealer["SIM"])

    return dealers


def connect_agent_to_dealer(agents, dealers, seeder):
    """
    Creates a random relationship from agents to dealers
    """
    logging.info("Randomly connecting agents to dealer ")

    deg_prob = params["average_degree"] / params["n_agents"] * params["n_dealers"]

    agent_weight_gen = NumpyRandomGenerator(method="exponential", scale=1.)

    agent_customer_df = pd.DataFrame.from_records(
        make_random_bipartite_data(agents.ids, dealers.ids, deg_prob,
                                   seed=seeder.next()),
        columns=["AGENT", "DEALER"])

    agent_customer_rel = agents.create_relationship(name="DEALERS",
                                                    seed=seeder.next())

    agent_customer_rel.add_relations(
        from_ids=agent_customer_df["AGENT"],
        to_ids=agent_customer_df["DEALER"],
        weights=agent_weight_gen.generate(agent_customer_df.shape[0]))


def add_purchase_action(circus, agents, dealers, seeder):
    """
    Adds a SIM purchase action from agents to dealer, with impact on stock of
    both actors
    """
    logging.info("Creating purchase action")

    timegen = WeekProfiler(clock=circus.clock,
                           week_profile=[5., 5., 5., 5., 5., 3., 3.],
                           seed=seeder.next())

    purchase_activity_gen = NumpyRandomGenerator(
        method="choice", a=range(1, 4), seed=seeder.next())

    # TODO: if we merge profiler and generator, we could have higher probs here
    # based on calendar
    # TODO2: or not, maybe we should have a sub-operation with its own counter
    #  to "come back to normal", instead of sampling a random variable at
    #  each turn => would improve efficiency

    purchase = Action(
        name="purchases",
        initiating_actor=agents,
        actorid_field="AGENT",
        timer_gen=timegen,
        activity_gen=purchase_activity_gen,

        states={
            "on_holiday": {
                "activity": ConstantGenerator(value=0),
                "back_to_default_probability": ConstantGenerator(value=0)
            }
        }
    )

    purchase.set_operations(
        circus.clock.ops.timestamp(named_as="DATETIME"),

        agents.get_relationship("DEALERS").ops.select_one(
            from_field="AGENT",
            named_as="DEALER"
        ),

        dealers.get_relationship("SIM").ops.select_one(
            from_field="DEALER",
            named_as="SOLD_SIM",
            one_to_one=True,
            drop=True),

        agents.get_relationship("SIM").ops.add(
            from_field="AGENT",
            item_field="SOLD_SIM"),

        # not specifying the logged columns => by defaults, log everything
        operations.FieldLogger(log_id="purchases"),
    )

    circus.add_action(purchase)


def add_agent_holidays_action(circus, agents, seeder):
    """
    Adds actions that reset to 0 the activity level of the purchases action of
    some actors
    """
    logging.info("Adding 'holiday' periods for agents ")

    # TODO: this is a bit weird, I think what I'd need is a profiler that would
    # return duration (i.e timer count) with probability related to time
    # until next typical holidays :)
    # We could call this YearProfile though the internal mechanics would be
    # different than week and day profiler
    holiday_time_gen = WeekProfiler(clock=circus.clock,
                                    week_profile=[1, 1, 1, 1, 1, 1, 1],
                                    seed=seeder.next())

    # TODO: we'd obviously have to adapt those weitgh to longer periods
    # thought this interface is not very intuitive
    # => create a method where can can specify the expected value of the
    # inter-event interval, and convert that into an activity
    holiday_start_activity = ScaledParetoGenerator(m=.25, a=1.2,
                                                   seed=seeder.next())

    holiday_end_activity = ScaledParetoGenerator(m=150, a=1.2,
                                                 seed=seeder.next())

    going_on_holidays = Action(
        name="agent_start_holidays",
        initiating_actor=agents,
        actorid_field="AGENT",
        timer_gen=holiday_time_gen,
        activity_gen=holiday_start_activity)

    returning_from_holidays = Action(
        name="agent_start_holidays",
        initiating_actor=agents,
        actorid_field="AGENT",
        timer_gen=holiday_time_gen,
        activity_gen=holiday_end_activity,
        auto_reset_timer=False)

    going_on_holidays.set_operations(

        circus.get_action("purchases").ops.transit_to_state(
            actor_id_field="AGENT",
            state="on_holiday"),
        returning_from_holidays.ops.reset_timers(actor_id_field="AGENT"),

        # just for the logs
        circus.clock.ops.timestamp(named_as="TIME"),
        ConstantGenerator(value="going").ops.generate(named_as="STATES"),
        operations.FieldLogger(log_id="holidays"),
    )

    returning_from_holidays.set_operations(
        circus.get_action("purchases").ops.transit_to_state(
            actor_id_field="AGENT",
            state="default"),

        # just for the logs
        circus.clock.ops.timestamp(named_as="TIME"),
        ConstantGenerator(value="returning").ops.generate(named_as="STATES"),
        operations.FieldLogger(log_id="holidays"),
    )

    circus.add_actions(going_on_holidays, returning_from_holidays)


def test_snd_scenario():

    setup_logging()
    seeder = seed_provider(master_seed=123456)

    the_clock = Clock(datetime(year=2016, month=6, day=8), step_s=60,
                      format_for_out="%d%m%Y %H:%M:%S", seed=seeder.next())

    flying = Circus(the_clock)

    agents = create_agents_with_sims(seeder)
    dealers = create_dealers_with_sims(seeder)
    connect_agent_to_dealer(agents, dealers, seeder)

    add_purchase_action(flying, agents, dealers, seeder)
    add_agent_holidays_action(flying, agents, seeder)

    logs = flying.run(n_iterations=100)

    for logid, lg in logs.iteritems():
        logging.info(
            " - some {}:\n{}\n\n".format(logid, lg.head(15).to_string()))

