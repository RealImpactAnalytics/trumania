from __future__ import division

from datetime import datetime

from datagenerator.actor import *
from datagenerator.circus import *
from datagenerator.clock import *
from datagenerator.util_functions import *


# AgentA: has stock of SIMs
# AgentB: has stock of SIMs
# SIMs: has ID
# AgentA buys stock to AgentB

params = {
    "n_agents": 5000,

    # very low number of dealer, to have tons of collision and validate the
    # one-to-one behaviour in that case
    "n_dealers": 10,
    "n_distributors": 3,

    "average_agent_degree": 20,

    "n_init_sims_dealer": 1000,
    "n_init_sims_distributor": 500000
}


def create_agents(seeder):
    """
    Create the AGENT actor (i.e. customer) together with its "SIM" labeled
    stock, to keep track of which SIMs are own by which agent
    """
    logging.info("Creating agents ")

    agents = Actor(size=params["n_agents"], prefix="AGENT_", max_length=3)
    agents.create_relationship(name="SIM", seed=seeder.next())

    agents.create_attribute(name="AGENT_NAME",
                            init_gen=FakerGenerator(seed=seeder.next(), method="name"))

    # note: the SIM multi-attribute is not initialized with any SIM: agents
    # start with no SIM

    return agents


def create_dealers_and_sims_stock(seeder):
    """
    Create the DEALER actor together with their init SIM stock
    """
    logging.info("Creating dealer and their SIM stock  ")

    dealers = Actor(size=params["n_dealers"], prefix="DEALER_", max_length=3)

    # SIM relationship to maintain some stock
    sims = dealers.create_relationship(name="SIM", seed=seeder.next())
    sim_ids = build_ids(size=params["n_init_sims_dealer"], prefix="SIM_")
    sims_dealer = make_random_assign(owned=sim_ids, owners=dealers.ids,
                                     seed=seeder.next())
    sims.add_relations(from_ids=sims_dealer["from"], to_ids=sims_dealer["to"])

    # one more dealer with just 3 sims in stock => this one will trigger
    # lot's of failed sales
    broken_dealer = pd.DataFrame({
        "DEALER": "broke_dealer",
        "SIM": ["SIM_OF_BROKE_DEALER_%d" % s for s in range(3)]
    })

    sims.add_relations(from_ids=broken_dealer["DEALER"],
                       to_ids=broken_dealer["SIM"])

    return dealers


def create_distributors_with_sims(seeder):
    """
    Distributors are similar to dealers, just with much more sims, and the
    actions to buy SIM from them is "by bulks" (though the action is not
    created here)
    """
    logging.info("Creating distributors and their SIM stock  ")

    distributors = Actor(size=params["n_distributors"], prefix="DISTRIBUTOR_",
                         max_length=1)

    sims = distributors.create_relationship(name="SIM", seed=seeder.next())

    sim_generator = SequencialGenerator(prefix="SIM_", max_length=30)
    sim_ids = sim_generator.generate(params["n_init_sims_distributor"])
    sims_dist = make_random_assign(owned=sim_ids, owners=distributors.ids,
                                   seed=seeder.next())
    sims.add_relations(from_ids=sims_dist["from"], to_ids=sims_dist["to"])

    # this tells to the "restock" action of the distributor how many
    # sim should be re-generated to replenish the stocks
    distributors.create_attribute("SIMS_TO_RESTOCK", init_values=0)

    return distributors, sim_generator


def connect_agent_to_dealer(agents, dealers, seeder):
    """
    Relationship from agents to dealers
    """
    logging.info("Randomly connecting agents to dealer ")

    deg_prob = params["average_agent_degree"] / params["n_agents"] * params["n_dealers"]

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

    # every agent is also connected to the "broke dealer", to make sure this
    # one gets out of stock quickly
    agent_customer_rel.add_relations(
        from_ids=agents.ids,
        to_ids="broke_dealer",
        weights=4)


def connect_dealers_to_distributors(dealers, distributors, seeder):

    # let's be simple: each dealer has only one provider
    distributor_rel = dealers.create_relationship("DISTRIBUTOR",
                                                  seed=seeder.next())

    state = np.random.RandomState(seeder.next())
    assigned = state.choice(a=distributors.ids, size=dealers.size, replace=True)

    distributor_rel.add_relations(from_ids=dealers.ids, to_ids=assigned)

    # We're also adding a "bulk buy size" attribute to each dealer that defines
    # how many SIM are bought at one from the distributor.
    bulk_gen = ParetoGenerator(xmin=500, a=1.5, force_int=True,
                               seed=seeder.next())

    dealers.create_attribute("BULK_BUY_SIZE", init_gen=bulk_gen)


def add_distributor_recharge_action(circus, distributors, sim_generator):
    """
    adds an action that increases the stock the distributor. This is triggered externaly by
    the bulk purchase action below
    """

    restocking = circus.create_action(
        name="distributor_restock",
        initiating_actor=distributors,
        actorid_field="DISTRIBUTOR_ID",

        # here again: no activity gen nor time profile here since the action
        # is triggered externally
    )

    restocking.set_operations(
        circus.clock.ops.timestamp(named_as="DATETIME"),

        distributors.ops.lookup(
            actor_id_field="DISTRIBUTOR_ID",
            select={"SIMS_TO_RESTOCK": "SIMS_TO_RESTOCK"}),

        sim_generator.ops.generate(
            named_as="NEW_SIMS_IDS",
            quantity_field="SIMS_TO_RESTOCK",),

        distributors.get_relationship("SIM").ops.add_grouped(
            from_field="DISTRIBUTOR_ID",
            grouped_items_field="NEW_SIMS_IDS"),

        # back to zero
        distributors.get_attribute("SIMS_TO_RESTOCK").ops.subtract(
            actor_id_field="DISTRIBUTOR_ID",
            subtracted_value_field="SIMS_TO_RESTOCK"),

        operations.FieldLogger(log_id="distributor_restock",
                               cols=["DATETIME", "DISTRIBUTOR_ID", "SIMS_TO_RESTOCK"]),

    )


def add_dealer_bulk_sim_purchase_action(circus, dealers, distributors, seeder):
    """
    Adds a SIM purchase action from agents to dealer, with impact on stock of
    both actors
    """
    logging.info("Creating purchase action")

    timegen = WeekProfiler(clock=circus.clock,
                           week_profile=[5., 5., 5., 5., 5., 3., 3.],
                           seed=seeder.next())

    purchase_activity_gen = ConstantGenerator(value=100)

    build_purchases = circus.create_action(
        name="bulk_purchases",
        initiating_actor=dealers,
        actorid_field="DEALER_ID",
        timer_gen=timegen,
        activity_gen=purchase_activity_gen)

    build_purchases.set_operations(
        circus.clock.ops.timestamp(named_as="DATETIME"),

        dealers.get_relationship("DISTRIBUTOR").ops.select_one(
            from_field="DEALER_ID",
            named_as="DISTRIBUTOR"),

        dealers.ops.lookup(actor_id_field="DEALER_ID",
                           select={"BULK_BUY_SIZE": "BULK_BUY_SIZE"}),

        distributors.get_relationship("SIM").ops.select_many(
            from_field="DISTRIBUTOR",
            named_as="SIM_BULK",
            quantity_field="BULK_BUY_SIZE",

            # if a SIM is selected, it is removed from the dealer's stock
            pop=True

            # (not modeling out-of-stock provider to keep the example simple...
        ),

        dealers.get_relationship("SIM").ops.add_grouped(
            from_field="DEALER_ID",
            grouped_items_field="SIM_BULK"),

        # not modeling money transfer to keep the example simple...

        # just logging the number of sims instead of the sims themselves...
        operations.Apply(source_fields="SIM_BULK",
                         named_as="NUMBER_OF_SIMS",
                         f=lambda s: s.map(len), f_args="series"),


        # finally, triggering some re-stocking by the distributor
        distributors.get_attribute("SIMS_TO_RESTOCK").ops.add(
            actor_id_field="DISTRIBUTOR",
            added_value_field="NUMBER_OF_SIMS"),

        circus.get_action("distributor_restock").ops.force_act_next(
            actor_id_field="DISTRIBUTOR"),

        operations.FieldLogger(log_id="bulk_purchases",
                               cols=["DEALER_ID", "DISTRIBUTOR",
                                     "NUMBER_OF_SIMS"]),
    )


def add_agent_sim_purchase_action(circus, agents, dealers, seeder):
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

    purchase = circus.create_action(
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
            named_as="DEALER"),

        dealers.get_relationship("SIM").ops.select_one(
            from_field="DEALER",
            named_as="SOLD_SIM",

            # each SIM can only be sold once
            one_to_one=True,

            # if a SIM is selected, it is removed from the dealer's stock
            pop=True,

            # If a chosen dealer has empty stock, we don't want to drop the
            # row in action_data, but keep it with a None sold SIM,
            # which indicates the sale failed
            discard_empty=False),

        operations.Apply(source_fields="SOLD_SIM",
                         named_as="FAILED_SALE",
                         f=pd.isnull, f_args="series"),

        # any agent who failed to buy a SIM will try again at next round
        # (we could do that probabilistically as well, just add a trigger...)
        purchase.ops.force_act_next(actor_id_field="AGENT",
                                    condition_field="FAILED_SALE"),

        # not specifying the logged columns => by defaults, log everything
        # ALso, we log the sale before dropping to failed sales, to keep
        operations.FieldLogger(log_id="purchases"),

        # only successful sales actually add a SIM to agents
        operations.DropRow(condition_field="FAILED_SALE"),

        agents.get_relationship("SIM").ops.add(
            from_field="AGENT",
            item_field="SOLD_SIM"),
    )


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

    # TODO: we'd obviously have to adapt those weight to longer periods
    # thought this interface is not very intuitive
    # => create a method where can can specify the expected value of the
    # inter-event interval, and convert that into an activity
    holiday_start_activity = ParetoGenerator(xmin=.25, a=1.2,
                                             seed=seeder.next())

    holiday_end_activity = ParetoGenerator(xmin=150, a=1.2,
                                           seed=seeder.next())

    going_on_holidays = circus.create_action(
        name="agent_start_holidays",
        initiating_actor=agents,
        actorid_field="AGENT",
        timer_gen=holiday_time_gen,
        activity_gen=holiday_start_activity)

    returning_from_holidays = circus.create_action(
        name="agent_stops_holidays",
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


def add_agent_reviews_actions(circus, agents, seeder):
    """
    This illustrates the dynamic creation of new actors: reviews are modeled as "actor"
     (even though they are mostly inactive data container) that are created dynamically
     and linked to agents.

    I guess most of the time reviews would be modeled as logs instead of actors, but
     let's just ignore that for illustration purposes... ^^
    """

    timegen = WeekProfiler(clock=circus.clock,
                           week_profile=[5., 5., 5., 5., 5., 3., 3.],
                           seed=seeder.next())

    review_activity_gen = NumpyRandomGenerator(
        method="choice", a=range(1, 4), seed=seeder.next())

    # the system starts with no reviews
    review_actor = Actor(size=0)
    review_actor.create_attribute("DATE")
    review_actor.create_attribute("TEXT")
    review_actor.create_attribute("AGENT_ID")
    review_actor.create_attribute("AGENT_NAME")

    reviews = circus.create_action(
        name="agent_reviews",
        initiating_actor=agents,
        actorid_field="AGENT",
        timer_gen=timegen,
        activity_gen=review_activity_gen,
    )

    review_id_gen = SequencialGenerator(start=0, prefix="REVIEW_ID")
    text_id_gen = FakerGenerator(method="text", seed=seeder.next())

    reviews.set_operations(
        circus.clock.ops.timestamp(named_as="DATETIME"),

        agents.ops.lookup(actor_id_field="AGENT",
                          select={"AGENT_NAME": "AGENT_NAME"}),

        review_id_gen.ops.generate(named_as="REVIEW_ID"),

        text_id_gen.ops.generate(named_as="REVIEW_TEXT"),

        review_actor.ops.update(
            actor_id_field="REVIEW_ID",
            copy_attributes_from_fields={
                "DATE": "DATETIME",
                "TEXT": "REVIEW_TEXT",
                "AGENT_ID": "AGENT",
                "AGENT_NAME": "AGENT_NAME",
            }
        ),

        # actually, here we're modelling review both as actors and logs..
        operations.FieldLogger(log_id="reviews")
    )


def test_snd_scenario():

    setup_logging()
    seeder = seed_provider(master_seed=123456)

    the_clock = Clock(datetime(year=2016, month=6, day=8), step_s=60,
                      format_for_out="%d%m%Y %H:%M:%S", seed=seeder.next())

    flying = Circus(the_clock)

    distributors, sim_generator = create_distributors_with_sims(seeder)
    add_distributor_recharge_action(flying, distributors, sim_generator)
    dealers = create_dealers_and_sims_stock(seeder)
    agents = create_agents(seeder)
    connect_agent_to_dealer(agents, dealers, seeder)
    connect_dealers_to_distributors(dealers, distributors, seeder)

    add_agent_sim_purchase_action(flying, agents, dealers, seeder)
    add_dealer_bulk_sim_purchase_action(flying, dealers, distributors, seeder)
    add_agent_holidays_action(flying, agents, seeder)
    add_agent_reviews_actions(flying, agents, seeder)

    logs = flying.run(n_iterations=100)

    for logid, lg in logs.iteritems():
        log_dataframe_sample(" - some {}".format(logid), lg)

    purchases = logs["purchases"]
    log_dataframe_sample(" - some failed purchases: ",
                         purchases[purchases["FAILED_SALE"]])

    sales_of_broke = purchases[purchases["DEALER"] == "broke_dealer"]
    log_dataframe_sample(" - some purchases from broke dealer: ",
                         sales_of_broke)

    all_logs_size = np.sum(df.shape[0] for df in logs.values())
    logging.info("\ntotal number of logs: {}".format(all_logs_size))

    for logid, lg in logs.iteritems():
        logging.info(" {} {} logs".format(len(lg), logid))

    # broke dealer should have maximum 3 successful sales
    ok_sales_of_broke = sales_of_broke[~sales_of_broke["FAILED_SALE"]]
    assert ok_sales_of_broke.shape[0] <= 3


