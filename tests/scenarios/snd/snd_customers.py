import logging
from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
import pandas as pd


def create_customers(circus, params):

    logging.info(" adding customers")

    customers = Actor(size=params["n_customers"],
                      ids_gen=SequencialGenerator(prefix="CUST_"))

    logging.info(" adding mobility relationships to customers")

    mobility_rel = customers.create_relationship(
        "POSSIBLE_SITES",
        seed=circus.seeder.next())

    # TODO: make sure the number of sites per customer is "reasonable"
    mobility_df = pd.DataFrame.from_records(
        make_random_bipartite_data(customers.ids,
                                   circus.sites.ids,
                                   0.4,
                                   seed=circus.seeder.next()),
        columns=["CID", "SID"])

    mobility_weight_gen = NumpyRandomGenerator(
        method="exponential", scale=1., seed=circus.seeder.next())

    mobility_rel.add_relations(
        from_ids=mobility_df["CID"],
        to_ids=mobility_df["SID"],
        weights=mobility_weight_gen.generate(mobility_df.shape[0]))

    # Initialize the mobility by allocating one first random site to each
    # customer among its network
    customers.create_attribute(name="CURRENT_SITE",
                                    init_relationship="POSSIBLE_SITES")

    return customers


def add_mobility_action(circus):

    logging.info(" creating mobility action")

    # TODO: post-generation check: is the actual move set realistic?
    mov_prof = [1., 1., 1., 1., 1., 1., 1., 1., 5., 10., 5., 1., 1., 1., 1.,
                1., 1., 5., 10., 5., 1., 1., 1., 1.]
    mobility_time_gen = CyclicTimerGenerator(
        clock=circus.clock,
        profile=mov_prof,
        profile_time_steps="1H",
        start_date=pd.Timestamp("12 September 2016 00:00.00"),
        seed=circus.seeder.next())

    mobility_action = circus.create_action(
        name="customer_mobility",

        initiating_actor=circus.customers,
        actorid_field="CUST_ID",

        timer_gen=mobility_time_gen,
    )

    logging.info(" adding operations")

    mobility_action.set_operations(
        circus.customers.ops.lookup(
            actor_id_field="CUST_ID",
            select={"CURRENT_SITE": "PREV_SITE"}),

        # selects a destination site (or maybe the same as current... ^^)

        circus.customers \
            .get_relationship("POSSIBLE_SITES") \
            .ops.select_one(from_field="CUST_ID", named_as="NEW_SITE"),

        # update the SITE attribute of the customers accordingly
        circus.customers \
            .get_attribute("CURRENT_SITE") \
            .ops.update(
                actor_id_field="CUST_ID",
                copy_from_field="NEW_SITE"),

        circus.clock.ops.timestamp(named_as="TIME"),

        # create mobility logs
        operations.FieldLogger(log_id="customer_mobility_logs",
                               cols=["TIME", "CUST_ID", "PREV_SITE",
                                     "NEW_SITE"]),
    )


def add_purchase_sim_action(circus, params):

    purchase_timer_gen = DefaultDailyTimerGenerator(circus.clock,
                                                    circus.seeder.next())

    # between 1 to 6 SIM bought per year per customer
    purchase_activity_gen = NumpyRandomGenerator(
#        method="choice", a=np.arange(1, 6) / 360.,
        method="choice", a=np.arange(1, 6),
        seed=circus.seeder.next())

    purchase_action = circus.create_action(
        name="sim_purchases_to_pos",
        initiating_actor=circus.customers,
        actorid_field="CUST_ID",
        timer_gen=purchase_timer_gen,
        activity_gen=purchase_activity_gen
    )

    purchase_action.set_operations(

        circus.customers.ops.lookup(
            actor_id_field="CUST_ID",
            select={"CURRENT_SITE": "SITE"}
        ),

        circus.sites.get_relationship("POS").ops.select_one(
            from_field="SITE",
            named_as="POS",

            weight=circus.pos.get_attribute_values("ATTRACTIVENESS"),

            # TODO: this means customer in a location without POS do not buy
            # anything => we could add a re-try mechanism here
            discard_empty=True
        ),


        circus.pos.get_relationship("SIMS").ops.select_one(
            from_field="POS",
            named_as="SIM",

            pop=True,
            discard_empty=True
        ),

        circus.sites.get_relationship("CELLS").ops.select_one(
            from_field="SITE",
            named_as="CELL",
        ),

        SequencialGenerator(prefix="SIM_TX_")\
            .ops.generate(named_as="TX_ID"),

        ConstantGenerator(value=params["sim_price"])\
            .ops.generate(named_as="VALUE"),

        circus.clock.ops.timestamp(named_as="TIME"),

        FieldLogger(log_id="sim_purchases_to_pos")
    )







