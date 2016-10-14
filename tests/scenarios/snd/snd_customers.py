from __future__ import division
from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
import patterns
import pandas as pd


def add_customers(circus, params):

    logging.info(" adding customers")
    customers = circus.create_actor(name="customers",
                                    size=params["n_customers"],
                                    ids_gen=SequencialGenerator(prefix="CUST_"))

    logging.info(" adding 'possible sites' mobility relationship to customers")

    mobility_rel = customers.create_relationship(
        "POSSIBLE_SITES",
        seed=circus.seeder.next())

    sites = circus.actors["sites"]

    mobility_df = pd.DataFrame.from_records(
        make_random_bipartite_data(
            customers.ids,
            sites.ids,
            p=params["mean_known_sites_per_customer"]/sites.size,
            seed=circus.seeder.next()),
        columns=["CID", "SID"])

    mobility_weight_gen = NumpyRandomGenerator(
        method="exponential", scale=1., seed=circus.seeder.next())

    mobility_rel.add_relations(
        from_ids=mobility_df["CID"],
        to_ids=mobility_df["SID"],
        weights=mobility_weight_gen.generate(mobility_df.shape[0]))

    logging.info(" assigning a first random site to each customer")
    customers.create_attribute(name="CURRENT_SITE",
                               init_relationship="POSSIBLE_SITES")

    return customers


def add_mobility_action(circus, params):

    logging.info(" creating customer mobility action")
    mov_prof = [1., 1., 1., 1., 1., 1., 1., 1., 5., 10., 5., 1., 1., 1., 1.,
                1., 1., 5., 10., 5., 1., 1., 1., 1.]
    mobility_time_gen = CyclicTimerGenerator(
        clock=circus.clock,
        seed=circus.seeder.next(),
        config=CyclicTimerProfile(
            profile=mov_prof,
            profile_time_steps="1H",
            start_date=pd.Timestamp("12 September 2016 00:00.00"),
        )
    )

    gaussian_activity = NumpyRandomGenerator(
        method="normal", loc=params["mean_daily_customer_mobility_activity"],
        scale=params["std_daily_customer_mobility_activity"],
        seed=circus.seeder.next())

    mobility_activity_gen = gaussian_activity.map(f=bound_value(lb=.5))

    mobility_action = circus.create_action(
        name="customer_mobility",

        initiating_actor=circus.actors["customers"],
        actorid_field="CUST_ID",

        timer_gen=mobility_time_gen,
        activity_gen=mobility_activity_gen
    )

    logging.info(" adding operations")

    mobility_action.set_operations(
        circus.actors["customers"].ops.lookup(
            actor_id_field="CUST_ID",
            select={"CURRENT_SITE": "PREV_SITE"}),

        # selects a destination site (or maybe the same as current... ^^)

        circus.actors["customers"] \
            .get_relationship("POSSIBLE_SITES") \
            .ops.select_one(from_field="CUST_ID", named_as="NEW_SITE"),

        # update the SITE attribute of the customers accordingly
        circus.actors["customers"] \
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


def add_purchase_actions(circus, params):

    customers = circus.actors["customers"]
    pos = circus.actors["pos"]
    sites = circus.actors["sites"]

    for product, description in params["products"].items():

        logging.info("creating customer {} purchase action".format(product))
        purchase_timer_gen = DefaultDailyTimerGenerator(circus.clock,
                                                        circus.seeder.next())

        max_activity = purchase_timer_gen.activity(
            n_actions=1,
            per=pd.Timedelta(
            days=description["customer_purchase_min_period_days"]))

        min_activity = purchase_timer_gen.activity(
            n_actions=1,
            per=pd.Timedelta(
            days=description["customer_purchase_max_period_days"]))

        purchase_activity_gen = NumpyRandomGenerator(
            method="uniform",
            low=1 / max_activity,
            high=1 / min_activity,
            seed=circus.seeder.next()).map(f=lambda per: 1 / per)

        low_stock_bulk_purchase_trigger = DependentTriggerGenerator(
            value_to_proba_mapper=operations.bounded_sigmoid(
                x_min=1,
                x_max=description["max_pos_stock_triggering_pos_restock"],
                shape=description["restock_sigmoid_shape"],
                incrementing=False)
        )

        item_price_gen = NumpyRandomGenerator(
            method="choice", a=description["item_prices"],
            seed=circus.seeder.next())

        action_name = "customer_{}_purchase".format(product)
        purchase_action = circus.create_action(
            name=action_name,
            initiating_actor=customers,
            actorid_field="CUST_ID",
            timer_gen=purchase_timer_gen,
            activity_gen=purchase_activity_gen)

        purchase_action.set_operations(

            customers.ops.lookup(
                actor_id_field="CUST_ID",
                select={"CURRENT_SITE": "SITE"}),

            sites.get_relationship("POS").ops.select_one(
                from_field="SITE",
                named_as="POS",

                weight=pos.get_attribute_values("ATTRACTIVENESS"),

                # TODO: this means customer in a location without POS do not buy
                # anything => we could add a re-try mechanism here
                discard_empty=True),

            pos.get_relationship(product).ops.select_one(
                from_field="POS",
                named_as="BOUGHT_ITEM",

                pop=True,

                discard_empty=False),

            operations.Apply(source_fields="BOUGHT_ITEM",
                             named_as="FAILED_SALE_OUT_OF_STOCK",
                             f=pd.isnull, f_args="series"),

            sites.get_relationship("CELLS").ops.select_one(
                from_field="SITE",
                named_as="CELL", ),

            SequencialGenerator(prefix="TX_").ops.generate(named_as="TX_ID"),

            item_price_gen.ops.generate(named_as="VALUE"),

            circus.clock.ops.timestamp(named_as="TIME"),

            FieldLogger(log_id=action_name),

            patterns.trigger_action_if_low_stock(
                circus,
                stock_relationship=pos.get_relationship(product),
                actor_id_field="POS",
                restock_trigger=low_stock_bulk_purchase_trigger,
                triggered_action_name="pos_{}_bulk_purchase".format(product)
            ),
        )
