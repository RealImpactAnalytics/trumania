from __future__ import division
import logging
import pandas as pd
import patterns

from trumania.core.operations import FieldLogger, bound_value, bounded_sigmoid, Apply
from trumania.core.random_generators import SequencialGenerator, DependentTriggerGenerator, NumpyRandomGenerator
from trumania.components.time_patterns.profilers import DefaultDailyTimerGenerator
from trumania.core.clock import CyclicTimerGenerator, CyclicTimerProfile


def add_customers(circus, params):

    logging.info(" adding customers")
    customers = circus.create_actor(name="customers",
                                    size=params["n_customers"],
                                    ids_gen=SequencialGenerator(prefix="CUST_"))

    logging.info(" adding 'possible sites' mobility relationship to customers")

    mobility_rel = customers.create_relationship("POSSIBLE_SITES")

    # probability of each site to be chosen, based on geo_level1 population
    site_weight = circus.actors["sites"] \
        .get_attribute("GEO_LEVEL_1_POPULATION") \
        .get_values(None)

    customer_gen = NumpyRandomGenerator(method="choice",
                                        seed=next(circus.seeder),
                                        a=customers.ids,
                                        replace=False)

    site_gen = NumpyRandomGenerator(method="choice",
                                    seed=next(circus.seeder),
                                    a=circus.actors["sites"].ids,
                                    p=site_weight.values / sum(site_weight))

    mobility_weight_gen = NumpyRandomGenerator(
        method="exponential", scale=1., seed=next(circus.seeder))

    # Everybody gets at least one site
    mobility_rel.add_relations(
        from_ids=customers.ids,
        to_ids=site_gen.generate(customers.size),
        weights=mobility_weight_gen.generate(customers.size))

    # at each iteration, give a new site to a sample of people
    # the sample will be of proportion p
    p = 0.5

    # to get an average site per customer of mean_known_sites_per_customer,
    # if each iteration samples with p,
    # we need mean_known_sites_per_customer/p iterations
    #
    # we remove one iteration that already happened for everybody here above
    for i in range(int(params["mean_known_sites_per_customer"] / p) - 1):
        sample = customer_gen.generate(int(customers.size * p))
        mobility_rel.add_relations(
            from_ids=sample,
            to_ids=site_gen.generate(len(sample)),
            weights=mobility_weight_gen.generate(len(sample)))

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
        seed=next(circus.seeder),
        config=CyclicTimerProfile(
            profile=mov_prof,
            profile_time_steps="1H",
            start_date=pd.Timestamp("12 September 2016 00:00.00"),
        )
    )

    gaussian_activity = NumpyRandomGenerator(
        method="normal", loc=params["mean_daily_customer_mobility_activity"],
        scale=params["std_daily_customer_mobility_activity"],
        seed=next(circus.seeder))

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
        FieldLogger(log_id="customer_mobility_logs",
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
                                                        next(circus.seeder))

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
            seed=next(circus.seeder)).map(f=lambda per: 1 / per)

        low_stock_bulk_purchase_trigger = DependentTriggerGenerator(
            value_to_proba_mapper=bounded_sigmoid(
                x_min=1,
                x_max=description["max_pos_stock_triggering_pos_restock"],
                shape=description["restock_sigmoid_shape"],
                incrementing=False))

        item_price_gen = NumpyRandomGenerator(
            method="choice", a=description["item_prices"],
            seed=next(circus.seeder))

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

            sites.get_relationship("CELLS").ops.select_one(
                from_field="SITE",
                named_as="CELL_ID"),

            # injecting geo level 2 and distributor in purchase action:
            # this is only required for approximating targets of that
            # distributor
            sites.ops.lookup(
                actor_id_field="SITE",
                select={"GEO_LEVEL_2": "geo_level2_id",
                        "{}__dist_l1".format(product): "distributor_l1"}
            ),

            pos.get_relationship(product).ops.select_one(
                from_field="POS",
                named_as="INSTANCE_ID",

                pop=True,

                discard_empty=False),

            circus.actors[product].ops.select_one(named_as="PRODUCT_ID"),

            Apply(source_fields="INSTANCE_ID",
                  named_as="FAILED_SALE_OUT_OF_STOCK",
                  f=pd.isnull, f_args="series"),

            SequencialGenerator(prefix="TX_CUST_{}".format(product)).ops.generate(
                named_as="TX_ID"),

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
