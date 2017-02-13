from datagenerator.core import circus
import datagenerator.core.random_generators as gen
import datagenerator.core.operations as ops
import datagenerator.components.time_patterns.profilers as profilers
import datagenerator.core.util_functions as util_functions

import pandas as pd

# each step?() function below implement one step of the second example of the
# tutorial documented at
# https://realimpactanalytics.atlassian.net/wiki/display/LM/Data+generator+tutorial


def build_circus():
    return circus.Circus(
        name="example2",
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))


def create_pos_actor(the_circus):
    """
    Creates a point of sale actor and attach it to the circus
    """
    pos = the_circus.create_actor(
        name="point_of_sale", size=100,
        ids_gen=gen.SequencialGenerator(prefix="POS_"))

    name_gen = gen.FakerGenerator(method="name", seed=the_circus.seeder.next())
    pos.create_attribute("NAME", init_gen=name_gen)

    city_gen = gen.FakerGenerator(method="city", seed=the_circus.seeder.next())
    pos.create_attribute("CITY", init_gen=city_gen)

    company_gen = gen.FakerGenerator(method="company",
                                     seed=the_circus.seeder.next())
    pos.create_attribute("COMPANY", init_gen=company_gen)

    pos.create_relationship(name="items")


def add_report_action(the_circus):
    """
    adds an operations that logs the stock level of each POS at the end of each
    day
    """

    pos = the_circus.actors["point_of_sale"]
    report_action = the_circus.create_action(
        name="report",
        initiating_actor=pos,
        actorid_field="POS_ID",

        timer_gen=gen.ConstantDependentGenerator(
            value=the_circus.clock.n_iterations(duration=pd.Timedelta("24h")) - 1)
    )

    report_action.set_operations(
        the_circus.clock.ops.timestamp(named_as="TIME", random=False,
                                     log_format="%Y-%m-%d"),

        pos.get_relationship("items").ops.get_neighbourhood_size(
            from_field="POS_ID",
            named_as="STOCK_LEVEL"),

        ops.FieldLogger(log_id="report", cols=["TIME", "POS_ID", "STOCK_LEVEL"])
    )


def create_customer_actor(the_circus):
    """
    Creates a customer actor and attach it to the circus
    """
    customer = the_circus.create_actor(
        name="customer", size=2500,
        ids_gen=gen.SequencialGenerator(prefix="CUS_"))

    customer.create_attribute(
        name="FIRST_NAME",
        init_gen=gen.FakerGenerator(method="first_name",
                                seed=the_circus.seeder.next()))
    customer.create_attribute(
        name="LAST_NAME",
        init_gen=gen.FakerGenerator(method="last_name",
                                seed=the_circus.seeder.next()))

    customer.create_relationship(name="my_items")


def add_items_to_pos_stock(the_circus):
    """
    Generates and add 5 items to the "items" relationship of each POS
    """
    pos = the_circus.actors["point_of_sale"]

    items_gen = gen.SequencialGenerator(prefix="ITEM_")
    the_circus.attach_generator("items_gen", items_gen)

    item_arrays = [items_gen.generate(5) for _ in pos.ids]
    pos.get_relationship("items").add_grouped_relations(
        from_ids=pos.ids,
        grouped_ids=item_arrays)


def add_periodic_restock_action(the_circus):
    """
    Adds a periodic POS restock action to the circus, having each POS
    systematically adding a random amount of items to their stock
    """
    pos = the_circus.actors["point_of_sale"]

    # using this timer means POS are more likely to trigger a re-stock during
    # day hours rather that at night.
    timer_gen = profilers.DefaultDailyTimerGenerator(
        clock=the_circus.clock, seed=the_circus.seeder.next())

    restock_action = the_circus.create_action(
            name="restock",
            initiating_actor=pos,
            actorid_field="POS_ID",

            timer_gen=timer_gen,

            # Using a ConstantGenerator here means each POS will have the same
            # activity level of exactly one action per day on average. Since
            # the time itself is random, period between 2 restocks will on
            # general not be exactly 24h
            activity_gen=gen.ConstantGenerator(value=timer_gen.activity(
                n_actions=1, per=pd.Timedelta("7 days")
            )),
        )

    stock_size_gen = gen.NumpyRandomGenerator(method="choice",
                                              a=[5, 15, 20, 25],
                                              p=[0.1, 0.2, 0.5, 0.2],
                                              seed=the_circus.seeder.next())

    item_bulk_gen = gen.DependentBulkGenerator(
        element_generator=the_circus.generators["items_gen"])

    restock_action.set_operations(
        the_circus.clock.ops.timestamp(named_as="TIME",
                                       log_format="%Y-%m-%d"),

        # include the POS NAME attribute as a field name "POS_NAME"
        pos.ops.lookup(actor_id_field="POS_ID", select={"NAME": "POS_NAME"}),

        stock_size_gen.ops.generate(named_as="RESTOCK_VOLUME"),

        item_bulk_gen.ops.generate(named_as="NEW_ITEM_IDS",
                                   observed_field="RESTOCK_VOLUME"),

        pos.get_relationship("items").ops.add_grouped(from_field="POS_ID",
                                           grouped_items_field="NEW_ITEM_IDS"),

        ops.FieldLogger(log_id="restock", cols=["TIME", "POS_ID", "POS_NAME",
                                                "RESTOCK_VOLUME"])
    )


def add_inactive_restock_action(the_circus):
    """
    This is a copy-paste of add_periodic_restock_action(), but without the
    timer nor the activity levels => as-is, this action never triggers
    """
    pos = the_circus.actors["point_of_sale"]

    restock_action = the_circus.create_action(
            name="restock",
            initiating_actor=pos,
            actorid_field="POS_ID")

    stock_size_gen = gen.NumpyRandomGenerator(method="choice",
                                              a=[5, 15, 20, 25],
                                              p=[0.1, 0.2, 0.5, 0.2],
                                              seed=the_circus.seeder.next())

    item_bulk_gen = gen.DependentBulkGenerator(
        element_generator=the_circus.generators["items_gen"])

    restock_action.set_operations(
        the_circus.clock.ops.timestamp(named_as="TIME"),

        # include the POS NAME attribute as a field name "POS_NAME"
        pos.ops.lookup(actor_id_field="POS_ID", select={"NAME": "POS_NAME"}),

        pos.get_relationship("items").ops.get_neighbourhood_size(
            from_field="POS_ID",
            named_as="PREV_STOCK_LEVEL"),

        stock_size_gen.ops.generate(named_as="RESTOCK_VOLUME"),

        item_bulk_gen.ops.generate(named_as="NEW_ITEM_IDS",
                                   observed_field="RESTOCK_VOLUME"),

        pos.get_relationship("items").ops.add_grouped(from_field="POS_ID",
                                           grouped_items_field="NEW_ITEM_IDS"),

        pos.get_relationship("items").ops.get_neighbourhood_size(
            from_field="POS_ID",
            named_as="NEW_STOCK_LEVEL"),

        ops.FieldLogger(log_id="restock",
                        cols=["TIME", "POS_ID", "POS_NAME", "RESTOCK_VOLUME",
                              "PREV_STOCK_LEVEL", "NEW_STOCK_LEVEL"])
    )


def create_purchase_action(the_circus):

    timer_gen = profilers.WorkHoursTimerGenerator(clock=the_circus.clock,
                                                  seed=the_circus.seeder.next())

    customers = the_circus.actors["customer"]

    purchase_action = the_circus.create_action(
            name="purchase",
            initiating_actor=customers,
            actorid_field="CUST_ID",

            timer_gen=timer_gen,

            # this time not all customers have the activity level: on average
            # they will collectively perform 1 action per day, but some will do
            # on average more actions per day and some will do on average less
            # actions per day
            activity_gen=gen.NumpyRandomGenerator(
                method="exponential",
                scale=timer_gen.activity(
                    n_actions=1, per=pd.Timedelta("24h")
                    ), seed=the_circus.seeder.next())
        )

    customers_items = customers.get_relationship("my_items")
    pos = the_circus.actors["point_of_sale"]
    pos_items = pos.get_relationship("items")

    purchase_action.set_operations(

        customers.ops.lookup(actor_id_field="CUST_ID",
                             select={
                                 "FIRST_NAME": "BUYER_FIRST_NAME",
                                 "LAST_NAME": "BUYER_LAST_NAME"}),

        pos.ops.select_one(named_as="POS_ID"),

        pos.ops.lookup(actor_id_field="POS_ID",
                       select={"COMPANY": "POS_NAME"}),

        # pick an item from the vendor's stock
        pos_items.ops.select_one(

            # join the POS table on the POS_ID field of the action_data
            from_field="POS_ID",

            # the result of that join is to be populated into that field
            named_as="BOUGHT_ITEM_ID",

            # each joined item should be unique (2 customers cannot buy the
            # same item)
            one_to_one=True,

            # remove the joined items from the POS relationship
            pop=True,

            # in case some POS is out of stock, just drop the row in the
            # action_data. (As an alternative, we could keep it and trigger
            # some retries for the empty value later on.. )
            discard_empty=True),

        # adds the item to the "my_items" relations of each customer
        customers_items.ops.add(
            # action_data field containing the added item
            item_field="BOUGHT_ITEM_ID",

            # action_data field containing the "from" side of the relations
            # (i..e the id of the customer buying the item in this case)
            from_field="CUST_ID"
        ),

        ops.FieldLogger(log_id="purchases")

    )


def update_purchase_action(the_circus):
    """
    Adds some operations to the existing customer purchase action in order to
    trigger a POS restock if their stock level gets low
    """

    purchase_action = the_circus.get_action("purchase")
    pos = the_circus.actors["point_of_sale"]

    # trigger_prop_func(level) specifies the probability of re-stocking as a
    # function the stock level
    trigger_prop_func = ops.bounded_sigmoid(

        # below x_min, probability is one, and decrements as x increases
        incrementing=False,

        # probability is 1 when level=2, 0 when level 10 and after
        x_min=2, x_max=10,

        # this controls the shape of the S curve in between
        shape=10)

    # Wraps the sigmoid into a dependent trigger, i.e.:
    #   - a generator, i.e producing random values
    #   - of booleans, hence the name "trigger"
    #   - dependent, i.e. as a function of action_data field at execution time
    trigger_gen = gen.DependentTriggerGenerator(
        value_to_proba_mapper=trigger_prop_func,
        seed=the_circus.seeder.next())

    # since those operations are added after the FieldLogger, the fields they
    # create will not be appended to the action_data
    purchase_action.append_operations(

        pos.get_relationship("items").ops.get_neighbourhood_size(
            from_field="POS_ID",
            named_as="POS_STOCK"),

        # generates random booleans with probability related to the stock level
        trigger_gen.ops.generate(
            observed_field="POS_STOCK",
            named_as="SHOULD_RESTOCK"),

        # trigger the restock action of the POS whose SHOULD_RESTOCK field is
        # now true
        the_circus.get_action("restock").ops.force_act_next(
            actor_id_field="POS_ID",
            condition_field="SHOULD_RESTOCK")
    )


def run_and_report(the_circus):
    the_circus.run(
        duration=pd.Timedelta("5 days"),
        log_output_folder="output/example2",
        delete_existing_logs=True
    )

    with open("output/example2/report.csv") as f:
        print "Logged {} lines".format(len(f.readlines()) - 1)


def step1():

    example2 = build_circus()

    create_pos_actor(example2)
    add_report_action(example2)
    run_and_report(example2)


def step2():

    example2 = build_circus()

    create_pos_actor(example2)
    add_items_to_pos_stock(example2)
    add_report_action(example2)
    run_and_report(example2)


def step3():

    example2 = build_circus()

    create_pos_actor(example2)
    add_items_to_pos_stock(example2)
    add_periodic_restock_action(example2)

    add_report_action(example2)
    run_and_report(example2)


def step4():

    example2 = build_circus()

    # point of sales
    create_pos_actor(example2)
    add_items_to_pos_stock(example2)
    add_periodic_restock_action(example2)
    add_report_action(example2)

    # customers
    create_customer_actor(example2)
    create_purchase_action(example2)

    run_and_report(example2)


def step5():

    example2 = build_circus()

    # point of sales
    create_pos_actor(example2)
    add_items_to_pos_stock(example2)
    add_inactive_restock_action(example2)
    add_report_action(example2)

    # customers
    create_customer_actor(example2)
    create_purchase_action(example2)

    update_purchase_action(example2)

    run_and_report(example2)


if __name__ == "__main__":
    util_functions.setup_logging()
    step5()
