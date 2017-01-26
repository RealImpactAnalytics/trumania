from datagenerator.core import circus
from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *


def step1():

    example2 = circus.Circus(
        name="example2",
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))

    pos = example2.create_actor(
        name="point_of_sale", size=100,
        ids_gen=SequencialGenerator(prefix="POS_"))

    name_gen = FakerGenerator(method="name", seed=example2.seeder.next())
    pos.create_attribute("NAME", init_gen=name_gen)

    city_gen = FakerGenerator(method="city", seed=example2.seeder.next())
    pos.create_attribute("CITY", init_gen=city_gen)

    company_gen = FakerGenerator(method="company", seed=example2.seeder.next())
    pos.create_attribute("COMPANY", init_gen=company_gen)

    items_relationship = pos.create_relationship(name="items")

    report_action = example2.create_action(
        name="report",
        initiating_actor=pos,
        actorid_field="POS_ID",

        timer_gen=ConstantDependentGenerator(
            value=example2.clock.n_iterations(duration=pd.Timedelta("24h")) - 1)
    )

    report_action.set_operations(
        example2.clock.ops.timestamp(named_as="TIME", random=False,
                                     log_format="%Y-%m-%d"),

        items_relationship.ops.get_neighbourhood_size(
            from_field="POS_ID",
            named_as="STOCK_LEVEL"),

        FieldLogger(log_id="report", cols=["TIME", "POS_ID", "STOCK_LEVEL"])
    )

    example2.run(
        duration=pd.Timedelta("72h"),
        log_output_folder="output/example2",
        delete_existing_logs=True
    )

    with open("output/example2/report.csv") as f:
        print "Logged {} lines".format(len(f.readlines())-1)


def step2():

    example2 = circus.Circus(
        name="example2",
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))

    n_pos = 100

    pos = example2.create_actor(
        name="point_of_sale", size=n_pos,
        ids_gen=SequencialGenerator(prefix="POS_"))

    name_gen = FakerGenerator(method="name", seed=example2.seeder.next())
    pos.create_attribute("NAME", init_gen=name_gen)

    city_gen = FakerGenerator(method="city", seed=example2.seeder.next())
    pos.create_attribute("CITY", init_gen=city_gen)

    company_gen = FakerGenerator(method="company", seed=example2.seeder.next())
    pos.create_attribute("COMPANY", init_gen=company_gen)

    items_relationship = pos.create_relationship(name="items")

    items_gen = SequencialGenerator(prefix="ITEM_")
    example2.attach_generator("items_gen", items_gen)

    item_arrays = [items_gen.generate(5) for i in range(n_pos)]
    items_relationship.add_grouped_relations(
        from_ids=pos.ids,
        grouped_ids=item_arrays)

    report_action = example2.create_action(
        name="report",
        initiating_actor=pos,
        actorid_field="POS_ID",

        timer_gen=ConstantDependentGenerator(
            value=example2.clock.n_iterations(duration=pd.Timedelta("24h")) - 1)
    )

    report_action.set_operations(
        example2.clock.ops.timestamp(named_as="TIME", random=False,
                                     log_format="%Y-%m-%d"),

        items_relationship.ops.get_neighbourhood_size(
            from_field="POS_ID",
            named_as="STOCK_LEVEL"),

        FieldLogger(log_id="report", cols=["TIME", "POS_ID", "STOCK_LEVEL"])
    )

    example2.run(
        duration=pd.Timedelta("72h"),
        log_output_folder="output/example2",
        delete_existing_logs=True
    )

    with open("output/example2/report.csv") as f:
        print "Logged {} lines".format(len(f.readlines())-1)


def step3():

    example2 = circus.Circus(
        name="example2",
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))

    n_pos = 100

    pos = example2.create_actor(
        name="point_of_sale", size=n_pos,
        ids_gen=SequencialGenerator(prefix="POS_"))

    name_gen = FakerGenerator(method="name", seed=example2.seeder.next())
    pos.create_attribute("NAME", init_gen=name_gen)

    city_gen = FakerGenerator(method="city", seed=example2.seeder.next())
    pos.create_attribute("CITY", init_gen=city_gen)

    company_gen = FakerGenerator(method="company", seed=example2.seeder.next())
    pos.create_attribute("COMPANY", init_gen=company_gen)

    items_relationship = pos.create_relationship(name="items")

    items_gen = SequencialGenerator(prefix="ITEM_")
    example2.attach_generator("items_gen", items_gen)

    item_arrays = [items_gen.generate(5) for i in range(n_pos)]
    items_relationship.add_grouped_relations(
        from_ids=pos.ids,
        grouped_ids=item_arrays)

    # REPORT --------------------------------

    report_action = example2.create_action(
        name="report",
        initiating_actor=pos,
        actorid_field="POS_ID",

        timer_gen=ConstantDependentGenerator(
            value=example2.clock.n_iterations(duration=pd.Timedelta("24h")) - 1)
    )

    report_action.set_operations(
        example2.clock.ops.timestamp(named_as="TIME", random=False,
                                     log_format="%Y-%m-%d"),

        items_relationship.ops.get_neighbourhood_size(
            from_field="POS_ID",
            named_as="STOCK_LEVEL"),

        FieldLogger(log_id="report", cols=["TIME", "POS_ID", "STOCK_LEVEL"])
    )

    # RESTOCK --------------------------------

    restock_action = example2.create_action(
        name="restock",
        initiating_actor=pos,
        actorid_field="POS_ID",

        timer_gen=DefaultDailyTimerGenerator(example2.clock,
                                             example2.seeder.next())
    )

    stock_size_gen = NumpyRandomGenerator(method="choice",
                                          a=[5, 15, 20, 25],
                                          p=[0.1, 0.2, 0.5, 0.2],
                                          seed=example2.seeder.next())

    item_bulk_gen = DependentBulkGenerator(example2.generators["items_gen"])

    restock_action.set_operations(
        example2.clock.ops.timestamp(named_as="TIME",
                                     log_format="%Y-%m-%d"),

        pos.ops.lookup(actor_id_field="POS_ID", select={"NAME":"NAME"}),

        stock_size_gen.ops.generate(named_as="RESTOCK_VOLUME"),

        item_bulk_gen.ops.generate(named_as="NEW_ITEM_IDS",
                                   observed_field="RESTOCK_VOLUME"),

        items_relationship.ops.add_grouped(from_field="POS_ID",
                                           grouped_items_field="NEW_ITEM_IDS"),

        FieldLogger(log_id="restock", cols=["TIME", "POS_ID", "NAME",
                                            "RESTOCK_VOLUME"])
    )

    example2.run(
        duration=pd.Timedelta("20 days"),
        log_output_folder="output/example2",
        delete_existing_logs=True
    )

    with open("output/example2/report.csv") as f:
        print "Logged {} lines in report".format(len(f.readlines())-1)

    with open("output/example2/report.csv") as f:
        print "Logged {} lines in restock".format(len(f.readlines())-1)


if __name__ == "__main__":
    step3()
