import pandas as pd

from trumania.core import circus
from trumania.core.util_functions import make_random_bipartite_data, setup_logging
from trumania.core.operations import FieldLogger, Apply
from trumania.core.random_generators import SequencialGenerator, FakerGenerator, NumpyRandomGenerator
from trumania.core.random_generators import ConstantDependentGenerator, ConstantGenerator

# each step_() function below implement one step of the first example of the
# tutorial documented at
# https://github.com/RealImpactAnalytics/trumania/wiki


def step1():

    example1 = circus.Circus(
        name="example1",
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))

    person = example1.create_population(
        name="person", size=1000,
        ids_gen=SequencialGenerator(prefix="PERSON_"))

    hello_world = example1.create_story(
        name="hello_world",
        initiating_population=person,
        member_id_field="PERSON_ID",

        # after each story, reset the timer to 0, so that it will get
        # executed again at the next clock tick (next hour)
        timer_gen=ConstantDependentGenerator(value=0)
    )

    hello_world.set_operations(
        ConstantGenerator(value="hello world").ops.generate(named_as="HELLO"),
        FieldLogger(log_id="hello")
    )

    example1.run(
        duration=pd.Timedelta("48h"),
        log_output_folder="output/example1",
        delete_existing_logs=True
    )

    with open("output/example1/hello.csv") as f:
        print("Logged {} lines".format(len(f.readlines()) - 1))


def step2():

    example1 = circus.Circus(
        name="example1",
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))

    person = example1.create_population(
        name="person", size=1000,
        ids_gen=SequencialGenerator(prefix="PERSON_"))

    person.create_attribute(
        "NAME",
        init_gen=FakerGenerator(method="name",
                                seed=next(example1.seeder)))

    hello_world = example1.create_story(
        name="hello_world",
        initiating_population=person,
        member_id_field="PERSON_ID",

        # after each story, reset the timer to 0, so that it will get
        # executed again at the next clock tick (next hour)
        timer_gen=ConstantDependentGenerator(value=0)
    )

    hello_world.set_operations(
        person.ops.lookup(
            id_field="PERSON_ID",
            select={"NAME": "NAME"}
        ),
        ConstantGenerator(value="hello world").ops.generate(named_as="HELLO"),
        example1.clock.ops.timestamp(named_as="TIME"),
        FieldLogger(log_id="hello")
    )

    example1.run(
        duration=pd.Timedelta("48h"),
        log_output_folder="output/example1",
        delete_existing_logs=True
    )

    with open("output/example1/hello.csv") as f:
        print("Logged {} lines".format(len(f.readlines()) - 1))


def step3():

    example1 = circus.Circus(
        name="example1",
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))

    person = example1.create_population(
        name="person", size=1000,
        ids_gen=SequencialGenerator(prefix="PERSON_"))

    person.create_attribute(
        "NAME",
        init_gen=FakerGenerator(method="name",
                                seed=next(example1.seeder)))

    hello_world = example1.create_story(
        name="hello_world",
        initiating_population=person,
        member_id_field="PERSON_ID",

        # after each story, reset the timer to 0, so that it will get
        # executed again at the next clock tick (next hour)
        timer_gen=ConstantDependentGenerator(value=0)
    )

    duration_gen = NumpyRandomGenerator(method="exponential", scale=60,
                                        seed=next(example1.seeder))

    sites = SequencialGenerator(prefix="SITE_").generate(1000)
    random_site_gen = NumpyRandomGenerator(method="choice", a=sites,
                                           seed=next(example1.seeder))

    hello_world.set_operations(
        person.ops.lookup(
            id_field="PERSON_ID",
            select={"NAME": "NAME"}
        ),

        ConstantGenerator(value="hello world").ops.generate(named_as="HELLO"),

        duration_gen.ops.generate(named_as="DURATION"),
        random_site_gen.ops.generate(named_as="SITE"),

        example1.clock.ops.timestamp(named_as="TIME"),

        FieldLogger(log_id="hello")
    )

    example1.run(
        duration=pd.Timedelta("48h"),
        log_output_folder="output/example1",
        delete_existing_logs=True
    )

    with open("output/example1/hello.csv") as f:
        print("Logged {} lines".format(len(f.readlines()) - 1))


def step4():
    """
    Woah, this got drastically slower
    """

    example1 = circus.Circus(
        name="example1",
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))

    person = example1.create_population(
        name="person", size=1000,
        ids_gen=SequencialGenerator(prefix="PERSON_"))

    person.create_attribute(
        "NAME",
        init_gen=FakerGenerator(method="name",
                                seed=next(example1.seeder)))

    sites = SequencialGenerator(prefix="SITE_").generate(1000)
    random_site_gen = NumpyRandomGenerator(method="choice", a=sites,
                                           seed=next(example1.seeder))

    allowed_sites = person.create_relationship(name="sites")
    for i in range(5):
        allowed_sites \
            .add_relations(from_ids=person.ids,
                           to_ids=random_site_gen.generate(person.size))

    hello_world = example1.create_story(
        name="hello_world",
        initiating_population=person,
        member_id_field="PERSON_ID",

        # after each story, reset the timer to 0, so that it will get
        # executed again at the next clock tick (next hour)
        timer_gen=ConstantDependentGenerator(value=0)
    )

    duration_gen = NumpyRandomGenerator(method="exponential", scale=60,
                                        seed=next(example1.seeder))

    hello_world.set_operations(
        person.ops.lookup(
            id_field="PERSON_ID",
            select={"NAME": "NAME"}
        ),

        ConstantGenerator(value="hello world").ops.generate(named_as="HELLO"),

        duration_gen.ops.generate(named_as="DURATION"),
        allowed_sites.ops.select_one(from_field="PERSON_ID",
                                     named_as="SITE"),

        example1.clock.ops.timestamp(named_as="TIME"),

        FieldLogger(log_id="hello")
    )

    example1.run(
        duration=pd.Timedelta("48h"),
        log_output_folder="output/example1",
        delete_existing_logs=True
    )

    with open("output/example1/hello.csv") as f:
        print("Logged {} lines".format(len(f.readlines()) - 1))


def step5():

    example1 = circus.Circus(
        name="example1",
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))

    person = example1.create_population(
        name="person", size=1000,
        ids_gen=SequencialGenerator(prefix="PERSON_"))

    person.create_attribute(
        "NAME",
        init_gen=FakerGenerator(method="name",
                                seed=next(example1.seeder)))

    sites = SequencialGenerator(prefix="SITE_").generate(1000)
    random_site_gen = NumpyRandomGenerator(method="choice", a=sites,
                                           seed=next(example1.seeder))

    allowed_sites = person.create_relationship(name="sites")

    # Add HOME sites
    allowed_sites.add_relations(from_ids=person.ids,
                                to_ids=random_site_gen.generate(person.size),
                                weights=0.4)

    # Add WORK sites
    allowed_sites.add_relations(from_ids=person.ids,
                                to_ids=random_site_gen.generate(person.size),
                                weights=0.3)

    # Add OTHER sites
    for i in range(3):
        allowed_sites \
            .add_relations(from_ids=person.ids,
                           to_ids=random_site_gen.generate(person.size),
                           weights=0.1)

    hello_world = example1.create_story(
        name="hello_world",
        initiating_population=person,
        member_id_field="PERSON_ID",

        # after each story, reset the timer to 0, so that it will get
        # executed again at the next clock tick (next hour)
        timer_gen=ConstantDependentGenerator(value=0)
    )

    duration_gen = NumpyRandomGenerator(method="exponential", scale=60,
                                        seed=next(example1.seeder))

    hello_world.set_operations(
        person.ops.lookup(
            id_field="PERSON_ID",
            select={"NAME": "NAME"}
        ),

        ConstantGenerator(value="hello world").ops.generate(named_as="HELLO"),

        duration_gen.ops.generate(named_as="DURATION"),
        allowed_sites.ops.select_one(from_field="PERSON_ID",
                                     named_as="SITE"),

        example1.clock.ops.timestamp(named_as="TIME"),

        FieldLogger(log_id="hello")
    )

    example1.run(
        duration=pd.Timedelta("48h"),
        log_output_folder="output/example1",
        delete_existing_logs=True
    )

    with open("output/example1/hello.csv") as f:
        print("Logged {} lines".format(len(f.readlines()) - 1))


def step6():

    example1 = circus.Circus(
        name="example1",
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))

    person = example1.create_population(
        name="person", size=1000,
        ids_gen=SequencialGenerator(prefix="PERSON_"))

    person.create_attribute(
        "NAME",
        init_gen=FakerGenerator(method="name",
                                seed=next(example1.seeder)))
    person.create_attribute(
        "POPULARITY",
        init_gen=NumpyRandomGenerator(
            method="uniform", low=0, high=1, seed=next(example1.seeder)))

    sites = SequencialGenerator(prefix="SITE_").generate(1000)
    random_site_gen = NumpyRandomGenerator(method="choice", a=sites,
                                           seed=next(example1.seeder))

    allowed_sites = person.create_relationship(name="sites")

    # SITES ------------------

    # Add HOME sites
    allowed_sites.add_relations(from_ids=person.ids,
                                to_ids=random_site_gen.generate(person.size),
                                weights=0.4)

    # Add WORK sites
    allowed_sites.add_relations(from_ids=person.ids,
                                to_ids=random_site_gen.generate(person.size),
                                weights=0.3)

    # Add OTHER sites
    for i in range(3):
        allowed_sites \
            .add_relations(from_ids=person.ids,
                           to_ids=random_site_gen.generate(person.size),
                           weights=0.1)

    # FRIENDS ------------------

    friends = person.create_relationship(name="friends")

    friends_df = pd.DataFrame.from_records(
        make_random_bipartite_data(
            person.ids,
            person.ids,
            p=0.005,  # probability for a node to be connected to
                      # another one : 5 friends on average = 5/1000
            seed=next(example1.seeder)),
        columns=["A", "B"])

    friends.add_relations(
        from_ids=friends_df["A"],
        to_ids=friends_df["B"])

    # STORIES ------------------

    hello_world = example1.create_story(
        name="hello_world",
        initiating_population=person,
        member_id_field="PERSON_ID",

        # after each story, reset the timer to 0, so that it will get
        # executed again at the next clock tick (next hour)
        timer_gen=ConstantDependentGenerator(value=0)
    )

    duration_gen = NumpyRandomGenerator(method="exponential", scale=60,
                                        seed=next(example1.seeder))

    hello_world.set_operations(
        person.ops.lookup(
            id_field="PERSON_ID",
            select={"NAME": "NAME"}
        ),

        ConstantGenerator(value="hello world").ops.generate(named_as="HELLO"),

        duration_gen.ops.generate(named_as="DURATION"),
        allowed_sites.ops.select_one(from_field="PERSON_ID",
                                     named_as="SITE"),

        friends.ops.select_one(
            from_field="PERSON_ID",
            named_as="COUNTERPART_ID",
            weight=person.get_attribute_values("POPULARITY"),
            # For people that do not have friends, it will try to find
            # the POPULARITY attribute of a None and crash miserably
            # Adding this flag will discard people that do not have friends
            discard_empty=True),

        person.ops.lookup(
            id_field="COUNTERPART_ID",
            select={"NAME": "COUNTER_PART_NAME"}
        ),

        example1.clock.ops.timestamp(named_as="TIME"),

        FieldLogger(log_id="hello")
    )

    example1.run(
        duration=pd.Timedelta("48h"),
        log_output_folder="output/example1",
        delete_existing_logs=True
    )

    with open("output/example1/hello.csv") as f:
        print("Logged {} lines".format(len(f.readlines()) - 1))


def step7():

    example1 = circus.Circus(
        name="example1",
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))

    person = example1.create_population(
        name="person", size=1000,
        ids_gen=SequencialGenerator(prefix="PERSON_"))

    person.create_attribute(
        "NAME",
        init_gen=FakerGenerator(method="name",
                                seed=next(example1.seeder)))
    person.create_attribute(
        "POPULARITY",
        init_gen=NumpyRandomGenerator(
            method="uniform", low=0, high=1, seed=next(example1.seeder)))

    sites = SequencialGenerator(prefix="SITE_").generate(1000)
    random_site_gen = NumpyRandomGenerator(method="choice", a=sites,
                                           seed=next(example1.seeder))

    allowed_sites = person.create_relationship(name="sites")

    # SITES ------------------

    # Add HOME sites
    allowed_sites.add_relations(from_ids=person.ids,
                                to_ids=random_site_gen.generate(person.size),
                                weights=0.4)

    # Add WORK sites
    allowed_sites.add_relations(from_ids=person.ids,
                                to_ids=random_site_gen.generate(person.size),
                                weights=0.3)

    # Add OTHER sites
    for i in range(3):
        allowed_sites \
            .add_relations(from_ids=person.ids,
                           to_ids=random_site_gen.generate(person.size),
                           weights=0.1)

    # FRIENDS ------------------

    friends = person.create_relationship(name="friends")

    friends_df = pd.DataFrame.from_records(
        make_random_bipartite_data(
            person.ids,
            person.ids,
            p=0.005,  # probability for a node to be connected to
                      # another one : 5 friends on average = 5/1000
            seed=next(example1.seeder)),
        columns=["A", "B"])

    friends.add_relations(
        from_ids=friends_df["A"],
        to_ids=friends_df["B"])

    # PRICE ------------------

    def price(story_data):

        result = pd.DataFrame(index=story_data.index)

        result["PRICE"] = story_data["DURATION"] * 0.05
        result["CURRENCY"] = "EUR"

        return result

    # STORIES ------------------

    hello_world = example1.create_story(
        name="hello_world",
        initiating_population=person,
        member_id_field="PERSON_ID",

        # after each story, reset the timer to 0, so that it will get
        # executed again at the next clock tick (next hour)
        timer_gen=ConstantDependentGenerator(value=0)
    )

    duration_gen = NumpyRandomGenerator(method="exponential", scale=60,
                                        seed=next(example1.seeder))

    hello_world.set_operations(
        person.ops.lookup(
            id_field="PERSON_ID",
            select={"NAME": "NAME"}
        ),

        ConstantGenerator(value="hello world").ops.generate(named_as="HELLO"),

        duration_gen.ops.generate(named_as="DURATION"),

        friends.ops.select_one(
            from_field="PERSON_ID",
            named_as="COUNTERPART_ID",
            weight=person.get_attribute_values("POPULARITY"),
            # For people that do not have friends, it will try to find
            # the POPULARITY attribute of a None and crash miserably
            # Adding this flag will discard people that do not have friends
            discard_empty=True),

        person.ops.lookup(
            id_field="COUNTERPART_ID",
            select={"NAME": "COUNTER_PART_NAME"}
        ),

        allowed_sites.ops.select_one(from_field="PERSON_ID",
                                     named_as="SITE"),

        allowed_sites.ops.select_one(from_field="COUNTERPART_ID",
                                     named_as="COUNTERPART_SITE"),

        Apply(
            source_fields=["DURATION", "SITE", "COUNTERPART_SITE"],
            named_as=["PRICE", "CURRENCY"],
            f=price, f_args="dataframe"),

        example1.clock.ops.timestamp(named_as="TIME"),

        FieldLogger(log_id="hello")
    )

    example1.run(
        duration=pd.Timedelta("48h"),
        log_output_folder="output/example1",
        delete_existing_logs=True
    )

    with open("output/example1/hello.csv") as f:
        print("Logged {} lines".format(len(f.readlines()) - 1))


if __name__ == "__main__":
    setup_logging()
    step7()
