import logging
import pandas as pd

from trumania.core import circus
import trumania.core.util_functions as util_functions
from trumania.core.operations import FieldLogger
from trumania.core.random_generators import SequencialGenerator, FakerGenerator, NumpyRandomGenerator
from trumania. components.time_patterns.profilers import WorkHoursTimerGenerator


util_functions.setup_logging()

logging.info("building circus")


def create_circus_with_actor():
    example_circus = circus.Circus(
        name="example",
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))

    person = example_circus.create_actor(
        name="person", size=1000,
        ids_gen=SequencialGenerator(prefix="PERSON_"))

    person.create_attribute(
        "NAME",
        init_gen=FakerGenerator(method="name",
                                seed=example_circus.seeder.next()))

    person.create_attribute(
        "age",
        init_gen=NumpyRandomGenerator(
            method="normal", loc="35", scale="5",
            seed=example_circus.seeder.next()))

    return example_circus


the_circus = create_circus_with_actor()

hello_world = the_circus.create_action(
    name="hello_world",
    initiating_actor=the_circus.actors["person"],
    actorid_field="PERSON_ID",

    # each actor instance is now going to have 10, 20 or 30
    # trigger of this action per week
    activity_gen=NumpyRandomGenerator(
        method="choice", a=[10, 20, 30],
        seed=the_circus.seeder.next()
    ),

    # action now only tiggers during office hours
    timer_gen=WorkHoursTimerGenerator(
        clock=the_circus.clock,
        seed=the_circus.seeder.next())
)

hello_world.set_operations(

    # adding a random timestamp, within the current clock step
    the_circus.clock
    .ops
    .timestamp(named_as="TIME"),

    # message is now a random sentence from Faker
    FakerGenerator(method="sentence",
                   nb_words=6, variable_nb_words=True,
                   seed=the_circus.seeder.next()
                   )
    .ops
    .generate(named_as="MESSAGE"),

    # selecting a random "other person"
    the_circus.actors["person"]
    .ops
    .select_one(named_as="OTHER_PERSON"),

    the_circus.actors["person"]
    .ops
    .lookup(actor_id_field="PERSON_ID",
            select={"NAME": "EMITTER_NAME"}),

    the_circus.actors["person"]
    .ops
    .lookup(actor_id_field="OTHER_PERSON",
            select={"NAME": "RECEIVER_NAME"}),

    # specifying which fields to put in the log
    FieldLogger(log_id="hello",
                cols=["TIME", "EMITTER_NAME", "RECEIVER_NAME", "MESSAGE"]
                )

)

the_circus.run(
    duration=pd.Timedelta("48h"),
    log_output_folder="output/example8",
    delete_existing_logs=True
)

with open("output/example8/hello.csv") as log:
    logging.info("some produced logs: \n\n" + "".join(log.readlines(10)[:10]))
