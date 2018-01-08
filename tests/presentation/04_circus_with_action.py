from trumania.core import circus
from trumania.core.circus import *
from trumania.core.actor import *
import trumania.core.util_functions as util_functions
from tabulate import tabulate


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
                                seed=next(example_circus.seeder)))

    person.create_attribute(
        "age",
        init_gen=NumpyRandomGenerator(
            method="normal", loc=35, scale=5,
            seed=next(example_circus.seeder)))

    return example_circus


the_circus = create_circus_with_actor()

hello_world = the_circus.create_action(
    name="hello_world",
    initiating_actor=the_circus.actors["person"],
    actorid_field="PERSON_ID",

    timer_gen=ConstantDependentGenerator(value=1)
)

hello_world.set_operations(

    # adding a random timestamp, within the current clock step
    the_circus.clock
        .ops
        .timestamp(named_as="TIME"),

    ConstantGenerator(value="hello world")
        .ops
        .generate(named_as="MESSAGE"),

    # selecting a random "other person"
    the_circus.actors["person"]
        .ops
        .select_one(named_as="OTHER_PERSON"),

    # specifying which fields to put in the log
    FieldLogger(log_id="hello",
        cols=["TIME", "PERSON_ID", "OTHER_PERSON", "MESSAGE"]
                )

)

the_circus.run(
    duration=pd.Timedelta("48h"),
    log_output_folder="output/example4",
    delete_existing_logs=True
)

with open("output/example4/hello.csv") as log:
    logging.info("some produced logs: \n\n" + "".join(log.readlines(100000)[:10]))
