import logging
import pandas as pd

from trumania.core import circus
import trumania.core.util_functions as util_functions
from trumania.core.operations import FieldLogger
from trumania.core.random_generators import SequencialGenerator, FakerGenerator, NumpyRandomGenerator
from trumania.core.random_generators import ConstantDependentGenerator


util_functions.setup_logging()

logging.info("building circus")


def create_circus_with_population():
    example_circus = circus.Circus(
        name="example",
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))

    person = example_circus.create_population(
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


def add_quotes(the_circus):

    quote_generator = FakerGenerator(method="sentence",
                                     nb_words=6,
                                     variable_nb_words=True,
                                     seed=next(the_circus.seeder))

    person = the_circus.populations["person"]

    quotes_rel = person.create_relationship("quotes")

    for w in range(4):
        quotes_rel.add_relations(
            from_ids=person.ids,
            to_ids=quote_generator.generate(size=person.size),
            weights=w
        )


the_circus = create_circus_with_population()
add_quotes(the_circus)

hello_world = the_circus.create_story(
    name="hello_world",
    initiating_population=the_circus.populations["person"],
    member_id_field="PERSON_ID",
    timer_gen=ConstantDependentGenerator(value=1)
)

hello_world.set_operations(

    # adding a random timestamp, within the current clock step
    the_circus.clock
        .ops
        .timestamp(named_as="TIME"),

    # message is now selected from the favourite quotes of the speaker
    the_circus.populations["person"].get_relationship("quotes")
        .ops
        .select_one(
            from_field="PERSON_ID",
            named_as="MESSAGE"),

    # selecting a random "other person"
    the_circus.populations["person"]
        .ops
        .select_one(named_as="OTHER_PERSON"),

    the_circus.populations["person"]
        .ops
        .lookup(id_field="PERSON_ID",
                select={"NAME": "EMITTER_NAME"}),

    the_circus.populations["person"]
        .ops
        .lookup(id_field="OTHER_PERSON",
                select={"NAME": "RECEIVER_NAME"}),

    # specifying which fields to put in the log
    FieldLogger(log_id="hello")

)

the_circus.run(
    duration=pd.Timedelta("12h"),
    log_output_folder="output/example4",
    delete_existing_logs=True
)

with open("output/example4/hello.csv") as log:
    logging.info("some produced logs: \n\n" + "".join(log.readlines(1000)[:10]))
