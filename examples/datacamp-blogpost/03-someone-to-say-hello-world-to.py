import logging
import pandas as pd
from tabulate import tabulate 

from trumania.core import circus, operations
from trumania.core.random_generators import SequencialGenerator, FakerGenerator, NumpyRandomGenerator, ConstantDependentGenerator, ConstantGenerator
import trumania.core.util_functions as util_functions


util_functions.setup_logging()

example_circus = circus.Circus(name="example", 
                               master_seed=12345,
                               start=pd.Timestamp("1 Jan 2017 00:00"),
                               step_duration=pd.Timedelta("1h"))

id_gen = SequencialGenerator(prefix="PERSON_")
age_gen = NumpyRandomGenerator(method="normal", loc=3, scale=5,
                               seed=next(example_circus.seeder))
name_gen = FakerGenerator(method="name", seed=next(example_circus.seeder))

person = example_circus.create_population(name="person", size=1000, ids_gen=id_gen)
person.create_attribute("NAME", init_gen=name_gen)
person.create_attribute("AGE", init_gen=age_gen)

hello_world = example_circus.create_story(
    name="hello_world",
    initiating_population=example_circus.populations["person"],
    member_id_field="PERSON_ID",
    timer_gen=ConstantDependentGenerator(value=1)
)

hello_world.set_operations(
    example_circus.clock.ops.timestamp(named_as="TIME"),
    ConstantGenerator(value="hello world").ops.generate(named_as="MESSAGE"),
    
    example_circus.populations["person"].ops.select_one(named_as="OTHER_PERSON"),

    example_circus.populations["person"]
        .ops.lookup(id_field="PERSON_ID", select={"NAME": "EMITTER_NAME"}),

    example_circus.populations["person"]
        .ops.lookup(id_field="OTHER_PERSON", select={"NAME": "RECEIVER_NAME"}),

    operations.FieldLogger(log_id="hello_3")
)

example_circus.run(
    duration=pd.Timedelta("48h"),
    log_output_folder="output/example_scenario",
    delete_existing_logs=True
)

# -- DEBUG output printout
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
df = pd.read_csv("output/example_scenario/hello_3.csv")
print(df.head(10))
print(df.tail(10))
