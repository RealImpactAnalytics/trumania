from datagenerator.core import circus
from datagenerator.core.circus import *
from datagenerator.core.actor import *
import datagenerator.core.util_functions as util_functions
from tabulate import tabulate


util_functions.setup_logging()

logging.info("building circus")

example = circus.Circus(
    name="example",
    master_seed=12345,
    start=pd.Timestamp("1 Jan 2017 00:00"),
    step_duration=pd.Timedelta("1h"))

person = example.create_actor(
    name="person", size=1000,
    ids_gen=SequencialGenerator(prefix="PERSON_"))

person.create_attribute(
    "NAME",
    init_gen=FakerGenerator(method="name",
                            seed=example.seeder.next()))

person.create_attribute(
    "age",
    init_gen=NumpyRandomGenerator(
        method="normal", loc="35", scale="5",
        seed=example.seeder.next()))

example.run(
    duration=pd.Timedelta("48h"),
    log_output_folder="output/example2",
    delete_existing_logs=True
)


logging.info("10 first persons: \n" +
    tabulate(person.to_dataframe().head(10),
             headers='keys', tablefmt='psql'))



