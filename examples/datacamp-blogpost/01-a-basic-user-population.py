import logging
import pandas as pd
from tabulate import tabulate 

from trumania.core import circus
from trumania.core.random_generators import SequencialGenerator, FakerGenerator, NumpyRandomGenerator
import trumania.core.util_functions as util_functions


util_functions.setup_logging()

example_circus = circus.Circus(name="example1", 
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


logging.info("\n" + 
  tabulate(person.to_dataframe().head(10), headers='keys', tablefmt='psql')
)



