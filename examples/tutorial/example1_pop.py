import pandas as pd
import logging

from trumania.core import circus
from trumania.core.util_functions import make_random_bipartite_data, setup_logging
from trumania.core.operations import FieldLogger, Apply
from trumania.core.random_generators import SequencialGenerator, FakerGenerator, NumpyRandomGenerator
from trumania.core.random_generators import ConstantDependentGenerator, ConstantGenerator

# each step_() function below implement one step of the first example of the
# tutorial documented at
# https://github.com/RealImpactAnalytics/trumania/wiki


if __name__ == "__main__":

    setup_logging()

    example1 = circus.Circus(
        name="example1",
        master_seed=12345,
        start=pd.Timestamp("1 Jan 2017 00:00"),
        step_duration=pd.Timedelta("1h"))

    person = example1.create_population(
        name="person", size=10000000,
        ids_gen=SequencialGenerator(prefix="PERSON_"))

    logging.info("pop created")

    person.create_attribute(
        "QUANTITY",
        init_gen=NumpyRandomGenerator(method="choice",
                                      a=[5, 15, 20, 25],
                                      p=[0.1, 0.2, 0.5, 0.2],
                                      seed=next(example1.seeder)))

    person.create_attribute(
        "other",
        init_gen=NumpyRandomGenerator(method="normal",
                                      scale=4, loc=6.,
                                      seed=next(example1.seeder)))

    person.create_attribute(
        "other",
        init_gen=NumpyRandomGenerator(method="exponential",
                                      scale=4,
                                      seed=next(example1.seeder)))

    logging.info("att created")

    person.create_attribute(
        "message",
        init_gen=ConstantGenerator(value="hello"))

    logging.info("saving")
    person.to_dataframe().to_csv("~/test.csv")

    logging.info("saved")
