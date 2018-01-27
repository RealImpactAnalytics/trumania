import logging
import pandas as pd
from tabulate import tabulate 

from trumania.core.circus import Circus
from trumania.core import circus, operations
from trumania.core.random_generators import SequencialGenerator, FakerGenerator, NumpyRandomGenerator, ConstantDependentGenerator, ConstantGenerator
import trumania.core.util_functions as util_functions
from trumania.components.time_patterns.profilers import DefaultDailyTimerGenerator
from trumania.components.social_networks.erdos_renyi import WithErdosRenyi

util_functions.setup_logging()

class Calling_scenario(WithErdosRenyi, Circus):


    def __init__(self):

        Circus.__init__(self,
            name="example", 
            master_seed=12345,
            start=pd.Timestamp("1 Jan 2017 00:00"),
            step_duration=pd.Timedelta("1h"))

        self._add_person_population()
       
        self.add_er_social_network_relationship(
            self.populations["person"],
            relationship_name="friends",
            average_degree=20)

        self._add_message_story()

    def _add_person_population(self):

        id_gen = SequencialGenerator(prefix="PERSON_")
        age_gen = NumpyRandomGenerator(method="normal", loc=3, scale=5,
                                       seed=next(self.seeder))
        name_gen = FakerGenerator(method="name", seed=next(self.seeder))

        person = self.create_population(name="person", size=1000, ids_gen=id_gen)
        person.create_attribute("NAME", init_gen=name_gen)
        person.create_attribute("AGE", init_gen=age_gen)

        quote_generator = FakerGenerator(method="sentence", nb_words=6, variable_nb_words=True,
                                         seed=next(self.seeder))

        quotes_rel = self.populations["person"].create_relationship("quotes")

        for w in range(4):
            quotes_rel.add_relations(
                from_ids=person.ids,
                to_ids=quote_generator.generate(size=person.size),
                weights=w
            )

    def _add_message_story(self):

        story_timer_gen = DefaultDailyTimerGenerator(
            clock=self.clock, 
            seed=next(self.seeder))

        low_activity = story_timer_gen.activity(n=3, per=pd.Timedelta("1 day"))
        med_activity = story_timer_gen.activity(n=10, per=pd.Timedelta("1 day"))
        high_activity = story_timer_gen.activity(n=20, per=pd.Timedelta("1 day"))

        activity_gen = NumpyRandomGenerator(
            method="choice", 
            a=[low_activity, med_activity, high_activity],
            p=[.2, .7, .1],
            seed=next(self.seeder))

        hello_world = self.create_story(
            name="hello_world",
            initiating_population=self.populations["person"],
            member_id_field="PERSON_ID",

            timer_gen=story_timer_gen,
            activity_gen=activity_gen
        )

        hello_world.set_operations(
            self.clock.ops.timestamp(named_as="TIME"),
            
            self.populations["person"].get_relationship("quotes")
                .ops.select_one(from_field="PERSON_ID",named_as="MESSAGE"),
            
            self.populations["person"]
                .get_relationship("friends")
                .ops.select_one(from_field="PERSON_ID", named_as="OTHER_PERSON"),

            self.populations["person"]
                .ops.lookup(id_field="PERSON_ID", select={"NAME": "EMITTER_NAME"}),

            self.populations["person"]
                .ops.lookup(id_field="OTHER_PERSON", select={"NAME": "RECEIVER_NAME"}),

            operations.FieldLogger(log_id="hello_6")
        )

# message story
example = Calling_scenario()

example.run(
    duration=pd.Timedelta("72h"),
    log_output_folder="output/example_scenario",
    delete_existing_logs=True
)

# -- DEBUG output printout
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
df = pd.read_csv("output/example_scenario/hello_6.csv")
print(df.head(10))
print(df.tail(10))
