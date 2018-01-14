from __future__ import division
import pandas as pd
import logging
import numpy as np

from trumania.core.util_functions import setup_logging, load_all_logs, build_ids, make_random_bipartite_data
from trumania.core.util_functions import make_random_assign, log_dataframe_sample
from trumania.core.random_generators import SequencialGenerator, NumpyRandomGenerator, ConstantGenerator
from trumania.core.random_generators import ParetoGenerator, FakerGenerator
from trumania.core.circus import Circus
from trumania.components.geographies.random_geo import WithRandomGeo
from trumania.components.time_patterns.profilers import HighWeekDaysTimerGenerator
from trumania.core import operations

# AgentA: has stock of SIMs
# AgentB: has stock of SIMs
# SIMs: has ID
# AgentA buys stock to AgentB

params = {
    "n_agents": 5000,

    # very low number of dealer, to have tons of collision and validate the
    # one-to-one behaviour in that case
    "n_dealers": 10,
    "n_distributors": 3,

    "average_agent_degree": 20,

    "n_init_sims_dealer": 1000,
    "n_init_sims_distributor": 500000,

    "output_folder": "snd_output_logs"
}


class SndScenario(WithRandomGeo, Circus):

    def __init__(self):
        Circus.__init__(self,
                        name="test_snd_scenario",
                        master_seed=1234,
                        start=pd.Timestamp("8 June 2016"),
                        step_duration=pd.Timedelta("60s"),)

        distributors, sim_generator = self.create_distributors_with_sims()
        self.add_distributor_recharge_story(distributors, sim_generator)
        dealers = self.create_dealers_and_sims_stock()
        agents = self.create_agents()
        self.connect_agent_to_dealer(agents, dealers)
        self.connect_dealers_to_distributors(dealers, distributors)

        self.add_agent_sim_purchase_story(agents, dealers)
        self.add_dealer_bulk_sim_purchase_story(dealers, distributors)
        self.add_agent_holidays_story(agents, )
        self.add_agent_reviews_stories(agents)

    def create_distributors_with_sims(self):
        """
        Distributors are similar to dealers, just with much more sims, and the
        stories to buy SIM from them is "by bulks" (though the story is not
        created here)
        """
        logging.info("Creating distributors and their SIM stock  ")

        distributors = self.create_population(
            name="distros",
            size=params["n_distributors"],
            ids_gen=SequencialGenerator(
                prefix="DISTRIBUTOR_",
                max_length=1))

        sims = distributors.create_relationship(name="SIM")

        sim_generator = SequencialGenerator(prefix="SIM_", max_length=30)
        sim_ids = sim_generator.generate(params["n_init_sims_distributor"])
        sims_dist = make_random_assign(set1=sim_ids, set2=distributors.ids,
                                       seed=next(self.seeder))
        sims.add_relations(from_ids=sims_dist["chosen_from_set2"],
                           to_ids=sims_dist["set1"])

        # this tells to the "restock" story of the distributor how many
        # sim should be re-generated to replenish the stocks
        distributors.create_attribute("SIMS_TO_RESTOCK", init_values=0)

        return distributors, sim_generator

    def add_distributor_recharge_story(self, distributors, sim_generator):
        """
        adds an story that increases the stock of distributor.
        This is triggered externaly by the bulk purchase story below
        """

        restocking = self.create_story(
            name="distributor_restock",
            initiating_population=distributors,
            member_id_field="DISTRIBUTOR_ID",

            # here again: no activity gen nor time profile here since the story
            # is triggered externally
        )

        restocking.set_operations(
            self.clock.ops.timestamp(named_as="DATETIME"),

            distributors.ops.lookup(
                id_field="DISTRIBUTOR_ID",
                select={"SIMS_TO_RESTOCK": "SIMS_TO_RESTOCK"}),

            sim_generator.ops.generate(
                named_as="NEW_SIMS_IDS",
                quantity_field="SIMS_TO_RESTOCK",),

            distributors.get_relationship("SIM").ops.add_grouped(
                from_field="DISTRIBUTOR_ID",
                grouped_items_field="NEW_SIMS_IDS"),

            # back to zero
            distributors.get_attribute("SIMS_TO_RESTOCK").ops.subtract(
                member_id_field="DISTRIBUTOR_ID",
                subtracted_value_field="SIMS_TO_RESTOCK"),

            operations.FieldLogger(log_id="distributor_restock",
                                   cols=["DATETIME", "DISTRIBUTOR_ID",
                                         "SIMS_TO_RESTOCK"]),
        )

    def create_dealers_and_sims_stock(self):
        """
        Create the DEALER population together with their init SIM stock
        """
        logging.info("Creating dealer and their SIM stock  ")

        dealers = self.create_population(name="dealers",
                                         size=params["n_dealers"],
                                         ids_gen=SequencialGenerator(
                                             prefix="DEALER_",
                                             max_length=3))

        # SIM relationship to maintain some stock
        sims = dealers.create_relationship(name="SIM")
        sim_ids = build_ids(size=params["n_init_sims_dealer"], prefix="SIM_")
        sims_dealer = make_random_assign(set1=sim_ids, set2=dealers.ids,
                                         seed=next(self.seeder))
        sims.add_relations(from_ids=sims_dealer["chosen_from_set2"],
                           to_ids=sims_dealer["set1"])

        # one more dealer with just 3 sims in stock => this one will trigger
        # lot's of failed sales
        broken_dealer = pd.DataFrame({
            "DEALER": "broke_dealer",
            "SIM": ["SIM_OF_BROKE_DEALER_%d" % s for s in range(3)]
        })

        sims.add_relations(from_ids=broken_dealer["DEALER"],
                           to_ids=broken_dealer["SIM"])

        return dealers

    def create_agents(self):
        """
        Create the AGENT population (i.e. customer) together with its "SIM" labeled
        stock, to keep track of which SIMs are own by which agent
        """
        logging.info("Creating agents ")

        agents = self.create_population(name="agents",
                                        size=params["n_agents"],
                                        ids_gen=SequencialGenerator(
                                            prefix="AGENT_",
                                            max_length=3))
        agents.create_relationship(name="SIM")

        agents.create_attribute(name="AGENT_NAME",
                                init_gen=FakerGenerator(seed=next(self.seeder),
                                                        method="name"))

        # note: the SIM multi-attribute is not initialized with any SIM: agents
        # start with no SIM

        return agents

    def connect_agent_to_dealer(self, agents, dealers):
        """
        Relationship from agents to dealers
        """
        logging.info("Randomly connecting agents to dealer ")

        deg_prob = params["average_agent_degree"] / params["n_agents"] * params["n_dealers"]

        agent_weight_gen = NumpyRandomGenerator(method="exponential", scale=1.,
                                                seed=1)

        agent_customer_df = pd.DataFrame.from_records(
            make_random_bipartite_data(agents.ids, dealers.ids, deg_prob,
                                       seed=next(self.seeder)),
            columns=["AGENT", "DEALER"])

        agent_customer_rel = agents.create_relationship(name="DEALERS")

        agent_customer_rel.add_relations(
            from_ids=agent_customer_df["AGENT"],
            to_ids=agent_customer_df["DEALER"],
            weights=agent_weight_gen.generate(agent_customer_df.shape[0]))

        # every agent is also connected to the "broke dealer", to make sure this
        # one gets out of stock quickly
        agent_customer_rel.add_relations(
            from_ids=agents.ids,
            to_ids=np.repeat("broke_dealer", agents.ids.shape),
            weights=4)

    def connect_dealers_to_distributors(self, dealers, distributors):

        # let's be simple: each dealer has only one provider
        distributor_rel = dealers.create_relationship("DISTRIBUTOR")

        state = np.random.RandomState(next(self.seeder))
        assigned = state.choice(a=distributors.ids, size=dealers.size, replace=True)

        distributor_rel.add_relations(from_ids=dealers.ids, to_ids=assigned)

        # We're also adding a "bulk buy size" attribute to each dealer that defines
        # how many SIM are bought at one from the distributor.
        bulk_gen = ParetoGenerator(xmin=500, a=1.5, force_int=True,
                                   seed=next(self.seeder))

        dealers.create_attribute("BULK_BUY_SIZE", init_gen=bulk_gen)

    def add_dealer_bulk_sim_purchase_story(self, dealers, distributors):
        """
        Adds a SIM purchase story from agents to dealer, with impact on stock of
        both populations
        """
        logging.info("Creating bulk purchase story")

        timegen = HighWeekDaysTimerGenerator(clock=self.clock,
                                             seed=next(self.seeder))

        purchase_activity_gen = ConstantGenerator(value=100)

        build_purchases = self.create_story(
            name="bulk_purchases",
            initiating_population=dealers,
            member_id_field="DEALER_ID",
            timer_gen=timegen,
            activity_gen=purchase_activity_gen)

        build_purchases.set_operations(
            self.clock.ops.timestamp(named_as="DATETIME"),

            dealers.get_relationship("DISTRIBUTOR").ops.select_one(
                from_field="DEALER_ID",
                named_as="DISTRIBUTOR"),

            dealers.ops.lookup(id_field="DEALER_ID",
                               select={"BULK_BUY_SIZE": "BULK_BUY_SIZE"}),

            distributors.get_relationship("SIM").ops.select_many(
                from_field="DISTRIBUTOR",
                named_as="SIM_BULK",
                quantity_field="BULK_BUY_SIZE",

                # if a SIM is selected, it is removed from the dealer's stock
                pop=True

                # (not modeling out-of-stock provider to keep the example simple...
            ),

            dealers.get_relationship("SIM").ops.add_grouped(
                from_field="DEALER_ID",
                grouped_items_field="SIM_BULK"),

            # not modeling money transfer to keep the example simple...

            # just logging the number of sims instead of the sims themselves...
            operations.Apply(source_fields="SIM_BULK",
                             named_as="NUMBER_OF_SIMS",
                             f=lambda s: s.map(len), f_args="series"),


            # finally, triggering some re-stocking by the distributor
            distributors.get_attribute("SIMS_TO_RESTOCK").ops.add(
                member_id_field="DISTRIBUTOR",
                added_value_field="NUMBER_OF_SIMS"),

            self.get_story("distributor_restock").ops.force_act_next(
                member_id_field="DISTRIBUTOR"),

            operations.FieldLogger(log_id="bulk_purchases",
                                   cols=["DEALER_ID", "DISTRIBUTOR",
                                         "NUMBER_OF_SIMS"]),
        )

    def add_agent_sim_purchase_story(self, agents, dealers):
        """
        Adds a SIM purchase story from agents to dealer, with impact on stock of
        both populations
        """
        logging.info("Creating purchase story")

        timegen = HighWeekDaysTimerGenerator(clock=self.clock,
                                             seed=next(self.seeder))

        purchase_activity_gen = NumpyRandomGenerator(
            method="choice", a=range(1, 4), seed=next(self.seeder))

        # TODO: if we merge profiler and generator, we could have higher probs here
        # based on calendar
        # TODO2: or not, maybe we should have a sub-operation with its own counter
        #  to "come back to normal", instead of sampling a random variable at
        #  each turn => would improve efficiency

        purchase = self.create_story(
            name="purchases",
            initiating_population=agents,
            member_id_field="AGENT",
            timer_gen=timegen,
            activity_gen=purchase_activity_gen,

            states={
                "on_holiday": {
                    "activity": ConstantGenerator(value=0),
                    "back_to_default_probability": ConstantGenerator(value=0)
                }
            }
        )

        purchase.set_operations(
            self.clock.ops.timestamp(named_as="DATETIME"),

            agents.get_relationship("DEALERS").ops.select_one(
                from_field="AGENT",
                named_as="DEALER"),

            dealers.get_relationship("SIM").ops.select_one(
                from_field="DEALER",
                named_as="SOLD_SIM",

                # each SIM can only be sold once
                one_to_one=True,

                # if a SIM is selected, it is removed from the dealer's stock
                pop=True,

                # If a chosen dealer has empty stock, we don't want to drop the
                # row in story_data, but keep it with a None sold SIM,
                # which indicates the sale failed
                discard_empty=False),

            operations.Apply(source_fields="SOLD_SIM",
                             named_as="FAILED_SALE",
                             f=pd.isnull, f_args="series"),

            # any agent who failed to buy a SIM will try again at next round
            # (we could do that probabilistically as well, just add a trigger..)
            purchase.ops.force_act_next(member_id_field="AGENT",
                                        condition_field="FAILED_SALE"),

            # not specifying the logged columns => by defaults, log everything
            # ALso, we log the sale before dropping to failed sales, to keep
            operations.FieldLogger(log_id="purchases"),

            # only successful sales actually add a SIM to agents
            operations.DropRow(condition_field="FAILED_SALE"),

            agents.get_relationship("SIM").ops.add(
                from_field="AGENT",
                item_field="SOLD_SIM"),
        )

    def add_agent_holidays_story(self, agents):
        """
        Adds stories that reset to 0 the activity level of the purchases
        story of some populations
        """
        logging.info("Adding 'holiday' periods for agents ")

        # TODO: this is a bit weird, I think what I'd need is a profiler that would
        # return duration (i.e timer count) with probability related to time
        # until next typical holidays :)
        # We could call this YearProfile though the internal mechanics would be
        # different than week and day profiler
        holiday_time_gen = HighWeekDaysTimerGenerator(clock=self.clock,
                                                      seed=next(self.seeder))

        # TODO: we'd obviously have to adapt those weight to longer periods
        # thought this interface is not very intuitive
        # => create a method where can can specify the expected value of the
        # inter-event interval, and convert that into an activity
        holiday_start_activity = ParetoGenerator(xmin=.25, a=1.2,
                                                 seed=next(self.seeder))

        holiday_end_activity = ParetoGenerator(xmin=150, a=1.2,
                                               seed=next(self.seeder))

        going_on_holidays = self.create_story(
            name="agent_start_holidays",
            initiating_population=agents,
            member_id_field="AGENT",
            timer_gen=holiday_time_gen,
            activity_gen=holiday_start_activity)

        returning_from_holidays = self.create_story(
            name="agent_stops_holidays",
            initiating_population=agents,
            member_id_field="AGENT",
            timer_gen=holiday_time_gen,
            activity_gen=holiday_end_activity,
            auto_reset_timer=False)

        going_on_holidays.set_operations(

            self.get_story("purchases").ops.transit_to_state(
                member_id_field="AGENT",
                state="on_holiday"),
            returning_from_holidays.ops.reset_timers(member_id_field="AGENT"),

            # just for the logs
            self.clock.ops.timestamp(named_as="TIME"),
            ConstantGenerator(value="going").ops.generate(named_as="STATES"),
            operations.FieldLogger(log_id="holidays"),
        )

        returning_from_holidays.set_operations(
            self.get_story("purchases").ops.transit_to_state(
                member_id_field="AGENT",
                state="default"),

            # just for the logs
            self.clock.ops.timestamp(named_as="TIME"),
            ConstantGenerator(value="returning").ops.generate(named_as="STATES"),
            operations.FieldLogger(log_id="holidays"),
        )

    def add_agent_reviews_stories(self, agents):
        """
        This illustrates the dynamic creation of new populations: reviews are modeled as "population"
         (even though they are mostly inactive data container) that are created dynamically
         and linked to agents.

        I guess most of the time reviews would be modeled as logs instead of populations, but
         let's just ignore that for illustration purposes... ^^
        """

        timegen = HighWeekDaysTimerGenerator(clock=self.clock,
                                             seed=next(self.seeder))

        review_activity_gen = NumpyRandomGenerator(
            method="choice", a=range(1, 4), seed=next(self.seeder))

        # the system starts with no reviews
        review_population = self.create_population(name="rev", size=0)
        review_population.create_attribute("DATE")
        review_population.create_attribute("TEXT")
        review_population.create_attribute("AGENT_ID")
        review_population.create_attribute("AGENT_NAME")

        reviews = self.create_story(
            name="agent_reviews",
            initiating_population=agents,
            member_id_field="AGENT",
            timer_gen=timegen,
            activity_gen=review_activity_gen,
        )

        review_id_gen = SequencialGenerator(start=0, prefix="REVIEW_ID")
        text_id_gen = FakerGenerator(method="text", seed=next(self.seeder))

        reviews.set_operations(
            self.clock.ops.timestamp(named_as="DATETIME"),

            agents.ops.lookup(id_field="AGENT",
                              select={"AGENT_NAME": "AGENT_NAME"}),

            review_id_gen.ops.generate(named_as="REVIEW_ID"),

            text_id_gen.ops.generate(named_as="REVIEW_TEXT"),

            review_population.ops.update(
                id_field="REVIEW_ID",
                copy_attributes_from_fields={
                    "DATE": "DATETIME",
                    "TEXT": "REVIEW_TEXT",
                    "AGENT_ID": "AGENT",
                    "AGENT_NAME": "AGENT_NAME",
                }
            ),

            # actually, here we're modelling review both as populations and logs..
            operations.FieldLogger(log_id="reviews")
        )


# having this method called "test_" makes it interpreted as a unit test
def test_snd_scenario():

    setup_logging()

    scenario = SndScenario()

    scenario.run(duration=pd.Timedelta("1h30m"), delete_existing_logs=True,
                 log_output_folder=params["output_folder"])
    logs = load_all_logs(params["output_folder"])

    for logid, lg in logs.items():
        log_dataframe_sample(" - some {}".format(logid), lg)

    purchases = logs["purchases"]
    log_dataframe_sample(" - some failed purchases: ",
                         purchases[purchases["FAILED_SALE"]])

    sales_of_broke = purchases[purchases["DEALER"] == "broke_dealer"]
    log_dataframe_sample(" - some purchases from broke dealer: ",
                         sales_of_broke)

    all_logs_size = np.sum(df.shape[0] for df in logs.values())
    logging.info("\ntotal number of logs: {}".format(all_logs_size))

    for logid, lg in logs.items():
        logging.info(" {} {} logs".format(len(lg), logid))

    # broke dealer should have maximum 3 successful sales
    ok_sales_of_broke = sales_of_broke[~sales_of_broke["FAILED_SALE"]]
    assert ok_sales_of_broke.shape[0] <= 3


if __name__ == "__main__":
    test_snd_scenario()
