
from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
import pandas as pd

import logging

params = {
    "n_sites": 500,
    "n_customers": 5000
}


class SND(Circus):

    def __init__(self):
        Circus.__init__(
            self,
            master_seed=12345,
            start=pd.Timestamp("13 Sept 2016 12:00"),
            step_s=300)

        self.sites = self.create_sites()
        self.customers = self.create_customers()
        self.add_mobility()

    def create_sites(self):
        logging.info(" adding sites")
        sites = Actor(ids_gen=SequencialGenerator(prefix="SITE_"),
                      size=params["n_sites"])
        return sites

    def create_customers(self):

        logging.info(" adding customers")

        customers = Actor(size=params["n_customers"],
                     ids_gen=SequencialGenerator(prefix="CUST_"))

        logging.info(" adding mobility relationships to customers")

        mobility_rel = customers.create_relationship(
            "POSSIBLE_CELLS",
            seed=self.seeder.next())

        mobility_df = pd.DataFrame.from_records(
            make_random_bipartite_data(customers.ids,
                                       self.sites.ids,
                                       0.4,
                                       seed=self.seeder.next()),
            columns=["CID", "SID"])

        mobility_weight_gen = NumpyRandomGenerator(
            method="exponential", scale=1., seed=self.seeder.next())

        mobility_rel.add_relations(
            from_ids=mobility_df["CID"],
            to_ids=mobility_df["SID"],
            weights=mobility_weight_gen.generate(mobility_df.shape[0]))

        # Initialize the mobility by allocating one first random cell to each
        # customer among its network
        customers.create_attribute(name="CURRENT_CELL",
                                        init_relationship="POSSIBLE_CELLS")

        return customers

    def add_mobility(self):

        logging.info(" creating mobility action")

        # TODO: post-generation check: is the actual move set realistic? 
        mov_prof = [1., 1., 1., 1., 1., 1., 1., 1., 5., 10., 5., 1., 1., 1., 1.,
                    1., 1., 5., 10., 5., 1., 1., 1., 1.]
        mobility_time_gen = CyclicTimerGenerator(
            clock=self.clock,
            profile=mov_prof,
            profile_time_steps="1H",
            start_date=pd.Timestamp("12 September 2016 00:00.00"),
            seed=self.seeder.next())

        mobility_action = self.create_action(
            name="mobility",

            initiating_actor=self.customers,
            actorid_field="CUST_ID",

            timer_gen=mobility_time_gen,
        )

        logging.info(" adding operations")

        mobility_action.set_operations(
            self.customers.ops.lookup(
                actor_id_field="CUST_ID",
                select={"CURRENT_CELL": "PREV_CELL"}),

            # selects a destination cell (or maybe the same as current... ^^)

            self.customers \
                .get_relationship("POSSIBLE_CELLS") \
                .ops.select_one(from_field="CUST_ID", named_as="NEW_CELL"),

            # update the CELL attribute of the customers accordingly
            self.customers \
                .get_attribute("CURRENT_CELL") \
                .ops.update(
                    actor_id_field="CUST_ID",
                    copy_from_field="NEW_CELL"),

            self.clock.ops.timestamp(named_as="TIME"),

            # create mobility logs
            operations.FieldLogger(log_id="mobility_logs",
                                   cols=["TIME", "CUST_ID", "PREV_CELL",
                                         "NEW_CELL"]),
        )


if __name__ == "__main__":

    setup_logging()
    circus = SND()

    logs = circus.run(n_iterations=10)

    for logid, log_df in logs.iteritems():
        logging.info(" - some {}:\n{}\n\n".format(
            logid,
            log_df.head(15).to_string()))

