
from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
import pandas as pd

from datetime import datetime
import snd_customers
import snd_sites
import snd_pos

import logging

params = {
    "n_sites": 500,
    "n_customers": 5000,
    "n_pos": 1000,

    # low initial number of SIM per POS to trigger bulk recharges
    "n_init_sim_per_pos": 100
}


class SND(Circus):

    def __init__(self):
        Circus.__init__(
            self,
            master_seed=12345,
            start=pd.Timestamp("13 Sept 2016 12:00"),
            step_s=300)

        sim_id_gen = SequencialGenerator(prefix="SIM_")

        self.sites = snd_sites.create_sites(self, params)
        self.customers = snd_customers.create_customers(self, params)
        snd_customers.add_mobility_action(self)
        self.pos = snd_pos.create_pos(self, params, sim_id_gen)
        snd_customers.add_purchase_sim_action(self)


if __name__ == "__main__":

    setup_logging()
    start_ts = pd.Timestamp(datetime.now())

    circus = SND()
    built_ts = pd.Timestamp(datetime.now())

    logs = circus.run(n_iterations=10)
    execution_ts = pd.Timestamp(datetime.now())

    for logid, log_df in logs.iteritems():
        logging.info(" - some {}:\n{}\n\n".format(
            logid,
            log_df.head(15).to_string()))

    all_logs_size = np.sum(df.shape[0] for df in logs.values())
    logging.info("total number of logs: {}".format(all_logs_size))

    logging.info("""execution times: "
     - building the circus: {}
     - running the simulation: {}
    """.format(built_ts - start_ts, execution_ts - built_ts))



