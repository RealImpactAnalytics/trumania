"""
SND scenario, based on

https://realimpactanalytics.atlassian.net/wiki/pages/viewpage.action?spaceKey=SD&title=Tech+Design+%3A+Lab+Data+Generator
"""


from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
from datagenerator.components.geographies.belgium import *
import pandas as pd

from datetime import datetime
import snd_customers
import snd_sites
import snd_pos
import snd_dealer
import snd_field_agents

import logging


# scenario is meant to match scenario 1 divided by 100
scenario_0 = {
    "n_sites": 50,
    "n_customers": 50000,
    "n_field_agents": 2,
    "n_pos": 500,
    "n_dealers": 3,     # should be 1 if really  divided by 100

    "mean_known_sites_per_customer": 4,

    "mean_daily_customer_mobility_activity": 2,
    "std_daily_customer_mobility_activity": .5,

    "mean_daily_fa_mobility_activity": 5,
    "std_daily_fa_mobility_activity": .5,

    "clock_time_step": "15 min",
    "n_init_sim_per_pos": 100,
    "n_init_sim_per_dealer": 1000,
    "sim_price": 10,

    "simulation_start_date": "13 Sept 2016 12:00",
    "simulation_duration": "2 days",
    "output_folder": "snd_output_logs/scenario_0"
}


# to be removed: temporary downscaling of the scenario to accelerate tests
scenario_0.update({
    "n_sites": 50,
    "n_customers": 500,
    "n_pos": 100,
    "n_dealers": 100,
    "n_iterations": 20
})


class SND(WithBelgium):

    def __init__(self, params):
        Circus.__init__(
            self,
            master_seed=12345,
            output_folder=params["output_folder"],
            start=pd.Timestamp(params["simulation_start_date"]),
            step_duration=pd.Timedelta(params["clock_time_step"]))

        # using one central sim_id generator to guarantee unicity of ids
        sim_id_gen = SequencialGenerator(prefix="SIM_")

        self.sites = snd_sites.create_sites(self, params)
        self.customers = snd_customers.create_customers(self, params)
        snd_customers.add_mobility_action(self, params)

        self.dealers = snd_dealer.create_dealers(self, params, sim_id_gen)

        self.pos = snd_pos.create_pos(self, params, sim_id_gen)
        snd_pos.add_sim_bulk_purchase_action(self)
        snd_customers.add_purchase_sim_action(self, params)

        self.field_agents = snd_field_agents.create_field_agents(self, params)
        snd_field_agents.add_mobility_action(self, params)
        snd_field_agents.add_survey_action(self)


if __name__ == "__main__":

    params = scenario_0

    setup_logging()
    start_ts = pd.Timestamp(datetime.now())

    circus = SND(params)
    built_ts = pd.Timestamp(datetime.now())

    circus.run(pd.Timedelta(params["simulation_duration"]),
               delete_existing_logs=True)
    execution_ts = pd.Timestamp(datetime.now())
    logs = load_all_logs(params["output_folder"])

    for logid, log_df in logs.iteritems():
        logging.info(" - some {}:\n{}\n\n".format(
            logid,
            log_df.head(15).to_string()))

    all_logs_size = np.sum(df.shape[0] for df in logs.values())
    logging.info("total number of logs: {}".format(all_logs_size))

    for logid, log_df in logs.iteritems():
        first_ts = log_df["TIME"].apply(pd.Timestamp).min().isoformat()
        last_ts = log_df["TIME"].apply(pd.Timestamp).max().isoformat()
        logging.info(" {} {} logs from {} to {}".format(
            len(log_df), logid, first_ts, last_ts))

    logging.info("""execution times: "
     - building the circus: {}
     - running the simulation: {}
    """.format(built_ts - start_ts, execution_ts - built_ts))
