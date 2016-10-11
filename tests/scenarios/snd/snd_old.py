"""
SND scenario, based on

https://realimpactanalytics.atlassian.net/wiki/pages/viewpage.action?spaceKey=SD&title=Tech+Design+%3A+Lab+Data+Generator

https://realimpactanalytics.atlassian.net/wiki/pages/viewpage.action?pageId=73958895

and

https://realimpactanalytics.atlassian.net/browse/OASD-1593
"""


from datagenerator.core.circus import *
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


class SND(WithBelgium):

    def __init__(self, params):
        Circus.__init__(
            self,
            master_seed=12345,
            output_folder=params["output_folder"],
            start=pd.Timestamp(params["simulation_start_date"]),
            step_duration=pd.Timedelta(params["clock_time_step"]))

        # using one central sim_id generator to guarantee unicity of ids
        distributor_id_gen = SequencialGenerator(prefix="DIST_")
        sim_id_gen = SequencialGenerator(prefix="SIM_")
        recharge_id_gen = SequencialGenerator(prefix="ER_")

        self.sites = snd_sites.add_sites(self, params)     #
        self.customers = snd_customers.add_customers(self, params)  #
        snd_customers.add_mobility_action(self, params)          #
        self.pos = snd_pos.add_pos(self, params, sim_id_gen, recharge_id_gen)

        self.telcos = snd_dealer.add_telcos(self, params,
                                            distributor_id_gen,
                                            sim_id_gen,
                                            recharge_id_gen)

        self.dealers_l1 = snd_dealer.create_dealers_l1(self, params,
                                                       distributor_id_gen,
                                                       sim_id_gen,
                                                       recharge_id_gen)

        self.dealers_l2 = snd_dealer.create_dealers_l2(self, params,
                                                       distributor_id_gen,
                                                       sim_id_gen,
                                                       recharge_id_gen)

        snd_pos.connect_pos_to_dealers(self)
        snd_pos.add_sim_bulk_purchase_action(self, params)
        snd_pos.add_ers_bulk_purchase_action(self, params)
        snd_customers.add_purchase_sim_action(self, params)
        snd_customers.add_purchase_er_action(self, params)

        self.field_agents = snd_field_agents.create_field_agents(self, params)
        snd_field_agents.add_mobility_action(self, params)
        snd_field_agents.add_survey_action(self)


if __name__ == "__main__":

    run_params = scenario_1

    setup_logging()
    start_ts = pd.Timestamp(datetime.now())

    circus = SND(run_params)
    built_ts = pd.Timestamp(datetime.now())

    circus.run(pd.Timedelta(run_params["simulation_duration"]),
               delete_existing_logs=True)
    execution_ts = pd.Timestamp(datetime.now())
    logs = load_all_logs(run_params["output_folder"])

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
