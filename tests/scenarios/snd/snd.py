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


scenario_1 = {

    "mean_known_sites_per_customer": 6,

    "mean_daily_customer_mobility_activity": .2,
    "std_daily_customer_mobility_activity": .2,

    "mean_daily_fa_mobility_activity": 1,
    "std_daily_fa_mobility_activity": .2,

    # average number of days between 2 item purchase by the same customer
    "customer_er_purchase_min_period_days": 1,
    "customer_er_purchase_max_period_days": 9,

    "customer_sim_purchase_min_period_days": 60,
    "customer_sim_purchase_max_period_days": 360,

    # clock_time_step limits the simulation in two ways:
    #  - it is impossible to have more than one event per actor per clock step
    #  - timestamps will typically be generated uniformly randomly inside a
    # clock step => resulting daily distributions will be as coarse grained as
    # the clock step.
    "clock_time_step": "1h",

    "sim_price": 10,

    # There are no parameters for the SIM purchases, we just scale the ERS
    # down by a factor 100 as a simplification for now
    "ers_to_sim_ratio": 100,

    # empirical distribution of pos initial stock level
    "pos_init_er_stock_distro": "stock_distro_notebook/max_stock500_bulk_100_200_450",

    "pos_ers_max_stock": 500,
    "pos_sim_max_stock": 500,

    # distribution of POS's bulk size when buying SIMs
    "pos_sim_bulk_purchase_sizes": [10, 15, 25],
    "pos_sim_bulk_purchase_sizes_dist": [.5, .3, .2],

    # distribution of POS's bulk size when buying electronic recharges
    "pos_ers_bulk_purchase_sizes": [100, 200, 450],
    "pos_ers_bulk_purchase_sizes_dist": [.4, .3, .3],

    # largest possible er or SIM stock level that can trigger a restock
    "max_pos_er_stock_triggering_restock": 50,
    "pos_er_restock_shape": 2,
    "max_pos_sim_stock_triggering_restock": 10,
    "pos_sim_restock_shape": 5,

    # distribution of SIM and ERs bulk size bought by POS
    # "mean_pos_sim_bulk_purchase_size": 1000,
    # "std_pos_sim_bulk_purchase_size": 100,
    #
    # "mean_pos_ers_bulk_purchase_size": 1000,
    # "std_pos_ers_bulk_purchase_size": 100,

    "simulation_start_date": "13 Sept 2016 12:00",
    "simulation_duration": "60 days",
    "output_folder": "snd_output_logs/scenario_0",
}


# temporary downscaling of the scenario to accelerate tests
scenario_1.update({
    "geography": "belgium_5",

    "n_customers": 10000,
    "n_pos": 50,
    "n_dealers_l2": 15,
    "n_dealers_l1": 1,

    "n_field_agents": 10,

    "n_telcos": 1,

    "simulation_duration": "5 days",
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
        distributor_id_gen = SequencialGenerator(prefix="DIST_")
        sim_id_gen = SequencialGenerator(prefix="SIM_")
        recharge_id_gen = SequencialGenerator(prefix="ER_")

        self.sites = snd_sites.create_sites(self, params)
        self.customers = snd_customers.create_customers(self, params)
        snd_customers.add_mobility_action(self, params)

        self.pos = snd_pos.create_pos(self, params, sim_id_gen, recharge_id_gen)

        self.telcos = snd_dealer.create_telcos(self, params,
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
