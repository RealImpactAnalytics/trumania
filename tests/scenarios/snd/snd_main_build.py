from datagenerator.core import circus
from datagenerator.core import util_functions

import snd_sites

import pandas as pd

scenario = {

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


    "geography": "belgium_500",
    "n_pos": 5000,

    "n_dealers_l2": 500,
    "n_dealers_l1": 25,
    "n_telcos": 1,

    "n_field_agents": 100,

    "n_customers": 500000,

    "simulation_start_date": "13 Sept 2016 12:00",
    "simulation_duration": "60 days"
}

if __name__ == "__main__":

    util_functions.setup_logging()

    snd = circus.Circus(
        name="snd_v1",
        master_seed=12345,
        start=pd.Timestamp(scenario["simulation_start_date"]),
        step_duration=pd.Timedelta(scenario["clock_time_step"]))

    snd_sites.add_sites(snd, scenario)

    snd.save_to_db(overwrite=True)





