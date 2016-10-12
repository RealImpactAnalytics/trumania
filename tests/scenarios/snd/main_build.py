from datagenerator.core import circus
from datagenerator.core import util_functions
from datagenerator.core import random_generators
import logging

import snd_customers
import snd_sites
import snd_pos
import snd_dealer
import snd_field_agents


import pandas as pd

static_params = {

    "circus_name": "snd_v1",

    "mean_known_sites_per_customer": 6,

    # clock_time_step limits the simulation in two ways:
    #  - it is impossible to have more than one event per actor per clock step
    #  - timestamps will typically be generated uniformly randomly inside a
    # clock step => resulting daily distributions will be as coarse grained as
    # the clock step.
    "clock_time_step": "1h",

    "clock_start_date": "13 Sept 2016 12:00",

    # empirical distribution of pos initial stock level
    "pos_init_er_stock_distro": "stock_distro_notebook/max_stock500_bulk_100_200_450",

    # distribution of POS's bulk size when buying SIMs
    "pos_sim_bulk_purchase_sizes": [10, 15, 25],
    "pos_sim_bulk_purchase_sizes_dist": [.5, .3, .2],

    # distribution of POS's bulk size when buying electronic recharges
    "pos_ers_bulk_purchase_sizes": [100, 200, 450],
    "pos_ers_bulk_purchase_sizes_dist": [.4, .3, .3],

    "geography": "belgium_500",
    "n_pos": 1000,

    "n_dealers_l2": 100,
    "n_dealers_l1": 25,
    "n_telcos": 1,

    "n_field_agents": 100,

    "n_customers": 50000,
}

if __name__ == "__main__":

    util_functions.setup_logging()

    snd = circus.Circus(
        name=static_params["circus_name"],
        master_seed=12345,
        start=pd.Timestamp(static_params["clock_start_date"]),
        step_duration=pd.Timedelta(static_params["clock_time_step"]))

    # Generators of item ids, used during the constructions of
    # the circus as well as during the execution of the actions => attached
    # to the circus, with their state, to ensure full reproduceability
    recharge_id_gen = random_generators.SequencialGenerator(prefix="ER_")
    snd.attach_generator("recharge_id_gen", recharge_id_gen)
    sim_id_gen = random_generators.SequencialGenerator(prefix="SIM_")
    snd.attach_generator("sim_id_gen", sim_id_gen)
    distributor_id_gen = random_generators.SequencialGenerator(prefix="DIST_")
    snd.attach_generator("distributor_id_gen", distributor_id_gen)

    snd_sites.add_sites(snd, static_params)
    snd_customers.add_customers(snd, static_params)

    snd_dealer.add_telcos(snd, static_params, distributor_id_gen, sim_id_gen,
                          recharge_id_gen)

    snd_pos.add_pos(snd, static_params, sim_id_gen, recharge_id_gen)
    snd_dealer.create_dealers_l1(snd, static_params, distributor_id_gen,
                                 sim_id_gen, recharge_id_gen)

    snd_dealer.create_dealers_l2(snd, static_params, distributor_id_gen,
                                 sim_id_gen, recharge_id_gen)

    snd_pos.connect_pos_to_dealer_l1(snd)

    snd_field_agents.create_field_agents(snd, static_params)

    logging.info("created circus:\n{}".format(snd))
    snd.save_to_db(overwrite=True)

