from datagenerator.core import circus
from datagenerator.core import util_functions
from datagenerator.core import random_generators
import logging

import snd_customers
import snd_sites
import snd_pos
import snd_dealer
import snd_field_agents
import patterns

import pandas as pd

static_params = {

    "circus_name": "snd_v2",

    "mean_known_sites_per_customer": 6,

    # clock_time_step limits the simulation in two ways:
    #  - it is impossible to have more than one event per actor per clock step
    #  - timestamps will typically be generated uniformly randomly inside a
    # clock step => resulting daily distributions will be as coarse grained as
    # the clock step.
    "clock_time_step": "1h",

    "clock_start_date": "13 Sept 2016 12:00",

    "products": {
        "SIM": {
            "pos_bulk_purchase_sizes": [5, 10, 15],
            "pos_bulk_purchase_sizes_dist": [.5, .3, .2],
            "telco_init_stock_customer_ratio": .05
        },
        # electronic recharges
        "ER": {
            "pos_init_distro": "stock_distro_notebook/max_stock500_bulk_100_200_450",
            "pos_bulk_purchase_sizes": [100, 200, 450],
            "pos_bulk_purchase_sizes_dist": [.4, .3, .3],
            "telco_init_stock_customer_ratio": 1
        },
        # physical recharges
        "PR": {
            "pos_bulk_purchase_sizes": [50, 100, 225],
            "pos_bulk_purchase_sizes_dist": [.4, .3, .3],
            "telco_init_stock_customer_ratio": .5
        },
        "MFS": {
            "pos_bulk_purchase_sizes": [50, 75, 200],
            "pos_bulk_purchase_sizes_dist": [.4, .4, .2],
            "telco_init_stock_customer_ratio": .2
        },
        "HANDSET": {
            "pos_bulk_purchase_sizes": [5, 10],
            "pos_bulk_purchase_sizes_dist": [.5, .5],
            "telco_init_stock_customer_ratio": .05
        },
    },


    # "geography": "belgium",
    "geography": "belgium_5",
    # "n_pos": 50000,
    "n_pos": 100,

    # "n_dealers_l2": 1000,
    "n_dealers_l2": 20,
    "n_dealers_l1": 2,
    "n_telcos": 1,

    "n_field_agents": 500,

    # "n_customers": 5000000,
    "n_customers": 10000,
}

if __name__ == "__main__":

    util_functions.setup_logging()

    snd = circus.Circus(
        name=static_params["circus_name"],
        master_seed=12345,
        start=pd.Timestamp(static_params["clock_start_date"]),
        step_duration=pd.Timedelta(static_params["clock_time_step"]))

    distributor_id_gen = random_generators.SequencialGenerator(prefix="DIST_")

    snd_sites.add_sites(snd, static_params)
    snd_customers.add_customers(snd, static_params)

    snd_pos.add_pos(snd, static_params)
    snd_dealer.add_telcos(snd, static_params, distributor_id_gen)
    snd_dealer.create_dealers(snd,
                              actor_name="dealers_l1", actor_size=static_params["n_dealers_l1"],
                              params=static_params, actor_id_gen=distributor_id_gen)
    snd_dealer.create_dealers(snd,
                              actor_name="dealers_l2", actor_size=static_params["n_dealers_l2"],
                              params=static_params, actor_id_gen=distributor_id_gen)

    patterns.create_distribution_link(snd,
                                      from_actor_name="pos",
                                      to_actor_name="dealers_l2")
    patterns.create_distribution_link(snd,
                                      from_actor_name="dealers_l2",
                                      to_actor_name="dealers_l1")
    patterns.create_distribution_link(snd,
                                      from_actor_name="dealers_l1",
                                      to_actor_name="telcos")

    snd_field_agents.create_field_agents(snd, static_params)

    logging.info("created circus:\n{}".format(snd))
    snd.save_to_db(overwrite=True)

