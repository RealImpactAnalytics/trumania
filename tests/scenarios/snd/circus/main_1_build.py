from trumania.core import circus
from trumania.core import util_functions
from trumania.core import random_generators
import logging

import snd_customers
import snd_pos
import snd_dealer
import snd_field_agents
import snd_geo
import snd_products

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

    "clock_start_date": "1 Jan 2016 00:00",

    "products": {
        "sim": {
            "pos_bulk_purchase_sizes": [5, 10, 15],
            "pos_bulk_purchase_sizes_dist": [.5, .3, .2],
            "telco_init_stock_customer_ratio": .05,

            "product_types_num": 5,
            "prefix": "SIM"
        },

        "electronic_recharge": {
            # "pos_init_distro":
            # "stock_distro_notebook/max_stock500_bulk_100_200_450",
            # "pos_bulk_purchase_sizes": [100, 200, 450],
            "pos_bulk_purchase_sizes": [10, 20, 45],
            "pos_bulk_purchase_sizes_dist": [.4, .3, .3],
            "telco_init_stock_customer_ratio": 1,

            "product_types_num": 1,
            "prefix": "ER"
        },

        "physical_recharge": {
            # "pos_bulk_purchase_sizes": [50, 100, 225],
            "pos_bulk_purchase_sizes": [5, 10, 25],
            "pos_bulk_purchase_sizes_dist": [.4, .3, .3],
            "telco_init_stock_customer_ratio": .5,

            "product_types_num": 1,
            "prefix": "PR"
        },

        "mfs": {
            # "pos_bulk_purchase_sizes": [50, 75, 200],
            "pos_bulk_purchase_sizes": [5, 7, 20],
            "pos_bulk_purchase_sizes_dist": [.4, .4, .2],
            "telco_init_stock_customer_ratio": .2,

            "product_types_num": 1,
            "prefix": "MFS"

        },

        "handset": {
            "pos_bulk_purchase_sizes": [5, 10],
            "pos_bulk_purchase_sizes_dist": [.5, .5],
            "telco_init_stock_customer_ratio": .05,

            "product_types_num": 100,
            "prefix": "HS"
        },
    },

    "geography": "belgium",
    "n_pos": 10000,

    # These are already declared in the geography
    # "n_dealers_l2": 25,
    # "n_dealers_l1": 4,

    "n_telcos": 1,

    "n_field_agents": 100,

    "n_customers": 20000,
}

if __name__ == "__main__":

    util_functions.setup_logging()

    snd = circus.Circus(
        name=static_params["circus_name"],
        master_seed=12345,
        start=pd.Timestamp(static_params["clock_start_date"]),
        step_duration=pd.Timedelta(static_params["clock_time_step"]))

    distributor_id_gen = random_generators.SequencialGenerator(prefix="DIST_")

    snd_products.create_products(snd, static_params)

    snd_geo.load_geo_actors(snd, static_params)
    snd_customers.add_customers(snd, static_params)

    snd_pos.add_pos(snd, static_params)

    snd_dealer.add_telcos(snd, static_params, distributor_id_gen)
    snd_dealer.prepare_dealers(snd, params=static_params)

    snd_field_agents.create_field_agents(snd, static_params)

    logging.info("created circus:\n{}".format(snd))
    snd.save_to_db(overwrite=True)
    snd.save_params_to_db("build", static_params)

    # supplementary output required for SND but not for the simulation
    snd_pos.save_pos_as_mobile_sync_csv(snd)
    snd_pos.save_pos_as_partial_ids_csv(snd, static_params)
    snd_geo.build_site_product_pos_target(snd, static_params)
    snd_dealer.save_providers_csv(snd, static_params)
