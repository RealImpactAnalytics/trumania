from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
from numpy.random import *

import snd_constants


def create_dealers(circus, params, sim_id_gen, recharge_id_gen):

    logging.info("creating dealers")
    dealers = Actor(size=params["n_dealers"],
                    ids_gen=SequencialGenerator(prefix="DEALER_"))

    dealers.create_stock_relationship(
        name="SIMS", item_id_gen=sim_id_gen,
        n_items_per_actor=params["n_init_sim_per_dealer"], seeder=circus.seeder)

    dealers.create_stock_relationship(
        name="ERS", item_id_gen=recharge_id_gen,
        n_items_per_actor=params["n_init_er_per_dealer"], seeder=circus.seeder)

    return dealers
