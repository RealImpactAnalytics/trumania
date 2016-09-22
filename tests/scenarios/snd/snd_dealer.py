from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
from numpy.random import *

import snd_constants


def create_dealers(circus, params, sim_id_gen):

    logging.info("creating dealers")
    dealers = Actor(size=params["n_dealers"],
                ids_gen=SequencialGenerator(prefix="DEALER_"))

    logging.info(" generating dealer initial SIM stock")
    dealer_sims = dealers.create_relationship("SIMS", seed=circus.seeder.next())
    sim_ids = sim_id_gen.generate(
        size=params["n_init_sim_per_dealer"] * dealers.size)

    sims_dealer = make_random_assign(
        set1=sim_ids,
        set2=dealers.ids,
        seed=circus.seeder.next())

    dealer_sims.add_relations(
        from_ids=sims_dealer["chosen_from_set2"],
        to_ids=sims_dealer["set1"])

    return dealers
