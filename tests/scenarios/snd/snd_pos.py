from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
from numpy.random import *

import snd_constants

def create_pos(circus, params, sim_id_gen):

    logging.info("creating POS")
    pos = Actor(size=params["n_pos"],
                ids_gen=SequencialGenerator(prefix="POS_"))

    # TODO we need 2 info for attractiveness: a real number in [-inf, +inf],
    # and its sigmoid transform into [0,1]
    logging.info("generating pos attractiveness")
    attract_gen = NumpyRandomGenerator(method="uniform", low=0.0, high=1.0,
                                       seed=circus.seeder.next())
    pos.create_attribute("ATTRACTIVENESS", init_gen=attract_gen)

    logging.info("assigning a site to each POS")
    state = RandomState(circus.seeder.next())
    pos_sites = state.choice(a=circus.sites.ids,
                             size=pos.size,
                             replace=True)
    pos.create_attribute("SITE", init_values=pos_sites)

    # TODO: Add POS coordinates based on SITE coordinates
    pos.create_attribute("LATITUDE", init_gen=ConstantGenerator(0.0))
    pos.create_attribute("LONGITUDE", init_gen=ConstantGenerator(0.0))

    name_gen = NumpyRandomGenerator(method="choice",
                                    seed=circus.seeder.next(),
                                    a=snd_constants.POS_NAMES)

    pos.create_attribute("NAME", init_gen=name_gen)

    logging.info("recording the list POS per site in site relationship")
    pos_rel = circus.sites.create_relationship("POS",
                                               seed=circus.seeder.next())
    pos_rel.add_relations(
        from_ids=pos.get_attribute_values("SITE", pos.ids),
        to_ids=pos.ids,
        weights=pos.get_attribute_values("ATTRACTIVENESS", pos.ids))

    logging.info("generating POS initial SIM stock")
    pos_sims = pos.create_relationship("SIMS", seed=circus.seeder.next())
    sim_ids = sim_id_gen.generate(size=params["n_init_sim_per_pos"] * pos.size)

    sims_dealer = make_random_assign(
        to_ids=sim_ids,
        from_ids=pos.ids,
        seed=circus.seeder.next())

    pos_sims.add_relations(
        from_ids=sims_dealer["from"],
        to_ids=sims_dealer["to"])

    return pos
