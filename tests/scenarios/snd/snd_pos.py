import logging
from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
import pandas as pd
from numpy.random import *


def create_pos(circus, params):

    logging.info("creating POS")
    pos = Actor(size=params["n_pos"],
                ids_gen=SequencialGenerator(prefix="POS_"))

    state = RandomState(circus.seeder.next())
    pos_sites = state.choice(a=circus.sites.ids,
                             size=pos.size,
                             replace=True)

    logging.info("assigning a site to each POS")
    pos.create_attribute("SITE", init_values=pos_sites)

    logging.info("recording the list POS per site in site relationship")
    pos_rel = circus.sites.create_relationship("POS", seed=circus.seeder.next())
    pos_rel.add_relations(
        from_ids=pos.get_attribute_values("SITE", pos.ids),
        to_ids=pos.ids)

    return pos