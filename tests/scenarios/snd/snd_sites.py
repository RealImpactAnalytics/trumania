import logging
from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
import pandas as pd
from numpy.random import *


def create_sites(circus, params):

    logging.info("adding sites")
    sites = Actor(ids_gen=SequencialGenerator(prefix="SITE_"),
                  size=params["n_sites"])

    cells_of_site_rel = sites.create_relationship("CELLS",
                                              seed=circus.seeder.next())

    latitude_generator = FakerGenerator(method="latitude",
                                        seed=circus.seeder.next())
    longitude_generator = FakerGenerator(method="longitude",
                                         seed=circus.seeder.next())

    sites.create_attribute("LATITUDE", init_gen=latitude_generator)
    sites.create_attribute("LONGITUDE", init_gen=longitude_generator)


    # between 1 and 9 cells per site
    logging.info("populating CELLS of SITES ")

    def gen_cell_ids(site_id):
        return ["CELL_%s_%d" % (site_id[5:], cid) for cid in
                range(np.random.choice(range(1, 10)))]

    cell_ids = sites.ids.to_series().apply(gen_cell_ids).tolist()
    cells_of_sites_df = pd.DataFrame(
            data=cell_ids,
            index=sites.ids).stack()

    cells_of_sites_df = cells_of_sites_df.reset_index(level=0)

    cells_of_sites_df.columns = ["SITE_ID", "CELL_ID"]

    cells_of_site_rel.add_relations(
        from_ids=cells_of_sites_df["SITE_ID"],
        to_ids=cells_of_sites_df["CELL_ID"]
    )

    return sites
