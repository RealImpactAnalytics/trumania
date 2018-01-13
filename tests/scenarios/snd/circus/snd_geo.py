import logging
import numpy as np
import pandas as pd
import os
from trumania.components import db
import trumania.core.operations as operations


def load_geo_actors(circus, params):
    logging.info("loading Belgium sites, cells and distribution network")
    circus.load_population(namespace=params["geography"], population_id="sites")
    circus.load_population(namespace=params["geography"], population_id="dist_l1")
    circus.load_population(namespace=params["geography"], population_id="dist_l2")


def build_site_product_pos_target(circus, params):
    """
    Generates some random target of amount of pos per site, based on the
    actual number of pos per site
    """

    target_file = os.path.join(
        db.namespace_folder(circus.name),
        "site_product_pos_target.csv")

    sites = circus.actors["sites"]

    target_action = operations.Chain(
        sites.relationships["POS"].ops.get_neighbourhood_size(
            from_field="site_id",
            named_as="pos_count_target"
        ),
        operations.FieldLogger(log_id="logs")
    )

    sites_df = pd.DataFrame({"site_id": sites.ids})

    _, logs = target_action(sites_df)

    target_df = logs["logs"]
    target_df["cartesian_product"] = "cp"

    products = pd.DataFrame({
        "product_type_id": params["products"].keys(),
        "cartesian_product": "cp"
    })

    target_df = pd.merge(left=target_df, right=products, on="cartesian_product")

    fact = np.random.normal(1, .1, size=target_df.shape[0])
    target_df["pos_count_target"] = target_df["pos_count_target"] * fact
    target_df["pos_count_target"] = target_df["pos_count_target"].astype(np.int)

    target_df.ix[target_df["pos_count_target"] < 10, "pos_count_target"] = 10
    target_df.drop(["cartesian_product"], axis=1, inplace=True)

    target_df.to_csv(target_file, index=False)
