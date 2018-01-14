from __future__ import division
import logging
import pandas as pd
import patterns
import snd_constants
import os

from trumania.core.operations import FieldLogger
from trumania.core.random_generators import ConstantGenerator, DependentBulkGenerator, FakerGenerator
import trumania.components.db as db


def add_telcos(circus, params, distributor_id_gen):

    logging.info("creating telcos")
    telcos = circus.create_population(name="telcos", size=params["n_telcos"],
                                      ids_gen=distributor_id_gen)

    for product, description in params["products"].items():

        logging.info("generating telco initial {} stock".format(product))
        init_stock_size = params["n_customers"] * description["telco_init_stock_customer_ratio"]
        product_id_gen = circus.generators["{}_id_gen".format(product)]
        int_stock_gen = ConstantGenerator(value=init_stock_size)\
            .flatmap(DependentBulkGenerator(element_generator=product_id_gen))

        telcos.create_stock_relationship_grp(name=product,
                                             stock_bulk_gen=int_stock_gen)


def add_telco_restock_actions(circus, params):
    """
    Add actions to "restock" the ERS and SIM products, i.e. create new items
    when needed (Telco do not buy products from anyone in this model,
    they just create them).

    :param circus:
    :param params:
    :return:
    """

    telcos = circus.actors["telcos"]
    pos_per_telco = circus.actors["pos"].size / telcos.size

    for product, description in params["products"].items():

        action_name = "telcos_{}_bulk_purchase".format(product)
        logging.info("creating {} action".format(action_name))

        bulk_purchases = circus.create_story(
            name=action_name,
            initiating_actor=telcos,
            actorid_field="TELCO",

            # no timer or activity: this is triggered by the bulk-purchases
            # of the level 1 dealers
        )

        # telcos do not "buys" product, they just create them
        product_id_gen = circus.generators["{}_id_gen".format(product)]
        bulk_gen = DependentBulkGenerator(product_id_gen)

        # bulk size distribution is a scaled version of POS bulk size distribution
        bulk_size_gen = patterns.scale_quantity_gen(
            stock_size_gen=circus.generators["pos_{}_bulk_size_gen".format(product)],
            scale_factor=pos_per_telco)

        bulk_purchases.set_operations(
            circus.clock.ops.timestamp(named_as="TIME"),

            bulk_size_gen.ops.generate(named_as="BULK_SIZE"),

            telcos.get_relationship(product).ops
                  .get_neighbourhood_size(from_field="TELCO",
                                          named_as="OLD_STOCK"),

            bulk_gen.ops.generate(named_as="ITEMS_BULK",
                                  observed_field="BULK_SIZE"),

            # and adding them to the buyer
            telcos.get_relationship(product).ops.add_grouped(
                from_field="TELCO",
                grouped_items_field="ITEMS_BULK"),

            telcos.get_relationship(product).ops \
                  .get_neighbourhood_size(
                        from_field="TELCO",
                        named_as="NEW_STOCK"),

            FieldLogger(log_id=action_name,
                        cols=["TIME", "TELCO", "OLD_STOCK",
                              "NEW_STOCK", "BULK_SIZE"]))


def prepare_dealers(circus, params):
    """
    updates the dist_l1 and dist_l2 populations with product stock
    and link from dist_l1 to telcos
    """

    for level in ["l1", "l2"]:

        actor_name = "dist_{}".format(level)

        logging.info("prepare {} actor".format(actor_name))
        dealers = circus.actors[actor_name]

        pos_per_dealer = circus.actors["pos"].size / dealers.size

        dealers.create_attribute(
            "DISTRIBUTOR_SALES_REP_NAME",
            init_gen=snd_constants.gen("CONTACT_NAMES", next(circus.seeder)))

        dealers.create_attribute(
            "DISTRIBUTOR_SALES_REP_PHONE",
            init_gen=FakerGenerator(method="phone_number",
                                    seed=next(circus.seeder)))

        for product, description in params["products"].items():

            logging.info("generating {} initial {} stock".format(actor_name, product))
            init_stock_size_gen = patterns.scale_quantity_gen(
                    stock_size_gen=circus.generators["pos_{}_init_stock_size_gen".format(product)],
                    scale_factor=pos_per_dealer)
            product_id_gen = circus.generators["{}_id_gen".format(product)]
            stock_gen = init_stock_size_gen.flatmap(
                DependentBulkGenerator(element_generator=product_id_gen))

            dealers.create_stock_relationship_grp(name=product,
                                                  stock_bulk_gen=stock_gen)

    # no need to connect dist l2 to l1: that comes from the belgium component
    logging.info("connecting all dist_l1 to telco, for each product")

    telcos = circus.actors["telcos"]
    dist_l1 = circus.actors["dist_l1"]

    for product in params["products"].keys():
        rel = dist_l1.create_relationship(name="{}__provider".format(product))

        # TODO: this assumes we have only one telco (I guess it's ok...)
        rel.add_relations(
            from_ids=dist_l1.ids,
            to_ids=telcos.ids[0])


def save_providers_csv(circus, params):

    target_file = os.path.join(db.namespace_folder(circus.name),
                               "distributor_agent_product.csv")

    pos_df = circus.actors["pos"].to_dataframe().reset_index()

    providers_df = pd.DataFrame(
        columns=["distributor_id", "agent_id", "product_type_id"])

    for product in params["products"].keys():

        """Add LINK between POS and DIST_L1"""

        pos_dist_l2 = circus.actors["pos"] \
            .relationships["{}__provider".format(product)].get_relations()

        dist_l2_dist_l1 = circus.actors["dist_l2"] \
            .relationships["{}__provider".format(product)].get_relations()

        # join dist_l1 responsible for pos
        pos_dist_l1 = pd.merge(
            left=pos_dist_l2, right=dist_l2_dist_l1[["from", "to"]],
            left_on="to", right_on="from", suffixes=('_pos', '_dist_l1')
        )

        # join pos mongo id
        pos_dist_l1 = pd.merge(
            left=pos_dist_l1, right=pos_df[["index", "MONGO_ID"]],
            left_on="from_pos", right_on="index"
        )

        pos_dist_l1 = pos_dist_l1.rename(columns={
            "MONGO_ID": "agent_id",
            "to_dist_l1": "distributor_id"
        })

        pos_dist_l1["product_type_id"] = product

        pos_dist_l1 = pos_dist_l1.reindex_axis(
            ["distributor_id", "agent_id", "product_type_id"], axis=1)

        providers_df = providers_df.append(pos_dist_l1)

        """Add LINK between DIST_L2 and DIST_L1"""

        dist_l2_dist_l1 = dist_l2_dist_l1.rename(columns={
            "from": "agent_id",
            "to": "distributor_id"
        })

        dist_l2_dist_l1["product_type_id"] = product

        dist_l2_dist_l1 = dist_l2_dist_l1.reindex_axis(
            ["distributor_id", "agent_id", "product_type_id"], axis=1)

        providers_df = providers_df.append(dist_l2_dist_l1)

    providers_df.to_csv(target_file, index=False)
