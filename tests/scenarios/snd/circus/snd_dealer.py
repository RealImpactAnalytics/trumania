from __future__ import division

from datagenerator.core.circus import *
from datagenerator.core.util_functions import *
import snd_constants
import patterns


def add_telcos(circus, params, distributor_id_gen):

    logging.info("creating telcos")
    telcos = circus.create_actor(name="telcos", size=params["n_telcos"],
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
    :return:
    """

    telcos = circus.actors["telcos"]
    pos_per_telco = circus.actors["pos"].size / telcos.size

    for product, description in params["products"].items():

        action_name = "telcos_{}_bulk_purchase".format(product)
        logging.info("creating {} action".format(action_name))

        bulk_purchases = circus.create_action(
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

            telcos.get_relationship(product).ops\
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

            operations.FieldLogger(log_id=action_name,
                                   cols=["TIME", "TELCO", "OLD_STOCK",
                                         "NEW_STOCK", "BULK_SIZE"]))


def prepare_dealers(circus, params):
    """
    updates the dist_l1 and dist_l2 actors with product stock
    and link from dist_l1 to telcos
    """

    for level in ["l1", "l2"]:

        actor_name = "dist_{}".format(level)

        logging.info("prepare {} actor".format(actor_name))
        dealers = circus.actors[actor_name]

        pos_per_dealer = circus.actors["pos"].size / dealers.size

        dealers.create_attribute(
            "DISTRIBUTOR_SALES_REP_NAME",
            init_gen=snd_constants.gen("CONTACT_NAMES", circus.seeder.next()))

        dealers.create_attribute(
            "DISTRIBUTOR_SALES_REP_PHONE",
            init_gen=FakerGenerator(method="phone_number",
                                    seed=circus.seeder.next()))

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
