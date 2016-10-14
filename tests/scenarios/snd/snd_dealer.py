from __future__ import division

from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
import snd_pos
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
                                             stock_bulk_gen=int_stock_gen,
                                             seed=circus.seeder.next())


def add_telco_restock_actions(circus):
    """
    Add actions to "restock" the ERS and SIM products, i.e. create new items
    when needed (Telco do not buy products from anyone in this model,
    they just create them).

    :param circus:
    :return:
    """

    n_customers = circus.actors["customers"].size

    _add_bulk_restock_action(
        circus,
        action_name="telco_bulk_restocks_sims",
        telco_actor=circus.actors["telcos"],
        bulk_restock_size=n_customers / 10,
        stock_relationship="SIM",
        stock_id_generator=circus.generators["SIM_id_gen"])

    _add_bulk_restock_action(
        circus,
        action_name="telco_bulk_restocks_ers",
        telco_actor=circus.actors["telcos"],
        bulk_restock_size=n_customers,
        stock_relationship="ER",
        stock_id_generator=circus.generators["ER_id_gen"])


def _add_bulk_restock_action(circus,
                             action_name,
                             telco_actor,
                             bulk_restock_size,
                             stock_relationship,
                             stock_id_generator):
    """
    Generic utility method to create a bulk restock action for a telco
    """

    logging.info("creating {} bulk restock action".format(action_name))
    build_purchases = circus.create_action(
        name=action_name,
        initiating_actor=telco_actor,
        actorid_field="TELCO",

        # no timer or activity
    )

    bulk_gen = DependentBulkGenerator(stock_id_generator)

    build_purchases.set_operations(
        circus.clock.ops.timestamp(named_as="TIME"),

        ConstantGenerator(value=bulk_restock_size).ops.generate(
            named_as="BULK_SIZE"),

        telco_actor.get_relationship(stock_relationship).ops\
            .get_neighbourhood_size(
                from_field="TELCO",
                named_as="OLD_STOCK"),

        # Telcos are "source" actor injecting items in the simulation
        bulk_gen.ops.generate(named_as="ITEMS_BULK",
                              observed_field="BULK_SIZE"),

        # and adding them to the buyer
        telco_actor.get_relationship(stock_relationship).ops.add_grouped(
            from_field="TELCO",
            grouped_items_field="ITEMS_BULK"),

        telco_actor.get_relationship(stock_relationship).ops\
            .get_neighbourhood_size(
                from_field="TELCO",
                named_as="NEW_STOCK"),

        operations.FieldLogger(log_id=action_name,
                               cols=["TIME", "TELCO", "OLD_STOCK",
                                     "NEW_STOCK", "BULK_SIZE"]))


def create_dealers(circus, actor_name, actor_size, params, actor_id_gen):
    """
    Adds the dealers level 1, i.e between the telcos and dealers level 2
    """

    logging.info("creating {} actor".format(actor_name))
    dealers = circus.create_actor(name=actor_name,
                                  size=actor_size,
                                  ids_gen=actor_id_gen)
    pos_per_dealer = circus.actors["pos"].size / dealers.size

    for product, description in params["products"].items():

        logging.info("generating {} initial {} stock".format(actor_name, product))
        init_stock_size_gen = patterns.scale_quantity_gen(
                stock_size_gen=circus.generators["pos_{}_init_stock_size_gen".format(product)],
                scale_factor=pos_per_dealer)
        product_id_gen = circus.generators["{}_id_gen".format(product)]
        stock_gen = init_stock_size_gen.flatmap(
            DependentBulkGenerator(element_generator=product_id_gen))

        dealers.create_stock_relationship_grp(
            name=product, stock_bulk_gen=stock_gen,seed=circus.seeder.next())


def add_dealers_l1_bulk_purchase_actions(circus, params):
    """
    Adds actions to this circus to trigger bulk purchases by the level 1
    dealers when their stock gets low.
    """

    dealers_l1 = circus.actors["dealers_l1"]
    telcos = circus.actors["telcos"]
    pos_per_dealer_l1 = circus.actors["pos"].size / dealers_l1.size
    pos_per_telco = circus.actors["pos"].size / telcos.size

    # trigger on the telco stock that should trigger a re-stock
    low_ers_stock_telco_bulk_purchase_trigger = DependentTriggerGenerator(
        value_to_proba_mapper=operations.bounded_sigmoid(
            x_min=pos_per_telco,
            x_max=params["max_pos_er_stock_triggering_restock"] * pos_per_telco,
            shape=params["pos_er_restock_shape"],
            incrementing=False))

    ers_bulk_size_gen = patterns.scale_quantity_gen(
        stock_size_gen=circus.generators["pos_ER_bulk_size_gen"],
        scale_factor=pos_per_dealer_l1)

    _add_bulk_purchase_action(
        circus,
        action_name="dealer_l1_buys_ers_from_telco",
        buyer_actor=dealers_l1,
        seller_actor=telcos,
        bulk_size_gen=ers_bulk_size_gen,
        link_relationship="telcos",
        buyer_stock_relationship="ER",
        seller_stock_relationship="ER",
        upperlevel_bulk_purchase_trigger=low_ers_stock_telco_bulk_purchase_trigger,
        upperlevel_bulk_purchase_action_name="telco_bulk_restocks_ers")

    # trigger on the telco stock that should trigger a re-stock
    low_sims_stock_dealer_bulk_purchase_trigger = DependentTriggerGenerator(
        value_to_proba_mapper=operations.bounded_sigmoid(
            x_min=pos_per_telco,
            x_max=params["max_pos_sim_stock_triggering_restock"] * pos_per_telco,
            shape=params["pos_sim_restock_shape"],
            incrementing=False))

    sims_bulk_size_gen = patterns.scale_quantity_gen(
        stock_size_gen=circus.generators["pos_SIM_bulk_size_gen"],
        scale_factor=pos_per_dealer_l1)

    _add_bulk_purchase_action(
        circus,
        action_name="dealer_l1_buys_sims_from_telco",
        buyer_actor=dealers_l1,
        seller_actor=telcos,
        bulk_size_gen=sims_bulk_size_gen,
        link_relationship="telcos",
        buyer_stock_relationship="SIM",
        seller_stock_relationship="SIM",
        upperlevel_bulk_purchase_trigger=low_sims_stock_dealer_bulk_purchase_trigger,
        upperlevel_bulk_purchase_action_name="telco_bulk_restocks_ers")

    return dealers_l1


def add_dealers_l2_bulk_purchase_actions(circus, params):
    """
    Adds actions to this circus to trigger bulk purchases by the level 2
    dealers when their stock gets low.

    # TODO: this is basically a copy-paste of the corresponging action for level 1
    """

    dealers_l2 = circus.actors["dealers_l2"]
    dealers_l1 = circus.actors["dealers_l1"]
    pos_per_dealer_l1 = circus.actors["pos"].size / dealers_l1.size
    pos_per_dealer_l2 = circus.actors["pos"].size / dealers_l2.size

    # trigger on the dealer level 1 stock that should trigger a re-stock
    low_ers_stock_dealer_l1_bulk_purchase_trigger = DependentTriggerGenerator(
        value_to_proba_mapper=operations.bounded_sigmoid(
            x_min=pos_per_dealer_l1,
            x_max=params["max_pos_er_stock_triggering_restock"] * pos_per_dealer_l1,
            shape=params["pos_er_restock_shape"],
            incrementing=False))

    ers_bulk_size_gen = patterns.scale_quantity_gen(
        stock_size_gen=circus.generators["pos_ER_bulk_size_gen"],
        scale_factor=pos_per_dealer_l2)

    _add_bulk_purchase_action(
        circus,
        action_name="dealer_l2_buys_ers_from_dealer_l1",
        buyer_actor=dealers_l2,
        seller_actor=dealers_l1,
        bulk_size_gen=ers_bulk_size_gen,
        link_relationship="dealers_l1",
        buyer_stock_relationship="ER",
        seller_stock_relationship="ER",
        upperlevel_bulk_purchase_trigger=low_ers_stock_dealer_l1_bulk_purchase_trigger,
        upperlevel_bulk_purchase_action_name="dealer_l1_buys_ers_from_telco")

    # trigger on the dealer l1 stock that should trigger a re-stock
    low_sims_stock_dealer_bulk_purchase_trigger = DependentTriggerGenerator(
        value_to_proba_mapper=operations.bounded_sigmoid(
            x_min=pos_per_dealer_l1,
            x_max=params["max_pos_sim_stock_triggering_restock"] * pos_per_dealer_l1,
            shape=params["pos_sim_restock_shape"],
            incrementing=False))

    sims_bulk_size_gen = patterns.scale_quantity_gen(
        stock_size_gen=circus.generators["pos_SIM_bulk_size_gen"],
        scale_factor=pos_per_dealer_l2)

    _add_bulk_purchase_action(
        circus,
        action_name="dealer_l2_buys_sims_from_dealer_l1",
        buyer_actor=dealers_l2,
        seller_actor=dealers_l1,
        bulk_size_gen=sims_bulk_size_gen,
        link_relationship="dealers_l1",
        buyer_stock_relationship="SIM",
        seller_stock_relationship="SIM",
        upperlevel_bulk_purchase_trigger=low_sims_stock_dealer_bulk_purchase_trigger,
        upperlevel_bulk_purchase_action_name="dealer_l1_buys_sims_from_telco")


def _add_bulk_purchase_action(circus,
                              action_name,
                              buyer_actor,
                              seller_actor,
                              bulk_size_gen,
                              link_relationship,
                              buyer_stock_relationship,
                              seller_stock_relationship,
                              upperlevel_bulk_purchase_trigger,
                              upperlevel_bulk_purchase_action_name):
    """
    Generic utility method to create a bulk purchase action between distributors
    """

    logging.info("creating {} bulk purchase action".format(action_name))
    build_purchases = circus.create_action(
        name=action_name,
        initiating_actor=buyer_actor,
        actorid_field="BUYER_ID",

        # no timer or activity: dealers bulk purchases are triggered externally
    )

    build_purchases.set_operations(
        circus.clock.ops.timestamp(named_as="TIME"),

        buyer_actor.get_relationship(link_relationship).ops.select_one(
            from_field="BUYER_ID",
            named_as="SELLER_ID"),

        bulk_size_gen.ops.generate(named_as="REQUESTED_BULK_SIZE"),

        buyer_actor.get_relationship(buyer_stock_relationship).ops\
            .get_neighbourhood_size(
                from_field="BUYER_ID",
                named_as="OLD_BUYER_STOCK"),

        # selecting and removing Sims from dealers
        seller_actor.get_relationship(seller_stock_relationship).ops \
            .select_many(
                from_field="SELLER_ID",
                named_as="ITEMS_BULK",
                quantity_field="REQUESTED_BULK_SIZE",

                # if an item is selected, it is removed from the dealer's stock
                pop=True,

                # TODO: put this back to False and log the failed purchases
                discard_missing=True),

        # and adding them to the buyer
        buyer_actor.get_relationship(buyer_stock_relationship).ops.add_grouped(
            from_field="BUYER_ID",
            grouped_items_field="ITEMS_BULK"),

        # We do not track the old and new stock of the dealer since the result
        # is misleading: since all purchases are performed in parallel,
        # if a dealer is selected several times, its stock level after the
        # select_many() is the level _after_ all purchases are done, which is
        # typically not what we want to include in the log.
        buyer_actor.get_relationship(buyer_stock_relationship).ops\
            .get_neighbourhood_size(
                from_field="BUYER_ID",
                named_as="NEW_BUYER_STOCK"),

        # actual number of bought items might be different due to out of stock
        operations.Apply(source_fields="ITEMS_BULK",
                         named_as="BULK_SIZE",
                         f=lambda s: s.map(len), f_args="series"),

        operations.FieldLogger(log_id=action_name,
                               cols=["TIME",  "BUYER_ID", "SELLER_ID",
                                     "OLD_BUYER_STOCK", "NEW_BUYER_STOCK",
                                     "BULK_SIZE"]),

        patterns.trigger_action_if_low_stock(
            circus,
            stock_relationship=seller_actor.get_relationship(seller_stock_relationship),
            actor_id_field="SELLER_ID",
            restock_trigger=upperlevel_bulk_purchase_trigger,
            triggered_action_name=upperlevel_bulk_purchase_action_name
        )
    )
