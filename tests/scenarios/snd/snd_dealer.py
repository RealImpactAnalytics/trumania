from __future__ import division

from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
from numpy.random import *


def create_telcos(circus, params, distributor_id_gen,
                  sim_id_gen, recharge_id_gen):

    logging.info("creating telcos")
    telcos = Actor(size=params["n_telcos"], ids_gen=distributor_id_gen)

    logging.info(" generating telcos initial SIM stock")

    pos_per_telco = params["n_pos"] / params["n_telcos"]
    sims_per_telco = params["n_init_sim_per_pos"] * pos_per_telco
    ers_per_telco = params["pos_ers_max_stock"] * pos_per_telco / 2

    telcos.create_stock_relationship(
        name="SIMS", item_id_gen=sim_id_gen,
        n_items_per_actor=sims_per_telco, seeder=circus.seeder)

    telcos.create_stock_relationship(
        name="ERS", item_id_gen=recharge_id_gen,
        n_items_per_actor=ers_per_telco, seeder=circus.seeder)

    sim_bulk_restock_size = \
        params["mean_pos_sim_bulk_purchase_size"] * pos_per_telco

    _add_bulk_restock_action(circus,
                             action_name="telco_bulk_restocks_sims",
                             actor=telcos,
                             bulk_restock_attribute="SIM_BULK_RESTOCK_SIZE",
                             bulk_restock_size=sim_bulk_restock_size,
                             stock_relationship="SIMS",
                             stock_id_generator=sim_id_gen)

    ers_bulk_restock_size = \
        params["mean_pos_ers_bulk_purchase_size"] * pos_per_telco

    _add_bulk_restock_action(circus,
                             action_name="telco_bulk_restocks_ers",
                             actor=telcos,
                             bulk_restock_attribute="ERS_BULK_RESTOCK_SIZE",
                             bulk_restock_size=ers_bulk_restock_size,
                             stock_relationship="ERS",
                             stock_id_generator=recharge_id_gen)

    return telcos


def _add_bulk_restock_action(circus,
                             action_name,
                             actor,
                             bulk_restock_attribute,
                             bulk_restock_size,
                             stock_relationship,
                             stock_id_generator):
    """
    Generic utility method to create a bulk restock action for a telco
    """

    logging.info("generating size of bulk restock for {}".format(action_name))
    actor.create_attribute(
        name=bulk_restock_attribute,
        init_gen=ConstantGenerator(value=bulk_restock_size)
    )

    logging.info("creating {} bulk restock action".format(action_name))
    build_purchases = circus.create_action(
        name=action_name,
        initiating_actor=actor,
        actorid_field="DISTRIBUTOR",

        # no timer or activity
    )

    bulk_gen = DependentBulkGenerator(stock_id_generator)

    build_purchases.set_operations(
        circus.clock.ops.timestamp(named_as="TIME"),

        actor.ops.lookup(
            actor_id_field="DISTRIBUTOR",
            select={bulk_restock_attribute: "REQUESTED_BULK_SIZE"}),

        actor.get_relationship(stock_relationship).ops \
            .get_neighbourhood_size(
                from_field="DISTRIBUTOR",
                named_as="OLD_STOCK"),

        bulk_gen.ops.generate(named_as="ITEMS_BULK",
                              observed_field="REQUESTED_BULK_SIZE"),

        # and adding them to the buyer
        actor.get_relationship(stock_relationship).ops.add_grouped(
            from_field="DISTRIBUTOR",
            grouped_items_field="ITEMS_BULK"),

        actor.get_relationship(stock_relationship).ops\
            .get_neighbourhood_size(
                from_field="DISTRIBUTOR",
                named_as="NEW_STOCK"),

        # actual number of bought items might be different due to out of stock
        operations.Apply(source_fields="ITEMS_BULK",
                         named_as="BULK_SIZE",
                         f=lambda s: s.map(len), f_args="series"),

        operations.FieldLogger(log_id=action_name,
                               cols=["TIME",  "DISTRIBUTOR", "OLD_STOCK",
                                     "NEW_STOCK"]))


def create_dealers_l1(circus, params, distributor_id_gen,
                      sim_id_gen, recharge_id_gen):

    logging.info("creating l1 dealers")
    dealers_l1 = Actor(size=params["n_dealers_l1"], ids_gen=distributor_id_gen)

    logging.info(" generating l1 dealer initial SIM stock")

    pos_per_dealer_l1 = params["n_pos"] / params["n_dealers_l1"]
    sims_per_dealer_l1 = params["n_init_sim_per_pos"] * pos_per_dealer_l1
    ers_per_dealer_l1 = params["pos_ers_max_stock"] * pos_per_dealer_l1 / 2

    dealers_l1.create_stock_relationship(
        name="SIMS", item_id_gen=sim_id_gen,
        n_items_per_actor=sims_per_dealer_l1, seeder=circus.seeder)

    dealers_l1.create_stock_relationship(
        name="ERS", item_id_gen=recharge_id_gen,
        n_items_per_actor=ers_per_dealer_l1, seeder=circus.seeder)

    logging.info(" linking l1 dealers to telcos")
    _create_distribution_link(circus, circus.telcos, dealers_l1, "TELCOS")

    ers_mean_bulk_size = \
        params["mean_pos_ers_bulk_purchase_size"] * pos_per_dealer_l1
    ers_std_bulk_size = \
        params["std_pos_ers_bulk_purchase_size"] * pos_per_dealer_l1

    _add_bulk_purchase_action(circus,
                              action_name="dealer_l1_buys_ers_from_telco",
                              buyer_actor=dealers_l1,
                              seller_actor=circus.telcos,
                              bulk_size_attribute="ER_BULK_BUY_SIZE",
                              mean_bulk_purchase_size=ers_mean_bulk_size,
                              std_bulk_purchase_size=ers_std_bulk_size,
                              link_relationship="TELCOS",
                              buyer_stock_relationship="ERS",
                              seller_stock_relationship="ERS")

    sim_mean_bulk_size = \
        params["mean_pos_sim_bulk_purchase_size"] * pos_per_dealer_l1
    sim_std_bulk_size = \
        params["std_pos_sim_bulk_purchase_size"] * pos_per_dealer_l1

    _add_bulk_purchase_action(circus,
                              action_name="dealer_l2_buys_sims_from_telco",
                              buyer_actor=dealers_l1,
                              seller_actor=circus.telcos,
                              bulk_size_attribute="SIM_BULK_BUY_SIZE",
                              mean_bulk_purchase_size=sim_mean_bulk_size,
                              std_bulk_purchase_size=sim_std_bulk_size,
                              link_relationship="TELCOS",
                              buyer_stock_relationship="SIMS",
                              seller_stock_relationship="SIMS")

    return dealers_l1


def create_dealers_l2(circus, params, distributor_id_gen,
                      sim_id_gen, recharge_id_gen):

    logging.info("creating l2 dealers")
    dealers_l2 = Actor(size=params["n_dealers_l2"], ids_gen=distributor_id_gen)

    logging.info(" generating l2 dealer initial SIM stock")

    pos_per_dealer_l2 = params["n_pos"] / params["n_dealers_l2"]

    # bugfix: we need enough ERS to feed the POS. 250k should be enough though..
    sims_per_dealer_l2 = 10000000
    ers_per_dealer_l2 = 10000000

    dealers_l2.create_stock_relationship(
        name="SIMS", item_id_gen=sim_id_gen,
        n_items_per_actor=sims_per_dealer_l2, seeder=circus.seeder)

    dealers_l2.create_stock_relationship(
        name="ERS", item_id_gen=recharge_id_gen,
        n_items_per_actor=ers_per_dealer_l2, seeder=circus.seeder)

    # logging.info(" linking l2 dealers to l1 dealers")
    # _create_distribution_link(circus,
    #                           circus.dealers_l1, dealers_l2, "DEALERS_L1")
    #
    # ers_mean_bulk_size = \
    #     params["mean_pos_ers_bulk_purchase_size"] * pos_per_dealer_l2
    # ers_std_bulk_size = \
    #     params["std_pos_ers_bulk_purchase_size"] * pos_per_dealer_l2
    #
    # _add_bulk_purchase_action(circus,
    #                           action_name="dealer_l2_buys_ers_from_dealer_l1",
    #                           buyer_actor=dealers_l2,
    #                           seller_actor=circus.dealers_l1,
    #                           bulk_size_attribute="ER_BULK_BUY_SIZE",
    #                           mean_bulk_purchase_size=ers_mean_bulk_size,
    #                           std_bulk_purchase_size=ers_std_bulk_size,
    #                           link_relationship="DEALERS_L1",
    #                           buyer_stock_relationship="ERS",
    #                           seller_stock_relationship="ERS")
    #
    # sim_mean_bulk_size = \
    #     params["mean_pos_sim_bulk_purchase_size"] * pos_per_dealer_l2
    # sim_std_bulk_size = \
    #     params["std_pos_sim_bulk_purchase_size"] * pos_per_dealer_l2
    #
    # _add_bulk_purchase_action(circus,
    #                           action_name="dealer_l2_buys_sims_from_dealer_l1",
    #                           buyer_actor=dealers_l2,
    #                           seller_actor=circus.dealers_l1,
    #                           bulk_size_attribute="SIM_BULK_BUY_SIZE",
    #                           mean_bulk_purchase_size=sim_mean_bulk_size,
    #                           std_bulk_purchase_size=sim_std_bulk_size,
    #                           link_relationship="DEALERS_L1",
    #                           buyer_stock_relationship="SIMS",
    #                           seller_stock_relationship="SIMS")

    return dealers_l2


def _create_distribution_link(circus,
                              upper_lever, current_level,
                              relationship_name):

    chose_one_upper = make_random_assign(
        set1=current_level.ids,
        set2=upper_lever.ids,
        seed=circus.seeder.next())

    rel = current_level.create_relationship(relationship_name,
                                            seed=circus.seeder.next())
    rel.add_relations(
        from_ids=chose_one_upper["set1"],
        to_ids=chose_one_upper["chosen_from_set2"])


def _add_bulk_purchase_action(circus,
                              action_name,
                              buyer_actor,
                              seller_actor,
                              bulk_size_attribute,
                              mean_bulk_purchase_size,
                              std_bulk_purchase_size,
                              link_relationship,
                              buyer_stock_relationship,
                              seller_stock_relationship):
    """
    Generic utility method to create a bulk purchase action between distributors
    """

    logging.info("generating size of bulk purchase for {}".format(action_name))
    buyer_actor.create_attribute(
        name=bulk_size_attribute,
        init_gen=BoundedGenerator(
            upstream_gen=NumpyRandomGenerator(
                method="normal",
                loc=mean_bulk_purchase_size,
                scale=std_bulk_purchase_size,
                seed=circus.seeder.next()
            ), lb=0
        ))

    logging.info("creating {} bulk purchase action".format(action_name))
    build_purchases = circus.create_action(
        name=action_name,
        initiating_actor=buyer_actor,
        actorid_field="BUYER_ID",

        # no timer or activity
    )

    build_purchases.set_operations(
        circus.clock.ops.timestamp(named_as="TIME"),

        buyer_actor.get_relationship(link_relationship).ops.select_one(
            from_field="BUYER_ID",
            named_as="SELLER_ID"),

        buyer_actor.ops.lookup(
            actor_id_field="BUYER_ID",
            select={bulk_size_attribute: "REQUESTED_BULK_SIZE"}),

        buyer_actor.get_relationship(buyer_stock_relationship).ops \
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
                                     "BULK_SIZE"]))
