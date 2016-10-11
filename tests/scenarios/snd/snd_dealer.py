from __future__ import division

from datagenerator.core.circus import *
from datagenerator.core.actor import *
from datagenerator.core.util_functions import *
import snd_pos
import patterns


def create_telcos(circus, params, distributor_id_gen,
                  sim_id_gen, recharge_id_gen):

    logging.info("creating telcos")
    telcos = Actor(size=params["n_telcos"], ids_gen=distributor_id_gen)

    logging.info("generating telco initial ERS stock")
    ers_stock_gen = ConstantGenerator(value=params["n_customers"])\
        .flatmap(DependentBulkGenerator(element_generator=recharge_id_gen))

    telcos.create_stock_relationship_grp(name="ERS",
                                         stock_bulk_gen=ers_stock_gen,
                                         seed=circus.seeder.next())

    logging.info("generating telco initial SIMS stock")
    sims_stock_gen = ConstantGenerator(value=params["n_customers"] / 10)\
        .flatmap(DependentBulkGenerator(element_generator=sim_id_gen))

    telcos.create_stock_relationship_grp(name="SIMS",
                                         stock_bulk_gen=sims_stock_gen,
                                         seed=circus.seeder.next())

    _add_bulk_restock_action(circus,
                             action_name="telco_bulk_restocks_sims",
                             telco_actor=telcos,
                             bulk_restock_size=params["n_customers"] / 10,
                             stock_relationship="SIMS",
                             stock_id_generator=sim_id_gen)

    _add_bulk_restock_action(circus,
                             action_name="telco_bulk_restocks_ers",
                             telco_actor=telcos,
                             bulk_restock_size=params["n_customers"],
                             stock_relationship="ERS",
                             stock_id_generator=recharge_id_gen)

    return telcos


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
                               cols=["TIME",  "TELCO", "OLD_STOCK",
                                     "NEW_STOCK", "BULK_SIZE"]))


def create_dealers_l1(circus, params, distributor_id_gen,
                      sim_id_gen, recharge_id_gen):

    logging.info("creating L1 dealers")
    dealers_l1 = Actor(size=params["n_dealers_l1"], ids_gen=distributor_id_gen)
    pos_per_dealer_l1 = circus.pos.size / dealers_l1.size

    logging.info("generating dealers L1 initial ERS stock")
    ers_stock_gen = snd_pos.ers_stock_size_gen(
            params, seed=circus.seeder.next(), scale_factor=pos_per_dealer_l1)\
        .flatmap(DependentBulkGenerator(element_generator=recharge_id_gen))

    dealers_l1.create_stock_relationship_grp(name="ERS",
                                             stock_bulk_gen=ers_stock_gen,
                                             seed=circus.seeder.next())

    logging.info("generating dealers L1 initial SIMS stock")
    sims_stock_gen = snd_pos.sim_stock_size_gen(
            params, seed=circus.seeder.next(), scale_factor=pos_per_dealer_l1) \
        .flatmap(DependentBulkGenerator(element_generator=sim_id_gen))

    dealers_l1.create_stock_relationship_grp(name="SIMS",
                                             stock_bulk_gen=sims_stock_gen,
                                             seed=circus.seeder.next())

    logging.info(" linking L1 dealers to telcos")
    _create_distribution_link(circus, circus.telcos, dealers_l1, "TELCOS")

    low_ers_stock_dealer_bulk_purchase_trigger = DependentTriggerGenerator(
        value_to_proba_mapper=operations.bounded_sigmoid(
            x_min=pos_per_dealer_l1,
            x_max=params["max_pos_er_stock_triggering_restock"] * pos_per_dealer_l1,
            shape=params["pos_er_restock_shape"],
            incrementing=False))

    ers_bulk_size_gen = snd_pos.ers_bulk_size_gen(
        params, seed=circus.seeder.next(), scale_factor=pos_per_dealer_l1)

    _add_bulk_purchase_action(
        circus,
        action_name="dealer_l1_buys_ers_from_telco",
        buyer_actor=dealers_l1,
        seller_actor=circus.telcos,
        bulk_size_gen=ers_bulk_size_gen,
        link_relationship="TELCOS",
        buyer_stock_relationship="ERS",
        seller_stock_relationship="ERS",
        upperlevel_bulk_purchase_trigger=low_ers_stock_dealer_bulk_purchase_trigger,
        upperlevel_bulk_purchase_action_name="telco_bulk_restocks_ers")

    low_sims_stock_dealer_bulk_purchase_trigger = DependentTriggerGenerator(
        value_to_proba_mapper=operations.bounded_sigmoid(
            x_min=pos_per_dealer_l1,
            x_max=params["max_pos_sim_stock_triggering_restock"] * pos_per_dealer_l1,
            shape=params["pos_sim_restock_shape"],
            incrementing=False))

    sims_bulk_size_gen = snd_pos.sims_bulk_size_gen(
        params, seed=circus.seeder.next(), scale_factor=pos_per_dealer_l1)

    _add_bulk_purchase_action(
        circus,
        action_name="dealer_l1_buys_sims_from_telco",
        buyer_actor=dealers_l1,
        seller_actor=circus.telcos,
        bulk_size_gen=sims_bulk_size_gen,
        link_relationship="TELCOS",
        buyer_stock_relationship="SIMS",
        seller_stock_relationship="SIMS",
        upperlevel_bulk_purchase_trigger=low_sims_stock_dealer_bulk_purchase_trigger,
        upperlevel_bulk_purchase_action_name="telco_bulk_restocks_ers")

    return dealers_l1


def create_dealers_l2(circus, params, distributor_id_gen,
                      sim_id_gen, recharge_id_gen):

    logging.info("creating L2 dealers")
    dealers_l2 = Actor(size=params["n_dealers_l2"], ids_gen=distributor_id_gen)
    pos_per_dealer_l2 = circus.pos.size / dealers_l2.size

    logging.info("generating dealers L2 initial ERS stock")
    ers_stock_gen = snd_pos.ers_stock_size_gen(
            params, seed=circus.seeder.next(), scale_factor=pos_per_dealer_l2)\
        .flatmap(DependentBulkGenerator(element_generator=recharge_id_gen))

    dealers_l2.create_stock_relationship_grp(name="ERS",
                                             stock_bulk_gen=ers_stock_gen,
                                             seed=circus.seeder.next())

    logging.info("generating dealers L2 initial SIMS stock")
    sims_stock_gen = snd_pos.sim_stock_size_gen(
            params, seed=circus.seeder.next(), scale_factor=pos_per_dealer_l2) \
        .flatmap(DependentBulkGenerator(element_generator=sim_id_gen))

    dealers_l2.create_stock_relationship_grp(name="SIMS",
                                             stock_bulk_gen=sims_stock_gen,
                                             seed=circus.seeder.next())

    logging.info(" linking L2 dealers to L1 dealers")
    _create_distribution_link(circus,
                              circus.dealers_l1, dealers_l2, "DEALERS_L1")

    ers_bulk_size_gen = snd_pos.ers_bulk_size_gen(
        params, seed=circus.seeder.next(), scale_factor=pos_per_dealer_l2)

    low_ers_stock_dealer_bulk_purchase_trigger = DependentTriggerGenerator(
        value_to_proba_mapper=operations.bounded_sigmoid(
            x_min=pos_per_dealer_l2,
            x_max=params["max_pos_er_stock_triggering_restock"] * pos_per_dealer_l2,
            shape=params["pos_er_restock_shape"],
            incrementing=False))

    _add_bulk_purchase_action(
        circus,
        action_name="dealer_l2_buys_ers_from_dealer_l1",
        buyer_actor=dealers_l2,
        seller_actor=circus.dealers_l1,
        bulk_size_gen=ers_bulk_size_gen,
        link_relationship="DEALERS_L1",
        buyer_stock_relationship="ERS",
        seller_stock_relationship="ERS",
        upperlevel_bulk_purchase_trigger=low_ers_stock_dealer_bulk_purchase_trigger,
        upperlevel_bulk_purchase_action_name="dealer_l1_buys_ers_from_telco"
    )

    sims_bulk_size_gen = snd_pos.sims_bulk_size_gen(
        params, seed=circus.seeder.next(), scale_factor=pos_per_dealer_l2)

    low_sim_stock_dealer_bulk_purchase_trigger = DependentTriggerGenerator(
        value_to_proba_mapper=operations.bounded_sigmoid(
            x_min=pos_per_dealer_l2,
            x_max=params["max_pos_sim_stock_triggering_restock"] * pos_per_dealer_l2,
            shape=params["pos_er_restock_shape"],
            incrementing=False))

    _add_bulk_purchase_action(
        circus,
        action_name="dealer_l2_buys_sims_from_dealer_l1",
        buyer_actor=dealers_l2,
        seller_actor=circus.dealers_l1,
        bulk_size_gen=sims_bulk_size_gen,
        link_relationship="DEALERS_L1",
        buyer_stock_relationship="SIMS",
        seller_stock_relationship="SIMS",
        upperlevel_bulk_purchase_trigger=low_sim_stock_dealer_bulk_purchase_trigger,
        upperlevel_bulk_purchase_action_name="dealer_l1_buys_sims_from_telco")

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
