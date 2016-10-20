"""
Re-usable operation chains throughout the SND scenario
"""
import datagenerator.core.operations as operations
import datagenerator.core.random_generators as random_generators
import datagenerator.core.util_functions as util_functions
import logging


def trigger_action_if_low_stock(
        circus, stock_relationship, actor_id_field, restock_trigger,
        triggered_action_name, field_prefix=""):
    """

    Looks up the stock level of the specified relationship and submits that
    to the provided restock_trigger. If the result is true, we send a
    "force_act_next" to the specified action.

    :param circus:
    :param stock_relationship:
    :param actor_id_field:
    :param restock_trigger:
    :param triggered_action_name:
    :param field_prefix:
    :return:
    """

    return operations.Chain(
        stock_relationship.ops.get_neighbourhood_size(
            from_field=actor_id_field,
            named_as="{}CURRENT_STOCK".format(field_prefix)),

        restock_trigger.ops.generate(
            named_as="{}SHOULD_RESTOCK".format(field_prefix),
            observed_field="{}CURRENT_STOCK".format(field_prefix)),

        circus.get_action(triggered_action_name).ops.force_act_next(
            actor_id_field=actor_id_field,
            condition_field="{}SHOULD_RESTOCK".format(field_prefix)),
    )


def scale_quantity_gen(stock_size_gen, scale_factor):
    """
    stock_size_gen must be a generator of positive numbers (think of them as
     "quantities" of stuff, i.e on a ratio scale)

    This just builds another generator of numbers scaled a requested,
     making sure the generated numbers are always positive
    """

    if scale_factor is not None:
        return stock_size_gen\
            .map(f_vect=operations.scale(factor=scale_factor)) \
            .map(f=operations.bound_value(lb=1))

    return stock_size_gen


def create_distribution_link(circus, from_actor_name, to_actor_name):
    """
    Create random links between vendors in the SND hierarchy,
    e.g. "from pos to dealer L2", ...
    """

    logging.info("linking {} to {}".format(from_actor_name, to_actor_name))

    chose_one_upper = util_functions.make_random_assign(
        set1=circus.actors[from_actor_name].ids,
        set2=circus.actors[to_actor_name].ids,
        seed=circus.seeder.next())

    rel = circus.actors[from_actor_name].create_relationship(
        name=to_actor_name,
        seed=circus.seeder.next())

    rel.add_relations(
        from_ids=chose_one_upper["set1"],
        to_ids=chose_one_upper["chosen_from_set2"])


def add_bulk_restock_actions(circus, params,
                             buyer_actor_name, seller_actor_name):

    buyer = circus.actors[buyer_actor_name]
    seller = circus.actors[seller_actor_name]
    pos_per_buyer = circus.actors["pos"].size / buyer.size

    for product, description in params["products"].items():
        action_name = "{}_{}_bulk_purchase".format(buyer_actor_name, product)
        upper_level_restock_action_name = "{}_{}_bulk_purchase".format(seller_actor_name, product)

        logging.info("creating {} action".format(action_name))

        # trigger for another bulk purchase done by the seller if their own
        # stock get low
        seller_low_stock_bulk_purchase_trigger = random_generators.DependentTriggerGenerator(
            value_to_proba_mapper=operations.bounded_sigmoid(
                x_min=pos_per_buyer,
                x_max=description["max_pos_stock_triggering_pos_restock"] * pos_per_buyer,
                shape=description["restock_sigmoid_shape"],
                incrementing=False))

        # bulk size distribution is a scaled version of POS bulk size distribution
        bulk_size_gen = scale_quantity_gen(
            stock_size_gen=circus.generators["pos_{}_bulk_size_gen".format(product)],
            scale_factor=pos_per_buyer)

        build_purchase_action = circus.create_action(
            name=action_name,
            initiating_actor=buyer,
            actorid_field="BUYER_ID",

            # no timer or activity: dealers bulk purchases are triggered externally
        )

        build_purchase_action.set_operations(
            circus.clock.ops.timestamp(named_as="TIME"),

            buyer.get_relationship(seller_actor_name).ops.select_one(
                from_field="BUYER_ID",
                named_as="SELLER_ID"),

            bulk_size_gen.ops.generate(named_as="REQUESTED_BULK_SIZE"),

            buyer.get_relationship(product).ops\
                .get_neighbourhood_size(
                    from_field="BUYER_ID",
                    named_as="OLD_BUYER_STOCK"),

            # TODO: the perfect case would prevent to go over max_stock at this point

            # selecting and removing Sims from dealers
            seller.get_relationship(product).ops \
                .select_many(
                    from_field="SELLER_ID",
                    named_as="ITEMS_BULK",
                    quantity_field="REQUESTED_BULK_SIZE",

                    # if an item is selected, it is removed from the dealer's stock
                    pop=True,

                    # TODO: put this back to False and log the failed purchases
                    discard_missing=True),

            # and adding them to the buyer
            buyer.get_relationship(product).ops.add_grouped(
                from_field="BUYER_ID",
                grouped_items_field="ITEMS_BULK"),

            # We do not track the old and new stock of the dealer since the result
            # is misleading: since all purchases are performed in parallel,
            # if a dealer is selected several times, its stock level after the
            # select_many() is the level _after_ all purchases are done, which is
            # typically not what we want to include in the log.
            buyer.get_relationship(product).ops\
                .get_neighbourhood_size(
                    from_field="BUYER_ID",
                    named_as="NEW_BUYER_STOCK"),

            # actual number of bought items might be different due to out of stock
            operations.Apply(source_fields="ITEMS_BULK",
                             named_as="BULK_SIZE",
                             f=lambda s: s.map(len), f_args="series"),

            random_generators.SequencialGenerator(
                prefix="TX_{}_{}".format(buyer_actor_name,
                                         product)).ops.generate(
                named_as="TX_ID"),

            operations.FieldLogger(log_id=action_name,
                                   cols=["TIME",  "BUYER_ID", "SELLER_ID",
                                         "OLD_BUYER_STOCK", "NEW_BUYER_STOCK",
                                         "BULK_SIZE"]),

            trigger_action_if_low_stock(
                circus,
                stock_relationship=seller.get_relationship(product),
                actor_id_field="SELLER_ID",
                restock_trigger=seller_low_stock_bulk_purchase_trigger,
                triggered_action_name=upper_level_restock_action_name
            )
        )

