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


def add_bulk_restock_actions(circus, params,
                             buyer_actor_name, seller_actor_name):

    buyer = circus.actors[buyer_actor_name]
    seller = circus.actors[seller_actor_name]
    pos_per_buyer = circus.actors["pos"].size / buyer.size

    for product, description in params["products"].items():
        action_name = "{}_{}_bulk_purchase".format(buyer_actor_name, product)
        upper_level_restock_action_name = "{}_{}_bulk_purchase".format(seller_actor_name, product)

        logging.info("creating {} action".format(action_name))

        # generator of item prices and type
        item_price_gen = random_generators.NumpyRandomGenerator(
            method="choice", a=description["item_prices"],
            seed=circus.seeder.next())

        item_prices_gen = random_generators.DependentBulkGenerator(
            element_generator=item_price_gen)

        item_type_gen = random_generators.NumpyRandomGenerator(
            method="choice",
            a=circus.actors[product].ids,
            seed=circus.seeder.next())

        item_types_gen = random_generators.DependentBulkGenerator(
            element_generator=item_type_gen)

        tx_gen = random_generators.SequencialGenerator(
            prefix="_".join(["TX", buyer_actor_name, product]))

        tx_seq_gen = random_generators.DependentBulkGenerator(
            element_generator=tx_gen)

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

            buyer.get_relationship("{}__provider".format(product))\
                .ops.select_one(from_field="BUYER_ID",
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
                    named_as="ITEM_IDS",
                    quantity_field="REQUESTED_BULK_SIZE",

                    # if an item is selected, it is removed from the dealer's stock
                    pop=True,

                    # TODO: put this back to False and log the failed purchases
                    discard_missing=True),

            # and adding them to the buyer
            buyer.get_relationship(product).ops.add_grouped(
                from_field="BUYER_ID",
                grouped_items_field="ITEM_IDS"),

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
            operations.Apply(source_fields="ITEM_IDS",
                             named_as="BULK_SIZE",
                             f=lambda s: s.map(len), f_args="series"),

            # Generate some item prices. Note that the same items will have a
            # different price through the whole distribution chain
            item_prices_gen.ops.generate(
                named_as="ITEM_PRICES",
                observed_field="BULK_SIZE"
            ),

            item_types_gen.ops.generate(
                named_as="ITEM_TYPES",
                observed_field="BULK_SIZE"
            ),

            tx_seq_gen.ops.generate(
                named_as="TX_IDS",
                observed_field="BULK_SIZE"
            ),

            operations.FieldLogger(log_id="{}_stock".format(action_name),
                                   cols=["TIME", "BUYER_ID", "SELLER_ID",
                                         "OLD_BUYER_STOCK", "NEW_BUYER_STOCK",
                                         "BULK_SIZE"]),

            operations.FieldLogger(log_id=action_name,
                                   cols=["TIME", "BUYER_ID", "SELLER_ID"],
                                   exploded_cols=["TX_IDS", "ITEM_IDS",
                                                  "ITEM_PRICES", "ITEM_TYPES"]),

            trigger_action_if_low_stock(
                circus,
                stock_relationship=seller.get_relationship(product),
                actor_id_field="SELLER_ID",
                restock_trigger=seller_low_stock_bulk_purchase_trigger,
                triggered_action_name=upper_level_restock_action_name
            )
        )

