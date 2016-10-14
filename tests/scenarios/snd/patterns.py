"""
Re-usable operation chains throughout the SND scenario
"""
import datagenerator.core.operations as operations
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

