from datagenerator.core import circus
from datagenerator.core import util_functions
import logging
import pandas as pd

import snd_customers
import snd_pos
import snd_dealer
import patterns
import snd_field_agents

runtime_params = {
    "mean_daily_customer_mobility_activity": .2,
    "std_daily_customer_mobility_activity": .2,

    "mean_daily_fa_mobility_activity": 1,
    "std_daily_fa_mobility_activity": .2,

    "products":  {
        "SIM": {
            "customer_purchase_min_period_days": 60,
            "customer_purchase_max_period_days": 360,

            "max_pos_stock_triggering_pos_restock": 10,
            "restock_sigmoid_shape": 5,

            "pos_max_stock": 35,
            "item_prices": [10]
        },
        # electronic recharges
        "ER": {
            "customer_purchase_min_period_days": 1,
            "customer_purchase_max_period_days": 9,

            "max_pos_stock_triggering_pos_restock": 50,
            "restock_sigmoid_shape": 2,

            "pos_max_stock": 500,

            "item_prices": [5, 10, 15, 25, 45]
        },
        # physical recharges
        "PR": {
            "customer_purchase_min_period_days": 2,
            "customer_purchase_max_period_days": 15,

            "max_pos_stock_triggering_pos_restock": 40,
            "restock_sigmoid_shape": 2,

            "pos_max_stock": 300,

            "item_prices": [5, 10, 15, 25, 45]
        },
        "MFS": {
            "customer_purchase_min_period_days": 1,
            "customer_purchase_max_period_days": 5,

            "max_pos_stock_triggering_pos_restock": 75,
            "restock_sigmoid_shape": 2,

            "pos_max_stock": 500,

            "item_prices": [1, 5, 10, 25, 50, 75, 100]
        },
        "HANDSET": {
            "customer_purchase_min_period_days": 180,
            "customer_purchase_max_period_days": 1000,

            "max_pos_stock_triggering_pos_restock": 4,
            "restock_sigmoid_shape": 2,

            "pos_max_stock": 10,

            "item_prices": [230, 410, 515, 234, 645]
        }
    }


}


if __name__ == "__main__":

    util_functions.setup_logging()

    snd = circus.Circus.load_from_db(circus_name="snd_v2")
    logging.info("loaded circus:\n{}".format(snd))

    snd_customers.add_mobility_action(snd, runtime_params)

    snd_pos.add_attractiveness_evolution_action(snd)
    snd_pos.add_pos_stock_log_action(snd)

    # restock action must be built in reverse order since they refer to each other
    # TODO: we should fix that since this also influence the order of the executions
    # => we'd like to re-stock directly, not with delays due to the size of the hierarchy
    snd_dealer.add_telco_restock_actions(snd, runtime_params)
    patterns.add_bulk_restock_actions(snd, runtime_params,
                                      buyer_actor_name="dealers_l1",
                                      seller_actor_name="telcos")

    patterns.add_bulk_restock_actions(snd, runtime_params,
                                      buyer_actor_name="dealers_l2",
                                      seller_actor_name="dealers_l1")

    patterns.add_bulk_restock_actions(snd, runtime_params,
                                      buyer_actor_name="pos",
                                      seller_actor_name="dealers_l2")

    snd_customers.add_purchase_actions(snd, runtime_params)

    snd_field_agents.add_mobility_action(snd, runtime_params)
    snd_field_agents.add_survey_action(snd)

    snd.run(
        duration=pd.Timedelta("30 days"),
        log_output_folder="snd_output_logs/{}".format(snd.name),
        delete_existing_logs=True)
