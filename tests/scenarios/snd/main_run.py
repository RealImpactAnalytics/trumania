from datagenerator.core import circus
from datagenerator.core import util_functions
import logging
import pandas as pd

import snd_customers
import snd_pos
import snd_dealer
import snd_field_agents

runtime_params = {
    "sim_price": 10,

    "mean_daily_customer_mobility_activity": .2,
    "std_daily_customer_mobility_activity": .2,

    "mean_daily_fa_mobility_activity": 1,
    "std_daily_fa_mobility_activity": .2,

    # average number of days between 2 item purchase by the same customer
    "customer_er_purchase_min_period_days": 1,
    "customer_er_purchase_max_period_days": 9,

    "customer_sim_purchase_min_period_days": 60,
    "customer_sim_purchase_max_period_days": 360,

    # largest possible er or SIM stock level that can trigger a restock
    "max_pos_er_stock_triggering_restock": 50,
    "pos_er_restock_shape": 2,
    "max_pos_sim_stock_triggering_restock": 10,
    "pos_sim_restock_shape": 5,

    "pos_ers_max_stock": 500,
    "pos_sim_max_stock": 500,

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
    snd_dealer.add_telco_restock_actions(snd)
    snd_dealer.add_dealers_l1_bulk_purchase_actions(snd, runtime_params)
    snd_dealer.add_dealers_l2_bulk_purchase_actions(snd, runtime_params)
    snd_pos.add_bulk_purchases_action(snd, runtime_params)

    snd_customers.add_purchase_sim_action(snd, runtime_params)
    snd_customers.add_purchase_er_action(snd, runtime_params)

    snd_field_agents.add_mobility_action(snd, runtime_params)
    snd_field_agents.add_survey_action(snd)

    snd.run(
        duration=pd.Timedelta("1 days"),
        log_output_folder="snd_output_logs/{}".format(snd.name),
        delete_existing_logs=True)
