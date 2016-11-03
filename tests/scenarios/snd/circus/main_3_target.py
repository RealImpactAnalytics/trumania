import pandas as pd
import numpy as np
import logging
import main_1_build
from datagenerator.components import db
import os


def create_distl2_daily_targets(product, write_mode):
    """
        Create some fake sellin and sellout target per distributor l2
        based on actual.
    """

    circus_name = main_1_build.static_params["circus_name"]

    target_file = os.path.join(
        db.namespace_folder(circus_name),
        "distributor_product_sellin_sellout_target.csv")

    logging.info(
        "producing distributor sellin sellout target in {}".format(target_file))

    input_file_name = "snd_output_logs/{}/dist_l1_{}_bulk_purchase_.csv".format(
        circus_name, product)

    bulk_purchases = pd.read_csv(input_file_name, parse_dates=[0])
    bulk_purchases["day"] = bulk_purchases["TIME"].apply(
        lambda s: s.strftime("%D"))

    mean_daily_sells = bulk_purchases\
        .groupby(["BUYER_ID", "day"])["BULK_SIZE"]\
        .agg({"target_units": len, "target_value": np.sum})\
        .groupby(level=0)\
        .median()

    for direction in ["sellin", "sellout"]:
        for metric in ["target_units", "target_value"]:
            fact = np.random.normal(1, .1, size=mean_daily_sells.shape[0])
            col = "_".join([direction, metric])

            mean_daily_sells[col] = mean_daily_sells[metric] * fact

            if metric == "target_units":
                mean_daily_sells[col] = mean_daily_sells[col].astype(np.int)
                mean_daily_sells.ix[mean_daily_sells[col] < 1, col] = 1
            else:
                mean_daily_sells.ix[mean_daily_sells[col] < 100, col] = 100

    mean_daily_sells.drop(
            ["target_units", "target_value"], axis=1, inplace=True)
    mean_daily_sells.reset_index(inplace=True)
    mean_daily_sells["product_type_id"] = product
    mean_daily_sells.rename(columns={"BUYER_ID": "distributor_id"},
                            inplace=True)

    with open(target_file, write_mode) as of:
        mean_daily_sells.to_csv(of, index=False)


if __name__ == "__main__":

    write_mode = "w"

    for product in main_1_build.static_params["products"].keys():
        create_distl2_daily_targets("mfs", write_mode)
        write_mode = "a"

