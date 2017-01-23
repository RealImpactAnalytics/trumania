import pandas as pd
import numpy as np
import logging
import main_1_build
from datagenerator.components import db
from datagenerator.core import operations
from datagenerator.core import util_functions
import os

circus_name = main_1_build.static_params["circus_name"]


def to_csv(df, target_file, write_mode):
    with open(target_file, write_mode) as of:
        df.to_csv(of, index=False, header=(write_mode == "w"))


def noisified(df, col, lb, col_type=np.float):
    fact = np.random.normal(1, .1, size=df.shape[0])
    col2 = df[col].apply(operations.bound_value(lb=lb)) * fact
    return col2.astype(col_type)


def create_distl1_daily_targets(product, write_mode):
    """
    Create some fake sellin and sellout target per distributor l1
    based on actual.
    """

    target_file = os.path.join(
        db.namespace_folder(circus_name),
        "distributor_product_sellin_sellout_target.csv")

    logging.info(
        " producing sellin sellout target for dist_l1s in {}"
        .format(target_file))

    # contains info on dist_l1 bulk purchases (dist_l1 buys from telco)
    # TIME, BUYER_ID, SELLER_ID, OLD_BUYER_STOCK, NEW_BUYER_STOCK, BULK_SIZE
    input_file_name = "output/{}/dist_l1_" \
                      "{}_bulk_purchase_stock.csv".format(
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
            col = "_".join([direction, metric])
            if metric == "target_units":
                mean_daily_sells[col] = noisified(mean_daily_sells,
                                                  col=metric, lb=1,
                                                  col_type=np.int)
            else:
                mean_daily_sells[col] = noisified(mean_daily_sells, col=metric,
                                                  lb=100)

    mean_daily_sells.drop(
            ["target_units", "target_value"], axis=1, inplace=True)
    mean_daily_sells.reset_index(inplace=True)
    mean_daily_sells["product_type_id"] = product
    mean_daily_sells.rename(columns={"BUYER_ID": "distributor_id"},
                            inplace=True)

    to_csv(mean_daily_sells, target_file, write_mode)


def create_distl1_daily_geo_targets(product, write_mode, nrows=None):
    """
    Create some fake daily geo_l2 target per product/distributor
    """

    target_file = os.path.join(
        db.namespace_folder(circus_name),
        "distributor_product_geol2_sellout_target.csv")

    logging.info(
        " producing geo_l2 sellout target for dist_l1s in {}"
        .format(target_file))

    # contains info on dist_l1 bulk purchases (dist_l1 buys from telco)
    # CUST_ID, SITE, POS, CELL_ID, geo_level2_id, distributor_l1, INSTANCE_ID,
    # PRODUCT_ID,FAILED_SALE_OUT_OF_STOCK,TX_ID,VALUE,TIME
    input_file_name = "output/{}/customer_{}_purchase.csv".format(
        circus_name, product)

    customer_purchases = pd.read_csv(
        input_file_name, parse_dates=[11], nrows=nrows)
    customer_purchases["day"] = customer_purchases["TIME"].apply(
        lambda s: s.strftime("%D"))

    customer_purchases["product_type_id"] = product

    mean_daily_sells = customer_purchases \
        .groupby(["product_type_id", "distributor_l1",
                  "geo_level2_id", "day"])["VALUE"] \
        .agg({"sellout_target_units": len, "sellout_target_value": np.sum}) \
        .groupby(level=[0, 1, 2]) \
        .median()\
        .reset_index()

    mean_daily_sells = mean_daily_sells.rename(columns={
        "distributor_l1": "distributor_id"
    })

    mean_daily_sells["sellout_target_units"] = noisified(
        mean_daily_sells, col="sellout_target_units", lb=25, col_type=np.int)

    mean_daily_sells["sellout_target_value"] = noisified(
        mean_daily_sells, col="sellout_target_value", lb=100)

    to_csv(mean_daily_sells, target_file, write_mode)


if __name__ == "__main__":

    util_functions.setup_logging()

    write_mode = "w"

    for product in main_1_build.static_params["products"].keys():

        logging.info("computing targets for {}".format(product))

        create_distl1_daily_targets(product, write_mode)
        create_distl1_daily_geo_targets(product, write_mode)

        write_mode = "a"
