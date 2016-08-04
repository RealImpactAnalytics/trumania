"""
    Extension for the CDR scenarios
"""

from bi.ria.generator.relationship import *


def compute_call_value(data):
    """
    custom function expecting a dataframe with a single column: DURATION
    """

    price_per_second = 2
    df = data[["DURATION"]] * price_per_second

    # must return a dataframe with a single column named "result"
    return df.rename(columns={"DURATION": "result"})


# TODO: cf Sipho suggestion: we could have generic "add", "diff"... operations
def substract_value_from_account(data):
    """
    custom function expecting a dataframe with a 2 column: MAIN_ACCT_OLD and
    VALUE, and computes the new account value from that
    """

    # maybe we should prevent negative accounts here? or not?
    new_value = data["MAIN_ACCT_OLD"] - data["VALUE"]

    # must return a dataframe with a single column named "result"
    return pd.DataFrame(new_value, columns=["result"])

def add_value_to_account(data):
    """
    custom function expecting a dataframe with a 2 column: MAIN_ACCT_OLD and
    VALUE, and computes the new account value from that
    """

    # maybe we should prevent negative accounts here? or not?
    new_value = data["MAIN_ACCT_OLD"] + data["VALUE"]

    # must return a dataframe with a single column named "result"
    return pd.DataFrame(new_value, columns=["result"])


def copy_id_if_topup(data):
    """
    """

    copied_ids = data[data["SHOULD_TOP_UP"]][["A_ID"]].reindex(data.index)

    return copied_ids.rename(columns={"A_ID": "result"})

