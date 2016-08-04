"""
    Extension for the CDR scenarios
"""

from bi.ria.generator.relationship import *


class ComputeCallValue(AddColumns):
    """
        Custom operation for the CDR scenario: computes a price from the
        duration
    """

    def __init__(self, price_per_second, named_as):
        super(ComputeCallValue, self).__init__()
        self.price_per_second = price_per_second
        self.named_as = named_as

    def build_output(self, data):
        # we can hard-code "DURATION" here since it's scenario-specific
        df = data[["DURATION"]] * self.price_per_second

        return df.rename(columns={"DURATION": self.named_as})


class AgentRelationship(Relationship):
    """
        relation from user to agent (seller). When selecting one seller,
        the operation returns both the chosen agent and the price of the item
    """
    def __init__(self, **kwargs):
        """

        :param r1:
        :param r2:
        :param chooser:
        :param agents:
        :return:
        """
        Relationship.__init__(self, **kwargs)

    def select_one(self, **kwargs):
        choices = Relationship.select_one(self, **kwargs)
        choices["value"] = 1000

        return choices