import os
import pandas as pd
from datagenerator.core.attribute import Attribute


class ExternalModel(object):
    """
    This is just the provider of the IO methods save and re

    TODO: we should store this elsewhere than in the git repo...
    """

    def attribute_path(self, root_dir, attribute_name):
        file_name = "{}.attribute.csv".format(attribute_name)
        return os.path.join(root_dir, file_name)
