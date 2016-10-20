from datagenerator.core import random_generators


POS_NAMES = ["WEBCALLCENTER", "RI-GROUP", "DATA UNICORN", "CALAOS", "QINZ",
             "OINOI", "CLIXXO", "ACTION TREE", "6OMEGA", "DATAMATIAN",
             "QUICK-BEE", "GUIDED-ANALYTICS", "RI-ANALYTICS", "BLUE STAG",
             "DATA-SAPIENS", "TINY TREE", "OROCANTO", "CLIXXO", "XEBI",
             "PREGASUS", "HELIXIRE", "ALICANTIO", "PIXABYTES", "KUNOOZ"]


def name_gen(seed):
    return random_generators.NumpyRandomGenerator(
        method="choice", seed=seed, a=POS_NAMES)


CONTACT_NAMES = [
    "Angella Prisco",
    "Randolph Favela",
    "Vickey Mounsey",
    "Hipolito Kittinger",
    "Julieta Betterton",
    "Carin Maitland",
    "Kimiko Wiedeman",
    "Connie Stanko",
    "Jospeh Vergara",
    "Dacia Crockett",
    "Logan Clapp",
    "Coreen Hoxie",
    "Corazon Dines",
    "Tyree Widman",
    "Darron Craver",
    "Hermila Penfold",
    "Shanna Jaffe",
    "Deadra Payton",
    "Scarlet Champine",
    "Marth Wheeler",
    "Olive Hessel",
    "Lasandra Foulger",
    "Marcelino Henegar",
    "Guillermina Benedetti",
    "Kristen Janusz",
    "Violeta Tolbert",
    "Deeanna Amundson",
    "Denisse Adams",
    "Cristopher Coles",
    "Kymberly Mends",
    "Chloe Rothenberg",
    "Zonia Corns",
    "Gonzalo Gao",
    "Glennie Menjivar",
    "Tonita Manley",
    "Thomasena Yarger",
    "Adina Pacheo",
    "Chassidy Horne",
    "Brittni Mertens",
    "Kristofer Fenley",
    "Enola Rudder",
    "Kati Sharma",
    "Kenton Demaio",
    "Lawanna Jess",
    "Han Shumway",
    "Roseanne Perea",
    "Georgiann Yoshida",
    "Danika Gierlach",
    "Beulah Glenn",
    "Adolfo Fuselier"
]


def contact_name_gen(seed):
    return random_generators.NumpyRandomGenerator(
        method="choice", seed=seed, a=POS_NAMES)

INTERNAL_TRANSACTION_TYPES = ["origin_to_mass_distributor",
                              "mass_distributor_to_dealer",
                              "mass_distributor_to_pos",
                              "dealer_to_dealer",
                              "dealer_to_pos",
                              "origin_to_retail",
                              "retail_to_pos"]

DISTRIBUTOR_NAMES = ["DEVOS & LEMMENS DISTRIB", "LA WILLIAMS DISTRIB",
                     "HEINZ DISTRIB", "AMORA DISTRIB", "CALVE DISTRIB"]

HANDSET_CATEGORY = ["basic", "feature", "smartphone"]

HANDSET_BRANDS = ["Samsung", "iPhone", "Sony", "Nokia", "HTC", "Nexus"]

SIM_TYPES = ["mini", "nano", "micro", "pico"]

SIM_CAP = ["2G", "3G", "4G", "5G"]
