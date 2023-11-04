import yaml
from box import Box
import os


def get_config():
    print(os.getcwd())
    with open(r"config.yaml") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
        config = Box(config)
    return config

config = get_config()
