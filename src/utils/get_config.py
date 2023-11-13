import yaml
from box import Box
import os


def get_config() -> Box:
    print(os.getcwd())
    with open(r"configs/config.yaml") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
        config = Box(config)
    return config


def get_feature_definitions() -> Box:
    print(os.getcwd())
    with open(r"configs/feature_set_definitions.yaml") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
        config = Box(config)
    return config


def get_config_from_path(path: str) -> Box:
    with open(path) as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
        config = Box(config)
    return config


config = get_config()
feature_set_definitions = get_feature_definitions()
