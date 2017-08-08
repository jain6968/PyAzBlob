import os
import configparser

__all__ = ["config"]


class Configuration:

    def __init__(self, account_name, account_sas, container_name):
        self.account_name = account_name
        self.account_sas = account_sas
        self.container_name = container_name


config_parser = configparser.ConfigParser()

# Read from settings.ini
config_parser.read("settings.ini")

# Read from environmental variable, if specified (overriding values in settings.ini)
account_name = os.environ.get("PYAZ_ACCOUNT_NAME", None)
account_sas = os.environ.get("PYAZ_ACCOUNT_SAS", None)
container_name = os.environ.get("PYAZ_CONTAINER_NAME", None)


config = Configuration(config_parser["StorageAccount"].get("name"),
                       config_parser["StorageAccount"].get("sas"),
                       config_parser["StorageAccount"].get("container"))


if account_name:
    config.account_name = account_name

if account_sas:
    config.account_sas = account_sas

if container_name:
    config.container_name = container_name

