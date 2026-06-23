import yaml


class ConfigLoader:

    @staticmethod
    def load(path):

        with open(path, "r") as file:
            return yaml.safe_load(file)
