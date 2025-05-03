from collections import UserDict
from pathlib import Path

import yaml


class ModelDefinition(UserDict):
    def __init__(self, code: str):
        # load the model definition from the yaml file
        absolute_path = Path(Path(__file__).resolve().parent, f'trend/{code}.yaml').resolve()
        config = yaml.safe_load(open(absolute_path))
        # Initialize with empty dict if None provided
        super().__init__(config or {})

    @property
    def config(self):
        return self.data


class TrendModelDefinition(ModelDefinition):
    @property
    def overlay(self):
        return self.data.get('overlay', {})

    @property
    def factors(self):
        return self.data.get('factors', {})
