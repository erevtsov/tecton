from pathlib import Path

import yaml


class ModelDefinition:
    def __init__(self, code: str):
        # load the model definition from the yaml file
        absolute_path = Path(Path(__file__).resolve().parent, f'trend/{code}.yaml').resolve()
        self._config = yaml.safe_load(open(absolute_path))

    @property
    def config(self):
        return self._config

    def get(self, key: str, default=None):
        return self._config.get(key, default)
