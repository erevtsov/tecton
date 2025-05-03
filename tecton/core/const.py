from enum import Enum
from typing import NamedTuple


class StorageBackend(Enum):
    S3 = 'S3'  # Amazon S3
    LOCAL = 'LOCAL'  # Local file system


class ModelRun(NamedTuple):
    model_code: str
    run_id: int
