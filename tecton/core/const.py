from enum import Enum


class StorageBackend(Enum):
    S3 = 'S3'  # Amazon S3
    LOCAL = 'LOCAL'  # Local file system
