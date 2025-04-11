"""
Abstraction for storage backends used for data ingestion.
    - retrieving should be done via Mantle.
    - this module is meant to be used for writing.

1. Use factory pattern to create instances of writers for different storage backends
2. currently support S3 and local
3. Functionality includes
    - instantiate class with backend type and file format
        - use enums for backend files
    - Writing data to the storage backend
    - Handling different file formats (e.g., CSV, Parquet)
    - Writing a single or multiple files (same function)
    - Error handling and logging
"""

import os
from abc import abstractmethod
from pathlib import Path

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from tecton.core.const import StorageBackend
from tecton.data.apitools.aws import get_s3_resource
from tecton.data.util import write_bytes


class ParquetWriter:
    """
    Abstract base class for writing Parquet files to different storage backends.
    """

    @abstractmethod
    def write(self, key: str, data: pa.Table | pl.DataFrame | pd.DataFrame) -> None:
        """
        Write data to the specified storage backend.
        :param key: The identifier for the file (e.g., S3 object key or local file name).
        :param data: The data to write (e.g., pandas DataFrame).
        """
        pass


class S3ParquetWriter(ParquetWriter):
    def __init__(self, bucket_name: str):
        """
        Initialize the S3TableWriter with the bucket name and file format.
        :param bucket_name: The name of the S3 bucket to write to.
        """
        self.bucket_name = bucket_name
        self.file_format = 'parquet'
        self.s3 = get_s3_resource()

    def write(self, key: str, data: pa.Table | pl.DataFrame | pd.DataFrame) -> None:
        """
        Write data to S3.
        :param key: The S3 object key where the data will be stored.
        :param data: The data to write (e.g., pandas DataFrame).
        """
        buffer = write_bytes(data)
        #
        s3_client = self.s3.get_client()
        s3_client.put_object(
            Bucket=self.bucket_name,
            Key=f'{key}.{self.file_format}',
            Body=buffer,
        )


class LocalParquetWriter(ParquetWriter):
    def __init__(self, base_dir: str):
        """
        Initialize the LocalParquetWriter with the directory path.
        :param base_dir: The local directory to write files to.
        """
        self.base_dir = base_dir
        self.file_format = 'parquet'

    def write(self, key: str, data: pa.Table | pl.DataFrame | pd.DataFrame) -> None:
        """
        Write data to a local file.
        :param key: The file name (without extension) where the data will be stored.
        :param data: The data to write (e.g., pandas DataFrame).
        """

        path = Path(f'{self.base_dir}/{key}.{self.file_format}')
        os.makedirs(path.parent.resolve(), exist_ok=True)

        if isinstance(data, pa.Table):
            table = data
        elif isinstance(data, pl.DataFrame):
            table = data.to_arrow()
        else:
            table = pa.Table.from_pandas(data)
        # write the table using pyarrow
        pq.write_table(
            table,
            path,  # Save to the local path
            compression='snappy',  # Optional: specify compression
        )


class ParquetWriterFactory:
    """
    Factory class to create instances of writers for different storage backends.
    """

    @staticmethod
    def create(storage_backend: StorageBackend) -> ParquetWriter:
        if storage_backend == StorageBackend.S3:
            bucket_name = os.environ['S3_BUCKET']
            return S3ParquetWriter(bucket_name=bucket_name)
        elif storage_backend == StorageBackend.LOCAL:
            base_dir = os.environ['LOCAL_DATA_DIR']
            return LocalParquetWriter(base_dir=base_dir)
        else:
            raise ValueError(f'Unsupported storage type: {storage_backend}')
