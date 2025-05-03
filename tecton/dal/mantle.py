import datetime as dt
import os
from pathlib import Path

import ibis
import pandas as pd
import yaml

from tecton.core.const import StorageBackend
from tecton.core.util import TableConfig, TableSet


class Mantle:
    def __init__(
        self,
        storage_backend: StorageBackend = None,
    ):
        self._con = ibis.duckdb.connect()
        self._storage_backend = storage_backend or StorageBackend[os.environ['STORAGE_BACKEND'].upper()]
        # Enable DuckDB's S3 access
        if self._storage_backend == StorageBackend.S3:
            self._con.raw_sql(f"""
                INSTALL httpfs;
                LOAD httpfs;
                SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';
                SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';
                SET s3_region='{os.environ['AWS_DEFAULT_REGION']}';""")
            self._storage_type_prefix = 's3://'
            self._root_path = os.environ['S3_BUCKET']
        elif self._storage_backend == StorageBackend.LOCAL:
            self._storage_type_prefix = ''
            self._root_path = str(Path(os.environ['LOCAL_DATA_DIR']).resolve())
        absolute_path = Path(Path(__file__).resolve().parent, 'table_config.yaml').resolve()
        self._config = yaml.safe_load(open(absolute_path))
        self.Tables = TableSet(self._config)

    def _get_file_path(self, table: TableConfig, start_date: dt.date, end_date: dt.date) -> str | list[str]:
        """
        Get the file path(s) for the specified table based on the configuration and date range.

        :param table: The name of the table to get the file path for.
        :param start_date: The start date for filtering the data (optional).
        :param end_date: The end date for filtering the data (optional).

        :return: A string or list of strings representing the file path(s) for the specified table.
        """
        yearmonths = None
        if table.partition.get('freq') == 'monthly' and start_date and end_date:
            # only scan the necessary files
            yearmonths = (
                pd.date_range(
                    start=max(start_date, table.partition.get('first', start_date)),
                    end=end_date,
                    freq='MS',  # Month Start
                )
                .strftime('%Y%m')
                .tolist()
            )
        if yearmonths is not None and len(yearmonths) == 0:
            path = [f'{self._storage_type_prefix}{self._root_path}{table.path}{ym}.parquet' for ym in yearmonths]
        else:
            path = f'{self._storage_type_prefix}{self._root_path}{table.path}*.parquet'

        return path

    def get_files(self, path: str | list[str]) -> ibis.expr.types.Table:
        """
        Load files from the specified path(s) into an Ibis table.

        :param path: The path or list of paths to the files to load. This can be a single string or a list of strings.

        :return: An Ibis table representing the data loaded from the specified files.
        """
        if isinstance(path, str):
            path = [path]
        p = Path(path[0])
        match p.suffix:
            case '.csv':
                return self._con.read_csv(path)
            case '.parquet' | '.pq':
                return self._con.read_parquet(path)
            case _:
                raise ValueError(f'Unsupported file type: {p.suffix}')

    def select(
        self,
        table: TableConfig,
        start_date: dt.date = None,
        end_date: dt.date = None,
        columns: list | tuple = None,
    ) -> ibis.expr.types.Table:
        """
        Select data from specified table.

        :param table: The name of the table to select from.
        :param start_date: The start date for filtering the data (optional).
        :param end_date: The end date for filtering the data (optional).
        :param columns: The columns to select from the table (optional). If None, all columns will be selected.

        :return: An Ibis table with the selected data, filtered by the specified date range and columns.
        """
        path = self._get_file_path(table=table, start_date=start_date, end_date=end_date)
        res = self.get_files(path)
        #
        if start_date:
            res = res.filter(res.date >= start_date)
        if end_date:
            res = res.filter(res.date <= end_date)
        if columns is not None:
            res = res.select(columns)
        return res
