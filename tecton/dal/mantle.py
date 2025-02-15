import datetime as dt
import os
from pathlib import Path

import ibis
import yaml


class Mantle:
    def __init__(self):
        self._con = ibis.duckdb.connect()
        # Enable DuckDB's S3 access
        self._con.raw_sql(f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';
            SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';
            SET s3_region='{os.environ['AWS_DEFAULT_REGION']}';""")
        self._s3_bucket = os.environ['S3_BUCKET']
        absolute_path = Path(Path(__file__).resolve().parent, 'table_config.yaml').resolve()
        self._config = yaml.safe_load(open(absolute_path))

    def select(
        self,
        table: str,
        start_date: dt.date = None,
        end_date: dt.date = None,
        columns: list | tuple = None,
    ) -> ibis.expr.types.Table:
        cfg = self._config[table]

        s3_path = f's3://{self._s3_bucket}{cfg["path"]}*.parquet'
        table = self.get_files(s3_path)
        if start_date:
            table = table.filter(table.date >= start_date)
        if end_date:
            table = table.filter(table.date <= end_date)
        if columns is not None:
            table = table.select(columns)
        return table

    def get_files(self, path: str) -> ibis.expr.types.Table:
        p = Path(path)
        match p.suffix:
            case '.csv':
                return self._con.read_csv(path)
            case ('.parquet', '.pq'):
                return self._con.read_parquet(path)
            case _:
                raise ValueError(f'Unsupported file type: {p.suffix}')
