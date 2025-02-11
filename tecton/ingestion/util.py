import io
import re

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq


def write_bytes(table):
    if isinstance(table, pa.Table):
        table = table
    elif isinstance(table, pl.DataFrame):
        table = table.to_arrow()
    else:
        table = pa.Table.from_pandas(table)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    return buffer


def to_snake_case(name: str | list):
    if isinstance(name, list):
        return [to_snake_case(n) for n in name]
    name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)  # Convert camelCase to snake_case
    name = re.sub(r'\s+', '_', name)  # Replace spaces with underscores
    return name.lower()  # Convert to lowercase
