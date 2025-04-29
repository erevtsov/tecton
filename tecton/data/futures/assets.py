import calendar
import datetime as dt
import os
from pathlib import Path

import dagster as dg

from tecton.core.const import StorageBackend
from tecton.dal.mantle import Mantle
from tecton.data.apitools.databento import (
    process_definition_data,
    process_statistics_data,
)
from tecton.data.apitools.writer import ParquetWriterFactory
from tecton.data.futures.ops import construct_continuous_ticker

monthly_partitions = dg.MonthlyPartitionsDefinition(start_date='2010-06-01', end_offset=1)

# TODO: move this to a config file
STORAGE_BACKEND = StorageBackend[os.environ['STORAGE_BACKEND'].upper()]
if STORAGE_BACKEND == StorageBackend.S3:
    s3_bucket_name = os.environ['S3_BUCKET']
    stats_core_path = f's3://{s3_bucket_name}/databento/statistics/glbx-mdp3-'
    desc_core_path = f's3://{s3_bucket_name}/databento/definition/glbx-mdp3-'
elif STORAGE_BACKEND == StorageBackend.LOCAL:
    LOCAL_DATA_DIR = os.environ['LOCAL_DATA_DIR']
    stats_core_path = str(Path(f'{LOCAL_DATA_DIR}/databento/statistics/glbx-mdp3-').resolve())
    desc_core_path = str(Path(f'{LOCAL_DATA_DIR}/databento/definition/glbx-mdp3-').resolve())

stats_suffix = '.statistics.csv'
desc_suffix = '.definition.csv'


@dg.asset(
    partitions_def=monthly_partitions,
    group_name='futures',
)
def futures_discrete_data(context: dg.AssetExecutionContext) -> None:
    date = context.partition_key
    m = Mantle()
    date = dt.datetime.strptime(date, '%Y-%m-%d')
    year_month = dt.datetime.strftime(date, '%Y%m')
    # descriptive/definition data
    desc_path = desc_core_path + year_month + '*-' + year_month + '*' + desc_suffix
    desc = m.get_files(desc_path).to_polars()
    desc = process_definition_data(desc)
    # statistic data (daily)
    stats_path = stats_core_path + year_month + '*-' + year_month + '*' + stats_suffix
    stats = m.get_files(stats_path).to_polars()
    stats = process_statistics_data(stats)
    # join the descriptive data with statistics data
    agg = desc.join(
        stats,
        how='left',
        on=['date', 'instrument_id'],
        validate='1:1',
        coalesce=True,
    )
    # write output
    writer = ParquetWriterFactory.create(storage_backend=STORAGE_BACKEND)
    writer.write(
        key=f'futures/{year_month}',
        data=agg,
    )


@dg.asset(
    partitions_def=monthly_partitions,
    group_name='futures',
    deps=[futures_discrete_data],
)
def futures_continuous_data(context: dg.AssetExecutionContext) -> None:
    """
    Backfill job for continuous futures data.
    Depends on the futures_backfill_by_month asset.
    TODO: add helper function with the continuous logic. This func should have 2 options:
        (1) use open interest or volume WITHOUT smoothing
        (2) use the same logic, but have a smoothing window
    """
    date = context.partition_key
    date = dt.datetime.strptime(date, '%Y-%m-%d')
    year_month = dt.datetime.strftime(date, '%Y%m')
    start_date = date.replace(day=1).date()
    end_date = date.replace(day=calendar.monthrange(date.year, date.month)[1]).date()
    #
    m = Mantle()
    table = m.select(m.Tables.futures.discrete, start_date=start_date, end_date=end_date)
    res = construct_continuous_ticker(data=table.to_polars())
    # write results
    writer = ParquetWriterFactory.create(storage_backend=STORAGE_BACKEND)
    writer.write(
        key=f'futures-cont/{year_month}',
        data=res,
    )
