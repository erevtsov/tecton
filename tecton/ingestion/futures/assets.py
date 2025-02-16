import calendar
import datetime as dt
import os

import dagster as dg
import dagster_aws.s3 as s3

from tecton.dal.mantle import Mantle
from tecton.ingestion.apitools.aws import get_s3_resource
from tecton.ingestion.apitools.databento import (
    construct_continuous_ticker,
    process_definition_data,
    process_statistics_data,
)
from tecton.ingestion.util import write_bytes

monthly_partitions = dg.MonthlyPartitionsDefinition(start_date='2024-02-01')

# TODO: move this to a config file
stats_core_path = 's3://synqvest/databento/statistics/glbx-mdp3-'
stats_suffix = '.statistics.csv'
desc_core_path = 's3://synqvest/databento/definition/glbx-mdp3-'
desc_suffix = '.definition.csv'


@dg.asset(
    partitions_def=monthly_partitions,
    group_name='futures',
)
def futures_backfill_by_month(context: dg.AssetExecutionContext, s3: s3.S3Resource) -> None:
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
    agg = desc.join(
        stats,
        how='left',
        on=['date', 'instrument_id'],
        validate='1:1',
        coalesce=True,
    )
    #
    buffer = write_bytes(agg)
    #
    s3_client = s3.get_client()
    s3_client.put_object(
        Bucket=os.environ['S3_BUCKET'],
        Key=f'futures/{year_month}.parquet',
        Body=buffer,
    )


@dg.asset(
    partitions_def=monthly_partitions,
    group_name='futures',
    deps=[futures_backfill_by_month],
)
def futures_continuous_backfill_by_month(context: dg.AssetExecutionContext, s3: s3.S3Resource) -> None:
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
    table = m.select('futures', start_date=start_date, end_date=end_date)
    res = construct_continuous_ticker(data=table.to_polars())
    #
    buffer = write_bytes(res)
    #
    s3_client = s3.get_client()
    s3_client.put_object(
        Bucket=os.environ['S3_BUCKET'],
        Key=f'futures-cont/{year_month}.parquet',
        Body=buffer,
    )


# Define the Definitions object
defs = dg.Definitions(
    assets=[
        futures_backfill_by_month,
        futures_continuous_backfill_by_month,
    ],
    resources={'s3': get_s3_resource()},
)
