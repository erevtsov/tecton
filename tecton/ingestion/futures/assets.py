import datetime as dt
import os

import dagster as dg
import dagster_aws.s3 as s3

from tecton.dal.mantle import Mantle
from tecton.ingestion.apitools.databento import process_definition_data, process_statistics_data
from tecton.ingestion.util import write_bytes

monthly_partitions = dg.MonthlyPartitionsDefinition(start_date='2024-02-01')
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
s3_resource = s3.S3Resource(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

stats_core_path = 's3://synqvest/databento/statistics/glbx-mdp3-'
stats_suffix = '.statistics.csv'
desc_core_path = 's3://synqvest/databento/definition/glbx-mdp3-'
desc_suffix = '.definition.csv'


@dg.asset(partitions_def=monthly_partitions)
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
        on=['ts_ref', 'instrument_id'],
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


# Define the Definitions object
defs = dg.Definitions(
    assets=[futures_backfill_by_month],
    resources={'s3': s3_resource},
)
