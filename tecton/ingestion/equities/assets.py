import datetime as dt
import os
from collections.abc import Sequence

import dagster as dg
import dagster_aws.s3 as s3
import polars as pl
import yaml

from tecton.dal.mantle import Mantle
from tecton.ingestion.apitools.alpha_vantage import etf_profile
from tecton.ingestion.apitools.open_figi import map_by_ticker
from tecton.ingestion.apitools.yfinance import get_equity_market_data
from tecton.ingestion.util import write_bytes

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
# static date so we can easily overwrite files
# DATE = (dt.date.today() - pd.tseries.offsets.BDay(1)).date()
DATE = dt.date(2025, 2, 6)
CONFIG_FILE_PATH = dg.file_relative_path(__file__, 'etl_config.yaml')

s3_resource = s3.S3Resource(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def build_etf_weights(symbol: str, exch_code: str) -> dg.Definitions:
    @dg.asset(name=f'etf_weights_{symbol}')
    def etl_table(s3: s3.S3Resource):
        #
        wts = etf_profile(symbol=symbol)
        # etf_profile call doesn't know anything about the date (it's not point in time)
        wts['date'] = DATE
        # i dont know the right place to make this assumption
        wts['exch_code'] = exch_code
        del wts['description']
        #
        buffer = write_bytes(wts)
        #
        s3_client = s3.get_client()
        s3_client.put_object(
            Bucket=os.environ['S3_BUCKET'],
            Key=f'equity/etf_weights/{symbol}_{DATE:%Y%m%d}.parquet',
            Body=buffer,
        )

    return etl_table


def load_etf_tables_from_yaml(yaml_path: str) -> Sequence[dg.AssetsDefinition]:
    config = yaml.safe_load(open(yaml_path))
    factory_assets = [
        build_etf_weights(
            symbol=etf['symbol'],
            exch_code=etf['exch_code'],
        )
        for etf in config['equities']['etf_universe']
    ]
    return factory_assets


etf_holdings = load_etf_tables_from_yaml(CONFIG_FILE_PATH)


@dg.asset(name='equity_universe', deps=[etl_table.key for etl_table in etf_holdings] if etf_holdings else [])
def equity_universe(s3: s3.S3Resource):
    # load the ETF Config
    etfs = pl.DataFrame(yaml.safe_load(open(CONFIG_FILE_PATH))['equities']['etf_universe'])
    # get all the etf_weights given a date
    s = Mantle()
    table = s.select('etf_weights', start_date=DATE, end_date=DATE).to_polars()
    # list of equities to map
    # set up
    equities = pl.concat([table.unique(subset=['symbol', 'exch_code'])['symbol', 'exch_code'], etfs])
    xmap = map_by_ticker(equities)
    xmap = xmap.with_columns(pl.lit(DATE).alias('date'))
    buffer = write_bytes(xmap)
    #
    s3_client = s3.get_client()
    s3_client.put_object(
        Bucket=os.environ['S3_BUCKET'],
        Key=f'equity/universe/{DATE:%Y%m%d}.parquet',
        Body=buffer,
    )


@dg.asset(name='equity_prices', deps=[equity_universe.key])
def equity_prices(s3: s3.S3Resource):
    # get the equity universe
    s = Mantle()
    table = s.select('equity_universe', start_date=DATE, end_date=DATE)
    # active only (should i throw these out earlier?)
    table = table.filter(table.active).to_polars()
    # get the market data
    px = get_equity_market_data(tickers=table['symbol'].to_list(), start_date=dt.date(2024, 1, 1), end_date=DATE)

    buffer = write_bytes(px)
    #
    s3_client = s3.get_client()
    s3_client.put_object(
        Bucket=os.environ['S3_BUCKET'],
        Key=f'equity/prices/{DATE:%Y%m%d}.parquet',
        Body=buffer,
    )


defs = dg.Definitions(
    assets=[*etf_holdings, equity_universe, equity_prices],
    resources={'s3': s3_resource},
)
