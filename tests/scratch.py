import datetime as dt

import pandas as pd  # noqa
import polars as pl  # noqa

from tecton.dal.mantle import Mantle
from tecton.data.apitools.databento import StatType  # noqa

stats_path = 's3://synqvest/databento/statistics/glbx-mdp3-20120201-20120229.statistics.csv'
desc_path = 's3://synqvest/databento/definition/glbx-mdp3-20120201-20120229.definition.csv'

m = Mantle()

"""
Understand relevant columns in the description file

ts_recv = close date
asset = futures root
raw_symbol = ticker
instrument_id = unique id to use for joins
activation
expiration

unit_of_measure_qty = contrat size
unit_of_measure = quote units

min_price_increment = tick size
min_price_increment_amount = tick value
point_value = min_price_increment_amount / min_price_increment
contract_value = price * point_value

display_factor = "unit"

maturity_year
maturity_month
"""

desc_cols = [
    'ts_recv',
    'asset',
    'group',
    'exchange',
    'security_type',
    'cfi',
    'raw_symbol',
    'instrument_id',
    'activation',
    'expiration',
    'unit_of_measure_qty',
    'unit_of_measure',
    'min_price_increment',
    'min_price_increment_amount',
    'display_factor',
    'settl_price_type',
]
desc = m.get_files(desc_path).to_polars()
symbols = desc['asset'].unique() + 'H5'
desc_slice = desc.filter(pl.col('ts_recv').is_in(dt.datetime(2024, 12, 2)) & pl.col('symbol').is_in(symbols))
desc_slice = desc_slice.to_pandas()

"""
Understand which stat types beed to pull
If more than one observation per day, de-dupe logic for each
"""


stats = m.get_files(stats_path)
stats_slice = stats.filter((stats.symbol == 'CLZ2') & (stats.stat_type == StatType.settlement_price.value))
stats_slice = stats_slice.to_pandas()
