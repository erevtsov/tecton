import calendar
import datetime as dt
import os

import dagster as dg
import polars as pl

from tecton.core.const import StorageBackend
from tecton.core.util import load_reference
from tecton.dal.mantle import Mantle
from tecton.data.apitools.writer import ParquetWriterFactory
from tecton.data.futures.assets import futures_continuous_data
from tecton.models.definition import TrendModelDefinition

STORAGE_BACKEND = StorageBackend[os.environ['STORAGE_BACKEND'].upper()]
monthly_partitions = dg.MonthlyPartitionsDefinition(start_date='2010-06-01', end_offset=1)
model_partitions = dg.StaticPartitionsDefinition(['trend_v1'])
month_model_partitions = dg.MultiPartitionsDefinition(
    {
        'date': monthly_partitions,
        'model_run': model_partitions,
    }
)


@dg.asset(
    partitions_def=month_model_partitions,
    group_name='trend',
    deps=[futures_continuous_data],
)
def factors(context: dg.AssetExecutionContext) -> None:
    date, model_code = context.partition_key.split('|')
    m = Mantle()
    date = dt.datetime.strptime(date, '%Y-%m-%d')
    year_month = dt.datetime.strftime(date, '%Y%m')
    model = TrendModelDefinition(code=model_code)
    # get continuous futures data for the month
    data = m.select(
        m.Tables.futures.cont,
        start_date=date.replace(day=1).date(),
        end_date=date.replace(day=calendar.monthrange(date.year, date.month)[1]).date(),
    ).to_polars()
    data = data.rename(
        {
            'price': 'close',
            'opening_price': 'open',
            'trading_session_low_price': 'low',
            'trading_session_high_price': 'high',
        }
    )

    # output columns: date, asset, model_code, factor_code, value, weight
    signals = pl.DataFrame()
    # this double loop is silly, should be able to compute the signals for entire universe
    # but whatever - the universe is small and this is simple and fast enough
    for asset in data.select('asset').unique().to_series():
        for name, config in model.factors.items():
            implementation = config['implementation']
            weight = config['weight']
            ref = load_reference(**implementation)

            arg_names = list(ref.__code__.co_varnames[: ref.__code__.co_argcount])
            arg_data = {}
            for arg in arg_names:
                if arg in ['open', 'high', 'low', 'close']:
                    arg_data[arg] = data.filter(pl.col('asset') == asset)[arg].to_numpy()
            for suffix, params in config['params'].items():
                signal = ref(**arg_data, **params)
                # add columns to signals result
                signal = pl.DataFrame(
                    {
                        'model_code': model_code,
                        'date': date,
                        'asset': asset,
                        'factor_code': name + suffix,
                        'value': signal,
                        'weight': weight,
                    }
                )
                signals = signals.vstack(signal)
    # write results
    writer = ParquetWriterFactory.create(storage_backend=STORAGE_BACKEND)
    # TODO: need to be able to partition by model code, run id
    #    do we have multiple files or one?
    writer.write(
        key=f'factors/{model_code}-{year_month}/',
        data=signals,
    )


@dg.asset(
    partitions_def=monthly_partitions,
    group_name='trend',
    # deps=[factors],
)
def forecast(context: dg.AssetExecutionContext, model: TrendModelDefinition) -> None:
    pass


"""
Assets of the trend model
- calculate signals
    - monthly partitioning
calculate the forecast
    - support different aggregation methods (i guess that's just a new model?)
    - monthly partitioning
"""
