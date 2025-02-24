from enum import Enum
from functools import reduce

import polars as pl


class StatType(Enum):
    opening_price = 1  # The price and quantity of the first trade of an instrument.
    indicative_opening_price = 2  # The probable price of the first trade of an instrument published during pre-open.
    settlement_price = 3  # The settlement price of an instrument. Flags will indicate whether the price is final or preliminary and actual or theoretical.
    trading_session_low_price = 4  # The lowest trade price of an instrument during the trading session.
    trading_session_high_price = 5  # The highest trade price of an instrument during the trading session.
    cleared_volume = 6  # The number of contracts cleared for an instrument on the previous trading date.
    lowest_offer = 7  # The lowest offer price for an instrument during the trading session.
    highest_bid = 8  # The highest bid price for an instrument during the trading session.
    open_interest = 9  # The current number of outstanding contracts of an instrument.
    fixing_price = 10  # The volume-weighted average price (VWAP) for a fixing period.


def process_definition_data(data: pl.DataFrame) -> pl.DataFrame:
    """
    This is expecting a data frame of "raw" definition data (i.e. from databento files or API)

    Notes on relevant columns:
        ts_recv = close date
        asset = futures root
        instrument_class
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
    desc_cols = (
        'ts_ref',
        'asset',
        'group',
        'exchange',
        'security_type',
        'currency',
        'settl_currency',
        'cfi',
        'raw_symbol',
        'instrument_id',
        'activation',
        'expiration',
        'unit_of_measure_qty',
        'unit_of_measure',
        'min_price_increment',
        'min_price_increment_amount',
        'point_value',
        'display_factor',
        'settl_price_type',
    )
    col_renamings = {
        'unit_of_measure_qty': 'contract_size',
        'unit_of_measure': 'quote_units',
        'min_price_increment': 'tick_size',
        'min_price_increment_amount': 'tick_value',
        'ts_ref': 'date',
        'raw_symbol': 'symbol',
    }
    # convert data to lazy
    output = data.lazy()
    # column dtype changes

    output = output.with_columns(pl.col('ts_recv').cast(pl.Date).alias('ts_ref'))
    # Sort the DataFrame by 'ts_recv' in descending order, then drop duplicates based on group columns.
    # keeping first since that's the latest entry
    output = output.sort('ts_recv', descending=True).unique(
        subset=['instrument_id', 'ts_ref'],
        keep='first',
    )
    # apply filters
    # S=spread, F=futures; keep only futures
    filters = [pl.col('instrument_class') == 'F']
    output = output.filter(filters)
    # derive columns
    output = output.with_columns(
        (pl.col('min_price_increment_amount') / pl.col('min_price_increment')).alias('point_value')
    )
    # select relevant columns
    output = output.select(desc_cols)
    # rename columns and return
    output = output.collect().rename(col_renamings)
    return output


def process_statistics_data(data: pl.DataFrame) -> pl.DataFrame:
    """
    Process statistics data for futures assets.
    """
    col_renamings = {'ts_ref': 'date'}

    output = data.lazy()
    #
    # .str.to_datetime('%Y-%m-%dT%H:%M:%S%.fZ')
    if output.schema.get('ts_ref') == pl.Utf8:
        output = output.with_columns(pl.col('ts_ref').str.to_datetime('%Y-%m-%dT%H:%M:%S%.fZ').cast(pl.Date))
    else:
        output = output.with_columns(pl.col('ts_ref').cast(pl.Date))
    # Sort the DataFrame by 'ts_event' in descending order, then drop duplicates based on group columns.
    output = output.sort('ts_event', descending=True).unique(
        subset=['instrument_id', 'ts_ref', 'stat_type'],
        keep='first',
    )

    mapping = {member.value: member.name for member in StatType}
    # Map the 'stat_type' column to enum string using the dict
    output = output.with_columns(
        pl.col('stat_type').map_elements(lambda x: mapping.get(x, 'Unknown'), return_dtype=pl.String).alias('stat_name')
    )
    # split the frame into quantities and prices
    price_stats = [
        StatType.settlement_price.value,
        StatType.opening_price.value,
        StatType.highest_bid.value,
        StatType.trading_session_low_price.value,
        StatType.trading_session_high_price.value,
        StatType.lowest_offer.value,
        StatType.fixing_price.value,
    ]
    quantity_stats = [
        StatType.cleared_volume.value,
        StatType.open_interest.value,
    ]
    prices = output.filter(pl.col('stat_type').is_in(price_stats)).collect()
    quantities = output.filter(pl.col('stat_type').is_in(quantity_stats)).collect()
    # quantities = output.filter((pl.col('quantity') < pl.Int32.max()) & (pl.col('quantity').is_not_null())).collect()
    # prices = output.filter((pl.col('price').is_not_null()) & (pl.col('price') < pl.Int64.max())).collect()
    # pivot quantities and prices and join
    key = ['instrument_id', 'ts_ref']
    quantities = quantities.pivot(
        values='quantity',
        index=key,
        on='stat_name',
        aggregate_function='first',
    ).drop_nulls(subset=key)
    prices = prices.pivot(
        values='price',
        index=key,
        on='stat_name',
        aggregate_function='first',
    ).drop_nulls(subset=key)
    output = quantities.join(prices, on=key, how='full', validate='1:1', coalesce=True)
    output = output.rename(col_renamings)
    return output


def construct_continuous_ticker(data: pl.DataFrame, blend_window: int = 5) -> pl.DataFrame:
    """
    Constructs a continuous futures ticker using an open interest-based roll with a
    customizable blending window (default: 5 days, meaning ±2 days around the roll).

    Args:
        data: A LazyFrame with columns ['date', 'symbol', 'asset', 'settlement_price', 'open_interest', 'volume'].
        blend_window: The total number of days for the roll transition (default: 5).

    Returns:
        pl.LazyFrame: A continuous price series with contract symbols and open interest.
    """
    df = data.select(['date', 'symbol', 'asset', 'settlement_price', 'open_interest']).lazy()

    # Ensure data is sorted
    df = df.sort(['asset', 'date', 'symbol'])

    # Identify front and next contracts based on open interest per asset and date
    df = df.with_columns(
        [pl.col('open_interest').rank('ordinal', descending=True).over(['date', 'asset']).alias('oi_rank')]
    )

    # Front contract: OI rank 1, Next contract: OI rank 2
    front_contract = df.filter(pl.col('oi_rank') == 1).rename(
        {'symbol': 'front_symbol', 'settlement_price': 'front_price', 'open_interest': 'front_open_interest'}
    )

    next_contract = df.filter(pl.col('oi_rank') == 2).rename(
        {'symbol': 'next_symbol', 'settlement_price': 'next_price', 'open_interest': 'next_open_interest'}
    )

    # Merge front and next contract data
    merged = front_contract.join(next_contract, on=['date', 'asset'], how='inner').sort(['asset', 'date'])

    # Identify roll dates (when OI of front drops below next)
    merged = merged.with_columns([(pl.col('front_symbol') != pl.col('front_symbol').shift(-1)).alias('roll_flag')])

    # Dynamically expand roll dates for blending window (half before, half after)
    half_window = (blend_window - 1) // 2  # Ensures equal spread around the roll date

    roll_shift_cols = [
        pl.col('roll_flag').shift(i).fill_null(False).alias(f'roll_shift_{i}')
        for i in range(-half_window, half_window + 1)
    ]
    merged = merged.with_columns(roll_shift_cols)

    # Compute roll mask using `reduce`
    roll_mask_exprs = [pl.col(f'roll_shift_{i}') for i in range(-half_window, half_window + 1)]
    roll_mask = reduce(lambda a, b: a | b, roll_mask_exprs).alias('in_roll_window')

    # Add roll mask back into the main dataframe
    merged = merged.with_columns(roll_mask)

    # Determine the active contract details
    blended = merged.with_columns(
        [
            pl.when(pl.col('in_roll_window'))
            .then(pl.col('next_price') * 0.5 + pl.col('front_price') * 0.5)
            .otherwise(pl.col('front_price'))
            .alias('price'),
            pl.when(pl.col('in_roll_window'))
            .then(pl.col('next_symbol'))
            .otherwise(pl.col('front_symbol'))
            .alias('symbol'),
            pl.when(pl.col('in_roll_window'))
            .then(pl.col('next_open_interest'))
            .otherwise(pl.col('front_open_interest'))
            .alias('open_interest'),
        ]
    )
    # Select necessary columns for output
    continuous_df = blended.select(['date', 'asset', 'price', 'symbol', 'open_interest'])

    return continuous_df.collect()
