import datetime as dt
from enum import Enum

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


def fix_prices(
    df: pl.DataFrame,
    price_cols: list,
    factor: float = 100,
    threshold: float = 0.9,
    window: int = 8,
    max_iterations: int = 10,
) -> pl.DataFrame:
    """
    Fixes erroneous prices by comparing to previous median values.
    Performs multiple passes to handle consecutive price errors.
    """
    result = df.clone()
    prev_result = None

    for iteration in range(max_iterations):
        prev_result = result.clone()

        result = (
            result.sort(['instrument_id', 'ts_ref'])
            .group_by('instrument_id')
            .map_groups(
                lambda group: (
                    group.with_columns(
                        [
                            pl.col(col_name)
                            .rolling_median(
                                window_size=window,
                                center=False,
                                min_periods=2,
                            )
                            .alias(f'{col_name}_med')
                            for col_name in price_cols
                        ]
                    )
                    .with_columns(
                        [
                            pl.when(
                                (pl.col(f'{col_name}_med').is_not_null())
                                & ((pl.col(col_name) / pl.col(f'{col_name}_med') - 1).abs() > threshold)
                            )
                            .then(
                                pl.when(pl.col(col_name) > pl.col(f'{col_name}_med'))
                                .then(pl.col(col_name) / factor)
                                .otherwise(pl.col(col_name) * factor)
                            )
                            .otherwise(pl.col(col_name))
                            .alias(col_name)
                            for col_name in price_cols
                        ]
                    )
                    .drop([f'{col_name}_med' for col_name in price_cols])
                )
            )
        )

        # Check if any prices changed in this iteration
        if iteration > 0:
            # Compare current result with previous iteration
            changes = False
            for col in price_cols:
                if not (result.get_column(col) == prev_result.get_column(col)).all():
                    changes = True
                    break

            if not changes:
                break

    return result


def process_statistics_data(data: pl.DataFrame) -> pl.DataFrame:
    """
    Process statistics data for futures assets.
    """
    col_renamings = {'ts_ref': 'date'}

    output = data.lazy()
    #
    # .str.to_datetime('%Y-%m-%dT%H:%M:%S%.fZ')
    if output.schema.get('ts_ref') == pl.Utf8:
        output = output.with_columns(pl.col('ts_ref').str.to_datetime('%Y-%m-%dT%H:%M:%S%.fZ'))
    # ts_ref can be null in some places, use ts_event in those scenarios
    output = output.with_columns(
        pl.when(pl.col('ts_ref').is_null())
        .then(pl.col('ts_event').dt.truncate('1d'))
        .otherwise(pl.col('ts_ref'))
        .alias('ts_ref')
    ).with_columns(pl.col('ts_ref').cast(pl.Date))
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
    quantities = (
        quantities.pivot(
            values='quantity',
            index=key,
            on='stat_name',
            aggregate_function='first',
        )
        .drop_nulls(subset=key)
        .sort(key)
    )
    prices = (
        prices.pivot(
            values='price',
            index=key,
            on='stat_name',
            aggregate_function='first',
        )
        .drop_nulls(subset=key)
        .sort(key)
    )
    # only fix prices during known month for now...
    if (prices['ts_ref'] == dt.date(2012, 2, 6)).sum() > 0:
        prices = fix_prices(
            df=prices,
            price_cols=list(set(prices.columns) - set(key)),
        )
    output = quantities.join(prices, on=key, how='full', validate='1:1', coalesce=True)
    output = output.rename(col_renamings)
    return output
