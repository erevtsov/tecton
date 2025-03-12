from functools import reduce

import polars as pl


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
    # Select all required columns
    df = data.select(
        [
            'date',
            'symbol',
            'asset',
            'settlement_price',
            'open_interest',
            'cleared_volume',
            'opening_price',
            'trading_session_low_price',
            'trading_session_high_price',
            'lowest_offer',
            'highest_bid',
        ]
    ).lazy()

    # Ensure data is sorted
    df = df.sort(['asset', 'date', 'symbol'])

    # Identify front and next contracts based on open interest per asset and date
    df = df.with_columns(
        [pl.col('open_interest').rank('ordinal', descending=True).over(['date', 'asset']).alias('oi_rank')]
    )

    # Front contract: OI rank 1
    front_contract = df.filter(pl.col('oi_rank') == 1).rename(
        {
            'symbol': 'front_symbol',
            'settlement_price': 'front_price',
            'open_interest': 'front_open_interest',
            'cleared_volume': 'front_volume',
            'opening_price': 'front_opening',
            'trading_session_low_price': 'front_low',
            'trading_session_high_price': 'front_high',
            'lowest_offer': 'front_offer',
            'highest_bid': 'front_bid',
        }
    )

    # Next contract: OI rank 2
    next_contract = df.filter(pl.col('oi_rank') == 2).rename(
        {
            'symbol': 'next_symbol',
            'settlement_price': 'next_price',
            'open_interest': 'next_open_interest',
            'cleared_volume': 'next_volume',
            'opening_price': 'next_opening',
            'trading_session_low_price': 'next_low',
            'trading_session_high_price': 'next_high',
            'lowest_offer': 'next_offer',
            'highest_bid': 'next_bid',
        }
    )

    # Merge front and next contract data
    merged = front_contract.join(next_contract, on=['date', 'asset'], how='inner').sort(['asset', 'date'])

    # Identify roll dates (when OI of front drops below next)
    merged = merged.with_columns(
        [(pl.col('front_symbol') != pl.col('front_symbol').shift(-1)).over(['asset']).alias('roll_flag')]
    )

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

    # Determine the active contract details with blending
    blended = merged.with_columns(
        [
            # Existing price and symbol logic
            pl.when(pl.col('in_roll_window'))
            .then(pl.col('next_price') * 0.5 + pl.col('front_price') * 0.5)
            .otherwise(pl.col('front_price'))
            .alias('price'),
            pl.when(pl.col('in_roll_window'))
            .then(pl.col('next_symbol'))
            .otherwise(pl.col('front_symbol'))
            .alias('symbol'),
            # New columns with blending
            pl.when(pl.col('in_roll_window'))
            .then(pl.col('next_volume'))
            .otherwise(pl.col('front_volume'))
            .alias('cleared_volume'),
            pl.when(pl.col('in_roll_window'))
            .then(pl.col('next_opening'))
            .otherwise(pl.col('front_opening'))
            .alias('opening_price'),
            pl.when(pl.col('in_roll_window'))
            .then(pl.col('next_low'))
            .otherwise(pl.col('front_low'))
            .alias('trading_session_low_price'),
            pl.when(pl.col('in_roll_window'))
            .then(pl.col('next_high'))
            .otherwise(pl.col('front_high'))
            .alias('trading_session_high_price'),
            pl.when(pl.col('in_roll_window'))
            .then(pl.col('next_offer'))
            .otherwise(pl.col('front_offer'))
            .alias('lowest_offer'),
            pl.when(pl.col('in_roll_window'))
            .then(pl.col('next_bid'))
            .otherwise(pl.col('front_bid'))
            .alias('highest_bid'),
            pl.when(pl.col('in_roll_window'))
            .then(pl.col('next_open_interest'))
            .otherwise(pl.col('front_open_interest'))
            .alias('open_interest'),
        ]
    )

    # Select all columns for output
    continuous_df = blended.select(
        [
            'date',
            'asset',
            'symbol',
            'price',
            'open_interest',
            'cleared_volume',
            'opening_price',
            'trading_session_low_price',
            'trading_session_high_price',
            'lowest_offer',
            'highest_bid',
        ]
    )

    return continuous_df.collect()
    return continuous_df.collect()
