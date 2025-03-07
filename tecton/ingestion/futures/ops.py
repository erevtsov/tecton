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
