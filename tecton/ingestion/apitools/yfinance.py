import datetime as dt

import polars as pl
import yfinance as yf

from tecton.ingestion.util import to_snake_case


def get_equity_market_data(tickers: list, start_date: dt.date, end_date: dt.date, chunk=20) -> pl.DataFrame:
    all_data = []  # List to store chunked results

    # Function to chunk list
    def chunk_list(lst, size):
        for i in range(0, len(lst), size):
            yield lst[i : i + size]

    # Iterate over ticker chunks
    for ticker_chunk in chunk_list(tickers, chunk):
        df = yf.download(ticker_chunk, start=start_date, end=end_date, group_by='ticker', auto_adjust=False)

        # Convert to Polars DataFrame and process
        pl_df = pl.from_pandas(df.stack('Ticker', future_stack=True).reset_index())

        all_data.append(pl_df)  # Append chunk result

    # Concatenate all chunked DataFrames
    px = pl.concat(all_data) if all_data else pl.DataFrame()

    px.columns = to_snake_case(px.columns)
    return px
