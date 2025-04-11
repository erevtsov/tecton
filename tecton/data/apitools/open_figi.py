import logging
import os
import urllib.parse
import urllib.request
from enum import Enum

import polars as pl

from tecton.data.util import to_snake_case

from .api_base import JsonType, api_call

OPENFIGI_API_KEY = os.environ['OPENFIGI_API_KEY']
OPENFIGI_BASE_URL = 'https://api.openfigi.com'
HEADERS = {'Content-Type': 'application/json'}
if OPENFIGI_API_KEY:
    HEADERS |= {'X-OPENFIGI-APIKEY': OPENFIGI_API_KEY}
logger = logging.getLogger(__name__)


class IdType(Enum):
    # ISIN - International Securities Identification Number.
    ID_ISIN = 'ID_ISIN'
    # Unique Bloomberg Identifier - A legacy, internal Bloomberg identifier.
    ID_BB_UNIQUE = 'ID_BB_UNIQUE'
    # Sedol Number - Stock Exchange Daily Official List.
    ID_SEDOL = 'ID_SEDOL'
    # Common Code - A nine digit identification number.
    ID_COMMON = 'ID_COMMON'
    # Wertpapierkennnummer/WKN - German securities identification code.
    ID_WERTPAPIER = 'ID_WERTPAPIER'
    # CUSIP - Committee on Uniform Securities Identification Procedures.
    ID_CUSIP = 'ID_CUSIP'
    # CINS - CUSIP International Numbering System.
    ID_CINS = 'ID_CINS'
    # Common Code - A nine digit identification number.
    ID_EXCH_SYMBOL = 'ID_EXCH_SYMBOL'
    # Full Exchange Symbol - Contains the exchange symbol for futures, options, indices inclusive of base symbol and other security elements.
    ID_FULL_EXCHANGE_SYMBOL = 'ID_FULL_EXCHANGE_SYMBOL'
    # An indistinct identifier which may be linked to multiple instruments. May need to be combined with other values to identify a unique instrument.
    BASE_TICKER = 'BASE_TICKER'


def mapping_call(data: list) -> JsonType:
    path = urllib.parse.urljoin(OPENFIGI_BASE_URL, '/v3/mapping')
    return api_call(
        path=path,
        headers=HEADERS,
        data=data,
        method='POST',
    )


def search_call(data: dict) -> JsonType:
    path = urllib.parse.urljoin(OPENFIGI_BASE_URL, '/v3/search')
    return api_call(
        path=path,
        headers=HEADERS,
        data=data,
        method='POST',
    )


def map_by_ticker(df: pl.DataFrame, chunk_size: int = 50) -> None:
    required_columns = {'symbol', 'exch_code'}
    assert required_columns.issubset(df.columns)

    # replace '-' with '/' in symbol column... that's what the openfigi api expects
    # also seems to be the standard in other places?
    df = df.with_columns(pl.col('symbol').str.replace('-', '/').alias('idValue'))
    df = df.with_columns(pl.lit('TICKER').alias('idType')).rename({'exch_code': 'exchCode'})
    frames = []
    # Chunking and calling mapping function
    for start in range(0, df.height, chunk_size):
        chunk = df.slice(start, chunk_size)
        print(start)
        res = mapping_call(chunk['idType', 'idValue', 'exchCode'].to_dicts())
        # Extract and flatten the data field
        for index, item in enumerate(res):
            if 'data' in item:  # Ensure 'data' key exists
                frames.append(item['data'][0])  # Flatten nested 'data' lists
            elif 'warning' in item:
                logger.warning(chunk.slice(index, 1))
    result = pl.DataFrame(frames)
    # join the symbol back in
    result = df['symbol', 'idValue'].join(result, left_on='idValue', right_on='ticker', how='left').drop(['idValue'])
    # snake case col names
    result.columns = to_snake_case(result.columns)
    # add "active" column
    result = result.with_columns((result['figi'].is_not_null()).alias('active'))
    return result
