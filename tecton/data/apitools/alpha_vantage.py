import os
import urllib.parse

import pandas as pd

from .api_base import JsonType, api_call

ALPHA_VANTAGE_API_KEY = os.environ['ALPHAVANTAGE_API_KEY']

ALPHA_VANTAGE_BASE_URL = 'https://www.alphavantage.co'


def base_call(function: str, params: dict) -> JsonType:
    path = urllib.parse.urljoin(
        ALPHA_VANTAGE_BASE_URL,
        f'/query?function={function}&apikey={ALPHA_VANTAGE_API_KEY}&{"&".join(f"{k}={v}" for k, v in params.items())}',
    )
    return api_call(
        path=path,
        headers={},
        data=None,
        method='GET',
    )


def etf_profile(symbol: str) -> pd.DataFrame:
    res = base_call(function='ETF_PROFILE', params={'symbol': symbol})
    res = pd.DataFrame(res['holdings'])
    res['weight'] = res['weight'].astype(float)
    res['composite_symbol'] = symbol
    return res
