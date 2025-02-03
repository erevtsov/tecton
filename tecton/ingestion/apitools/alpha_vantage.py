import os
from .api_base import api_call, JsonType
import urllib.parse

ALPHA_VANTAGE_API_KEY = 'demo'  # os.environ['ALPHAVANTAGE_API_KEY']

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


def etf_profile(symbol: str) -> JsonType:
    return base_call(function='ETF_PROFILE', params={'symbol': symbol})
