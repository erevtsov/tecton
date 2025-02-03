import json
import os
from .api_base import api_call, JsonType
from enum import Enum
import urllib.request
import urllib.parse


OPENFIGI_API_KEY = os.environ.get('OPENFIGI_API_KEY', None)  # Put your API key here or in env var

OPENFIGI_BASE_URL = 'https://api.openfigi.com'

HEADERS = {'Content-Type': 'application/json'}
if OPENFIGI_API_KEY:
    HEADERS |= {'X-OPENFIGI-APIKEY': OPENFIGI_API_KEY}


class IdType(Enum):
    # ISIN - International Securities Identification Number.
    ID_ISIN = ('ID_ISIN',)
    # Unique Bloomberg Identifier - A legacy, internal Bloomberg identifier.
    ID_BB_UNIQUE = ('ID_BB_UNIQUE',)
    # Sedol Number - Stock Exchange Daily Official List.
    ID_SEDOL = ('ID_SEDOL',)
    # Common Code - A nine digit identification number.
    ID_COMMON = ('ID_COMMON',)
    # Wertpapierkennnummer/WKN - German securities identification code.
    ID_WERTPAPIER = ('ID_WERTPAPIER',)
    # CUSIP - Committee on Uniform Securities Identification Procedures.
    ID_CUSIP = ('ID_CUSIP',)
    # CINS - CUSIP International Numbering System.
    ID_CINS = ('ID_CINS',)
    # Common Code - A nine digit identification number.
    ID_EXCH_SYMBOL = ('ID_EXCH_SYMBOL',)
    # Full Exchange Symbol - Contains the exchange symbol for futures, options, indices inclusive of base symbol and other security elements.
    ID_FULL_EXCHANGE_SYMBOL = ('ID_FULL_EXCHANGE_SYMBOL',)
    # An indistinct identifier which may be linked to multiple instruments. May need to be combined with other values to identify a unique instrument.
    BASE_TICKER = ('BASE_TICKER',)


def mapping_call(data: tuple) -> JsonType:
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
