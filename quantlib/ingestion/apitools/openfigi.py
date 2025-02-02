import json
import os
import urllib.parse
import urllib.request
from enum import Enum

JsonType = None | int | str | bool | list['JsonType'] | dict[str, 'JsonType']


OPENFIGI_API_KEY = os.environ.get('OPENFIGI_API_KEY', None)  # Put your API key here or in env var

OPENFIGI_BASE_URL = 'https://api.openfigi.com'


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


def api_call(
    path: str,
    data: dict | None = None,
    method: str = 'POST',
) -> JsonType:
    """
    Make an api call to `api.openfigi.com`.
    Uses builtin `urllib` library, end users may prefer to
    swap out this function with another library of their choice

    Args:
        path (str): API endpoint, for example "search"
        method (str, optional): HTTP request method. Defaults to "POST".
        data (dict | None, optional): HTTP request data. Defaults to None.

    Returns:
        JsonType: Response of the api call parsed as a JSON object
    """

    headers = {'Content-Type': 'application/json'}
    if OPENFIGI_API_KEY:
        headers |= {'X-OPENFIGI-APIKEY': OPENFIGI_API_KEY}

    request = urllib.request.Request(
        url=urllib.parse.urljoin(OPENFIGI_BASE_URL, path),
        data=data and bytes(json.dumps(data), encoding='utf-8'),
        headers=headers,
        method=method,
    )

    with urllib.request.urlopen(request) as response:
        json_response_as_string = response.read().decode('utf-8')
        json_obj = json.loads(json_response_as_string)
        return json_obj


def mapping_call(data: tuple) -> JsonType:
    return api_call(
        path='/v3/mapping',
        data=data,
        method='POST',
    )


def search_call(data: dict) -> JsonType:
    return api_call(
        path='/v3/search',
        data=data,
        method='POST',
    )


def main():
    """
    Make search and mapping API requests and print the results
    to the console

    Returns:
        None
    """
    search_request = {'query': 'APPLE'}
    print('Making a search request:', search_request)
    search_response = api_call('/v3/search', search_request)
    print('Search response:', json.dumps(search_response, indent=2))

    mapping_request = [
        {'idType': 'ID_BB_GLOBAL', 'idValue': 'BBG000BLNNH6', 'exchCode': 'US'},
    ]
    print('Making a mapping request:', mapping_request)
    mapping_response = api_call('/v3/mapping', mapping_request)
    print('Mapping response:', json.dumps(mapping_response, indent=2))


if __name__ == '__main__':
    main()
