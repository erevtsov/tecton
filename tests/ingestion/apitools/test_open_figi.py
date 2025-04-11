import json
from unittest import skip

from tecton.data.apitools.open_figi import mapping_call, search_call


@skip('Skipping - expired API key')
def test_search_call():
    search_request = {'query': 'APPLE'}
    print('Making a search request:', search_request)
    search_response = search_call(data=search_request)
    print('Search response:', json.dumps(search_response, indent=2))


@skip('Skipping - expired API key')
def test_mapping_call():
    mapping_request = [
        {'idType': 'ID_BB_GLOBAL', 'idValue': 'BBG000BLNNH6', 'exchCode': 'US'},
    ]
    print('Making a mapping request:', mapping_request)
    mapping_response = mapping_call(data=mapping_request)
    print('Mapping response:', json.dumps(mapping_response, indent=2))
