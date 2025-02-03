import urllib
import json

JsonType = None | int | str | bool | list['JsonType'] | dict[str, 'JsonType']


def api_call(
    path: str,
    headers: dict,
    data: dict | None = None,
    method: str = 'POST',
) -> JsonType:
    request = urllib.request.Request(
        url=path,
        data=data and bytes(json.dumps(data), encoding='utf-8'),
        headers=headers,
        method=method,
    )

    with urllib.request.urlopen(request) as response:
        json_response_as_string = response.read().decode('utf-8')
        json_obj = json.loads(json_response_as_string)
        return json_obj
