from .api_base import api_call, JsonType

def base_call(function: str, api_key: str, params: dict) -> JsonType:
    return api_call()