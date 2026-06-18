from __future__ import annotations

import copy
from typing import TypeAlias, TypeGuard

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]


def _is_object_dict(value: object) -> TypeGuard[dict[object, object]]:
    return isinstance(value, dict)


def _coerce_json_value(value: object) -> JsonValue:
    match value:
        case None | bool() | int() | float() | str():
            return value
        case list():
            return [_coerce_json_value(item) for item in value]
        case dict():
            result: JsonObject = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise ValueError("stream event object keys must be strings")
                result[key] = _coerce_json_value(item)
            return result
        case _:
            raise ValueError("stream event values must be JSON-serializable")


def validate_event(event: object) -> JsonObject:
    if not _is_object_dict(event):
        raise ValueError("stream event must be a JSON object")
    result: JsonObject = {}
    for key, item in event.items():
        if not isinstance(key, str):
            raise ValueError("stream event keys must be strings")
        result[key] = _coerce_json_value(item)
    return result


def clone_event(event: JsonObject) -> JsonObject:
    return copy.deepcopy(event)
