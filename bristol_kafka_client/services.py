from typing import Any, Union


def to_list_if_dict(data: Union[list[Any], dict[str, Any]]):
    return data if isinstance(data, list) else [data]
