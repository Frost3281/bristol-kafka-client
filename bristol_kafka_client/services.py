from typing import Any, Union


def to_list_if_dict(data: Union[list[Any], dict[str, Any]]) -> list[Any]:
    """Если данные являются словарем, то превращаем их в список."""
    return data if isinstance(data, list) else [data]
