from typing import Any, Iterable, TypeVar

T = TypeVar('T')


def filter_not_none(value: Iterable[T | None]) -> list[T]:
    """Фильтрует None из итерируемого объекта."""
    return [i for i in value if i is not None]


def flatten(data: Iterable[Iterable[Any]]) -> list[Any]:
    """Преобразование списка списков в список."""
    return [item for sublist in data for item in sublist]
