from typing import Iterable, TypeVar

T = TypeVar('T')


def filter_not_none(value: Iterable[T | None]) -> list[T]:
    """Фильтрует None из итерируемого объекта."""
    return [i for i in value if i is not None]
