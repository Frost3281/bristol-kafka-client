from bristol_kafka_client.utils import flatten


def test_flatten_list() -> None:
    """Тестирование flatten для списка списков."""
    assert flatten([[1, 2], [3, 4], [5, 6]]) == [1, 2, 3, 4, 5, 6]


def test_flatten_dict_values() -> None:
    """Тестирование flatten для значений словарей."""
    assert flatten({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]}.values()) == [1, 2, 3, 4, 5, 6]
