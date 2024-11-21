import pytest

from bristol_kafka_client.message_producing import (
    _to_kafka_bytes,  # noqa: PLC2701
)
from tests.conftest import Check


@pytest.mark.parametrize(
    'exclude_none',
    [False, True],
)
def test_transform_models(*, exclude_none: bool):
    """Тестирование преобразования моделей."""
    json_data = _to_kafka_bytes(
        [
            Check(id=1, name='Test', data='some data', result='success'),
            Check(id=2, name='Test', data='some data', result='success'),
            Check(id=3, name='Test', data='some data', result='success'),
        ],
        dump_by_alias=True,
        exclude_none=exclude_none,
    )
    assert b'data_alias' in (json_data or b'')
    assert b'result' in (json_data or b'')
    if not exclude_none:
        assert b'end_time' in (json_data or b'')


def test_transform_model():
    """Тестирование преобразования модели."""
    json_data = _to_kafka_bytes(
        Check(id=1, name='Test', data='some data', result='success'),
        dump_by_alias=False,
    )
    assert b'data' in (json_data or b'')


def test_transform_dict():
    """Тестирование преобразования словаря."""
    json_data = _to_kafka_bytes(
        {'id': 1, 'name': 'Test', 'data': 'some data', 'result': 'success'},
    )
    assert b'data' in (json_data or b'')


def test_transform_list():
    """Тестирование преобразования списка."""
    json_data = _to_kafka_bytes(
        [{'id': 1, 'name': 'Test', 'data': 'some data', 'result': 'success'}],
    )
    assert b'data' in (json_data or b'')
