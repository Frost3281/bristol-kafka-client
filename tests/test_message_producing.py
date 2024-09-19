from bristol_kafka_client.message_producing import (
    _transform_models_for_kafka,  # noqa: PLC2701
)
from tests.conftest import Check


def test_transform_models():
    """Тестирование преобразования модели."""
    json_data = _transform_models_for_kafka(
        [
            Check(id=1, name='Test', data='some data', result='success'),
            Check(id=2, name='Test', data='some data', result='success'),
            Check(id=3, name='Test', data='some data', result='success'),
        ],
        dump_by_alias=True,
    )
    assert b'data_alias' in (json_data or b'')
