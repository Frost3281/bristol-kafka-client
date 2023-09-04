from typing import Any

import pytest
from kafka import KafkaConsumer

from bristol_kafka_client import exceptions
from bristol_kafka_client.client import KafkaClient
from tests.conftest import Check


def _test_client(kafka_client: KafkaClient, check_data: dict[str, Any]) -> None:
    batch_size = 5
    record_batches_gen = kafka_client.consume_records(batch_size_before_insert=batch_size)
    record_batches = list(record_batches_gen)
    assert len(record_batches) > 0
    for batch in record_batches:
        assert batch == [Check(**check_data)]


def test_consume_records(
    mock_consumer: KafkaConsumer, kafka_client: KafkaClient, check_data: dict[str, Any],
):
    """Тестирование получения данных."""
    _test_client(kafka_client, check_data)


def test_consume_records_with_callable(
    kafka_client_callable: KafkaClient, check_data: dict[str, Any],
):
    _test_client(kafka_client_callable, check_data)


def test_consume_record(
    mock_consumer: KafkaConsumer, kafka_client: KafkaClient, check_data: dict[str, Any],
):
    """Тестирование получения одной записи."""
    check_list = list(kafka_client._consume_record())
    assert len(check_list) == 1
    assert check_list[0] == Check(**check_data)


def test_cant_instantiate_client_without_model_or_getter(mock_consumer):
    with pytest.raises(exceptions.SerializerNotSetError):
        KafkaClient(consumer=mock_consumer)
