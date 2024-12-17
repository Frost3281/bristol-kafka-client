from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiokafka import TopicPartition
from pydantic import BaseModel, ConfigDict, Field

from bristol_kafka_client.async_client import KafkaClientAsync
from bristol_kafka_client.client import KafkaClient


class Check(BaseModel):
    """Модель чека для тестов."""

    id: int
    name: str
    data: Annotated[Optional[str], Field(alias='data_alias')]
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    result: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)


@pytest.fixture(name='check_data')
def fixture_check_data():
    """Данные для тестирования."""
    return {'id': 1, 'name': 'Test', 'data': 'some data'}


@dataclass
class MockReturnValue:
    """Мок для возвращаемого значения консьюмера."""

    value: list[dict[str, str]]
    offset: int = 1


@pytest.fixture()
def mock_consumer(check_data):
    """Консьюмер."""
    consumer = MagicMock()
    consumer.__iter__.return_value = [MockReturnValue([check_data])]
    return consumer


@pytest.fixture()
def mock_async_consumer(check_data):
    """Консьюмер."""
    consumer = AsyncMock()
    consumer.getmany.return_value = {
        TopicPartition('check', 0): [MockReturnValue([check_data])],
    }
    return consumer


@pytest.fixture()
def kafka_client(mock_consumer):
    """Клиент Кафка."""
    return KafkaClient(
        consumer=mock_consumer,
        model=Check,
    )


@pytest.fixture()
def kafka_client_async(mock_async_consumer):
    """Клиент Кафка."""
    return KafkaClientAsync(
        consumer=mock_async_consumer,
        model=Check,
    )


@pytest.fixture()
def kafka_client_callable_async(mock_async_consumer):
    """Клиент Кафка с model_getter вместо model."""

    def _serialize_mock(message) -> Check:
        return Check(**message)

    return KafkaClientAsync(
        consumer=mock_async_consumer,
        model_getter=_serialize_mock,
    )


@pytest.fixture()
def kafka_client_callable(mock_consumer):
    """Клиент Кафка с model_getter вместо model."""

    def _serialize_mock(message) -> Check:
        return Check(**message)

    return KafkaClient(
        consumer=mock_consumer,
        model_getter=_serialize_mock,
    )
