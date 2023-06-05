from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from bristol_kafka_client.client import KafkaClient


class Check(BaseModel):
    """Модель чека для тестов."""

    id: int
    name: str
    data: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    result: Optional[str] = None


@pytest.fixture(name='check_data')
def fixture_check_data():
    """Данные для тестирования."""
    return {
        'id': 1,
        'name': 'Test',
        'data': 'some data'
    }


@dataclass
class MockReturnValue:
    """Мок для возвращаемого значения консьюмера."""

    value: list[dict[str, str]]
    offset: int
    partition: int
    topic: str


@pytest.fixture
def mock_consumer(check_data):
    """Консьюмер."""
    consumer = MagicMock()
    consumer.__iter__.return_value = [MockReturnValue([check_data], 1, 1, 'test_topic')]
    return consumer


@pytest.fixture
def kafka_client(mock_consumer):
    """Клиент Кафка."""
    return KafkaClient(
        consumer=mock_consumer,
        model=Check,
        _is_commit_only_manually=False
    )
