from typing import Any

import pytest

from bristol_kafka_client.async_client import KafkaClientAsync
from tests.conftest import Check


@pytest.mark.asyncio()
async def test_async_client(
    kafka_client_async: KafkaClientAsync,
    check_data: dict[str, Any],
) -> None:
    """Тестирование асинхронного клиента."""
    batch_size = 5
    record_batches = [
        record
        async for record in kafka_client_async.consume_records(
            batch_size_before_insert=batch_size,
        )
    ]
    assert len(record_batches) > 0
    for batch in record_batches:
        assert batch == [Check(**check_data)]
