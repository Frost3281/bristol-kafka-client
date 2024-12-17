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
    batch_size = 1
    record_batches = (
        record
        async for record in kafka_client_async.consume_records(
            batch_size_before_insert=batch_size,
        )
    )
    batch = await anext(record_batches)
    assert batch is not None
    assert batch == [Check(**check_data)]
