from contextlib import aclosing
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from aiokafka import TopicPartition

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


@pytest.mark.asyncio()
@pytest.mark.parametrize('written', [True, False])
async def test_commit_covers_only_whole_polls_written_to_database(written):
    """Пачка для БД включает весь второй poll, commit следует за записью."""
    partition = TopicPartition('events', 0)
    consumer = AsyncMock()
    consumer.getmany.side_effect = [
        {partition: [_record(0), _record(1)]},
        {partition: [_record(2), _record(3)]},
        {partition: [_record(4), _record(5)]},
    ]
    client = KafkaClientAsync(consumer=consumer, model_getter=_get_check)

    async with aclosing(client.consume_records(batch_size_before_insert=2)) as batches:
        batch = await anext(batches)
        assert [check.id for check in batch] == [1, 2, 3]
        consumer.commit.assert_not_awaited()
        if written:
            assert [check.id for check in await anext(batches)] == [4, 5]

    if written:
        consumer.commit.assert_awaited_once_with({partition: 4})
    else:
        consumer.commit.assert_not_awaited()


@pytest.mark.asyncio()
async def test_idle_exit_flushes_remaining_batch_and_commits_filtered_offsets():
    """Конечное чтение сохраняет неполную пачку и позицию отфильтрованного события."""
    filtered = TopicPartition('events', 0)
    accepted = TopicPartition('events', 1)
    consumer = AsyncMock()
    consumer.getmany.side_effect = [{filtered: [_record(0)], accepted: [_record(1)]}, {}]
    client = KafkaClientAsync(consumer=consumer, model_getter=_get_check)

    batches = [batch async for batch in client.consume_records(10, idle_timeout_ms=1)]

    assert [[check.id for check in batch] for batch in batches] == [[1]]
    consumer.commit.assert_awaited_once_with({filtered: 1, accepted: 2})


@pytest.mark.asyncio()
async def test_run_deadline_flushes_the_last_poll(monkeypatch):
    """Лимит запуска сохраняет данные последнего poll из нескольких по одному offset."""
    partition = TopicPartition('events', 0)
    consumer = AsyncMock()
    record = SimpleNamespace(value=[_record(1).value, _record(2).value], offset=0)
    consumer.getmany.return_value = {partition: [record]}
    client = KafkaClientAsync(consumer=consumer, model_getter=_get_check)
    ticks = iter([0.0, 0.0, 0.5, 1.0])
    monkeypatch.setattr(
        'bristol_kafka_client.async_client.perf_counter',
        lambda: next(ticks),
    )

    batches = [batch async for batch in client.consume_records(10, max_run_seconds=1)]

    assert [[check.id for check in batch] for batch in batches] == [[1, 2]]
    consumer.getmany.assert_awaited_once_with(timeout_ms=1000, max_records=10)
    consumer.commit.assert_awaited_once_with({partition: 1})


def _record(offset: int) -> SimpleNamespace:
    return SimpleNamespace(
        value={'id': offset, 'name': 'Check', 'data': None},
        offset=offset,
    )


def _get_check(message: dict[str, Any]) -> Check | None:
    return Check.model_validate(message) if message['id'] else None
