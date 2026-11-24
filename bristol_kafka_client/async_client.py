from __future__ import annotations

import asyncio
from dataclasses import dataclass
from time import perf_counter
from typing import Any, AsyncGenerator

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition

from ._base import BaseKafkaClient
from .services import to_list_if_dict
from .types import T_BaseModel
from .utils import filter_not_none, flatten

T_DictAny = dict[str, Any]
_PollRecords = dict[TopicPartition, list[ConsumerRecord]]


@dataclass
class KafkaClientAsync(BaseKafkaClient[T_BaseModel, AIOKafkaConsumer]):
    """Клиент для работы с Kafka."""

    async def consume_records(
        self,
        batch_size_before_insert: int = 100,
        *,
        idle_timeout_ms: int | None = None,
        max_run_seconds: float | None = None,
    ) -> AsyncGenerator[list[T_BaseModel], None]:
        """
        Выдаёт целые poll и подтверждает пачку при запросе следующей.

        Размер пачки задаёт порог выдачи, поскольку poll не разрезается.
        По умолчанию чтение непрерывное. При заданном таймауте простоя
        чтение завершается на пустом poll, при лимите запуска прекращаются
        новые poll. В обоих случаях выдаётся оставшаяся пачка.
        """
        started_at = perf_counter()
        deadline = None if max_run_seconds is None else started_at + max_run_seconds
        flush_at = started_at + self.max_time_wo_commit
        batch: list[T_BaseModel] = []
        offsets: dict[TopicPartition, int] = {}

        while True:
            records = await self._read_poll(
                max_records=batch_size_before_insert,
                idle_timeout_ms=idle_timeout_ms,
                deadline=deadline,
            )
            if records is None:
                break
            if records:
                models, poll_offsets = await self._decode_poll(records)
                batch.extend(models)
                offsets.update(poll_offsets)

            if len(batch) < batch_size_before_insert and perf_counter() < flush_at:
                continue

            # Вызывающий код продолжит чтение после успешной записи пачки.
            yield batch
            await self._commit(offsets)
            batch = []
            offsets = {}
            flush_at = perf_counter() + self.max_time_wo_commit

        if offsets:
            yield batch
            await self._commit(offsets)

    async def _read_poll(
        self,
        *,
        max_records: int,
        idle_timeout_ms: int | None,
        deadline: float | None,
    ) -> _PollRecords | None:
        """Читает одну порцию Kafka. Возвращает None по завершении чтения."""
        timeout_ms = self.max_time_wo_commit * 1000
        if idle_timeout_ms is not None:
            timeout_ms = idle_timeout_ms
        if deadline is not None:
            remaining_seconds = deadline - perf_counter()
            if remaining_seconds <= 0:
                return None
            timeout_ms = min(timeout_ms, int(remaining_seconds * 1000))

        records = await self.consumer.getmany(
            timeout_ms=timeout_ms,
            max_records=max_records,
        )
        if records:
            return records
        if idle_timeout_ms is not None:
            return None

        retry_delay = 10.0
        if deadline is not None:
            retry_delay = min(retry_delay, max(0.0, deadline - perf_counter()))
        await asyncio.sleep(retry_delay)
        return records

    async def _decode_poll(
        self,
        records: _PollRecords,
    ) -> tuple[list[T_BaseModel], dict[TopicPartition, int]]:
        """Разбирает всю порцию и вычисляет offsets, включая отброшенные записи."""
        messages = flatten(records.values())
        models = await asyncio.to_thread(self._to_records, messages)
        offsets = {
            partition: partition_records[-1].offset + 1
            for partition, partition_records in records.items()
        }
        return filter_not_none(models), offsets

    def _to_records(self, messages: list[ConsumerRecord]) -> list[T_BaseModel | None]:
        return [
            self.serialize(record)
            for message in messages
            for record in to_list_if_dict(message.value)
        ]

    async def close(self) -> None:
        """Закрываем соединение."""
        await self.consumer.stop()

    async def _commit(self, offsets: dict[TopicPartition, int]) -> None:
        if offsets and not self._is_commit_only_manually:
            await self.consumer.commit(offsets)
