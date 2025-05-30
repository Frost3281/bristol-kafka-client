import asyncio
from dataclasses import dataclass
from typing import Any, AsyncIterator

from aiokafka import AIOKafkaConsumer, ConsumerRecord

from ._base import BaseKafkaClient
from .services import to_list_if_dict
from .types import T_BaseModel
from .utils import filter_not_none, flatten

T_DictAny = dict[str, Any]


@dataclass
class KafkaClientAsync(BaseKafkaClient[T_BaseModel, AIOKafkaConsumer]):
    """Клиент для работы с Kafka."""

    async def consume_records(
        self,
        batch_size_before_insert: int = 100,
    ) -> AsyncIterator[list[T_BaseModel]]:
        """Получаем сообщения от консьюмера в бесконечном цикле."""
        async for fetched_item in self._consume_record(batch_size_before_insert):
            if fetched_item:
                self._fetched_items.append(fetched_item)
            if not self._is_batch_full_or_timeout_exceeded(batch_size_before_insert):
                continue
            async for items in self._yield_and_reset():
                yield items
        yield filter_not_none(self._fetched_items)

    async def _consume_record(
        self,
        batch_size_before_insert: int = 100,
    ) -> AsyncIterator[T_BaseModel | None]:
        """Получаем сообщения из Kafka."""
        while True:
            parts_to_records = await self.consumer.getmany(
                timeout_ms=self.max_time_wo_commit * 1000,
                max_records=batch_size_before_insert,
            )
            messages = flatten(parts_to_records.values())
            if not messages:  # топик пустой
                await asyncio.sleep(10)
                yield None
            for record in await asyncio.to_thread(self._to_records, messages):
                yield record

    def _to_records(self, messages: list[ConsumerRecord]) -> list[T_BaseModel | None]:
        return [
            self.serialize(record)
            for message in messages
            for record in to_list_if_dict(message.value)
        ]

    async def close(self) -> None:
        """Закрываем соединение."""
        await self.consumer.stop()

    async def _yield_and_reset(self) -> AsyncIterator[list[T_BaseModel]]:
        yield filter_not_none(self._fetched_items)
        self._fetched_items.clear()
        self._refresh_time()
        await self._commit()

    async def _commit(self) -> None:
        if not self._is_commit_only_manually:
            await self.consumer.commit()
