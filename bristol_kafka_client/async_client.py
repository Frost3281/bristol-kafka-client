from dataclasses import dataclass
from typing import Any, AsyncIterator

from aiokafka import AIOKafkaConsumer

from ._base import BaseKafkaClient
from .services import to_list_if_dict
from .types import T_BaseModel
from .utils import filter_not_none

T_DictAny = dict[str, Any]


@dataclass
class KafkaClientAsync(BaseKafkaClient[T_BaseModel, AIOKafkaConsumer]):
    """Клиент для работы с Kafka."""

    async def consume_records(
        self,
        batch_size_before_insert: int = 100,
    ) -> AsyncIterator[list[T_BaseModel]]:
        """Получаем сообщения от консьюмера в бесконечном цикле."""
        async for fetched_item in self._consume_record():
            if fetched_item is not None:
                self._fetched_items.append(fetched_item)
            if not self._is_batch_full_or_timeout_exceeded(batch_size_before_insert):
                continue
            async for items in self._yield_and_reset():
                yield items

    async def _consume_record(self) -> AsyncIterator[T_BaseModel | None]:
        """Получаем сообщения из Kafka."""
        while True:
            message = await self.consumer.getone()
            for record in to_list_if_dict(message.value):
                yield self.serialize(record)

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
