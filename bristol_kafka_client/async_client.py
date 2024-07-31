from dataclasses import dataclass
from typing import Any, AsyncIterator

from aiokafka import AIOKafkaConsumer

from ._base import BaseKafkaClient
from .services import to_list_if_dict
from .types import T_BaseModel

T_DictAny = dict[str, Any]


@dataclass
class KafkaClientAsync(BaseKafkaClient[T_BaseModel, AIOKafkaConsumer]):
    """Клиент для работы с Kafka."""

    async def consume_records(
        self,
        batch_size_before_insert: int = 100,
    ) -> AsyncIterator[list[T_BaseModel]]:
        """Получаем сообщения от консьюмера в бесконечном цикле."""
        fetched_items: list[T_BaseModel] = []
        async for fetched_item in self._consume_record():
            fetched_items.append(fetched_item)
            if len(fetched_items) >= batch_size_before_insert:
                yield fetched_items
                fetched_items.clear()
                if not self._is_commit_only_manually:
                    await self.consumer.commit()
        yield fetched_items

    async def _consume_record(self) -> AsyncIterator[T_BaseModel]:
        """Получаем сообщения из Kafka."""
        async for message in self.consumer:
            for record in to_list_if_dict(message.value):
                yield self.serialize(record)

    async def close(self) -> None:
        """Закрываем соединение."""
        await self.consumer.stop()
