from dataclasses import dataclass
from typing import Any, AsyncIterator

from aiokafka import AIOKafkaConsumer, TopicPartition

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
        result = await self.consumer.getmany(
            timeout_ms=self.max_time_wo_commit * 1000,
            max_records=batch_size_before_insert,
        )
        for tp, messages in result.items():
            converted_messages = [to_list_if_dict(message.value) for message in messages]
            yield filter_not_none(
                [self.serialize(msg) for msgs in converted_messages for msg in msgs],
            )
            await self._commit(tp, messages[-1].offset)

    async def close(self) -> None:
        """Закрываем соединение."""
        await self.consumer.stop()

    async def _commit(self, tp: TopicPartition, offset: int) -> None:
        if not self._is_commit_only_manually:
            await self.consumer.commit({tp: offset + 1})
