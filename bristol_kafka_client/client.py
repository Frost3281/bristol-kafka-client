from dataclasses import dataclass
from typing import Generic, Iterator, Type

# noinspection PyProtectedMember
from kafka import KafkaConsumer

from .types import T_BaseModel


@dataclass
class KafkaClient(Generic[T_BaseModel]):
    """Клиент для работы с Kafka."""

    consumer: KafkaConsumer
    model: Type[T_BaseModel]
    _is_commit_only_manually: bool = False

    def consume_records(
        self, batch_size_before_insert: int = 100,
    ) -> Iterator[list[T_BaseModel]]:
        """Получаем сообщения от консьюмера в бесконечном цикле."""
        fetched_items: list[T_BaseModel] = []
        for fetched_item in self._consume_record():
            fetched_items.append(fetched_item)
            if len(fetched_items) >= batch_size_before_insert:
                yield fetched_items
                fetched_items.clear()
            if not self._is_commit_only_manually:
                self.consumer.commit()
        yield fetched_items

    def _consume_record(self) -> Iterator[T_BaseModel]:
        """Получаем сообщения из Kafka."""
        for message in self.consumer:
            yield from (self.model(**record) for record in message.value)
