from dataclasses import dataclass
from typing import Any, Iterator

# noinspection PyProtectedMember
from kafka import KafkaConsumer

from ._base import BaseKafkaClient
from .services import to_list_if_dict
from .types import T_BaseModel

T_DictAny = dict[str, Any]


@dataclass
class KafkaClient(BaseKafkaClient[T_BaseModel, KafkaConsumer]):
    """Клиент для работы с Kafka."""

    def consume_records(
        self,
        batch_size_before_insert: int = 100,
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
            yield from (
                self.serialize(record) for record in to_list_if_dict(message.value)
            )
