from dataclasses import dataclass
from typing import Any, Iterator

# noinspection PyProtectedMember
from kafka import KafkaConsumer

from ._base import BaseKafkaClient
from .services import to_list_if_dict
from .types import T_BaseModel
from .utils import filter_not_none

T_DictAny = dict[str, Any]


@dataclass
class KafkaClient(BaseKafkaClient[T_BaseModel, KafkaConsumer]):
    """Клиент для работы с Kafka."""

    def consume_records(
        self,
        batch_size_before_insert: int = 100,
    ) -> Iterator[list[T_BaseModel]]:
        """Получаем сообщения от консьюмера в бесконечном цикле."""
        for fetched_item in self._consume_record():
            if fetched_item is not None:
                self._fetched_items.append(fetched_item)
            if not self._is_batch_full_or_timeout_exceeded(batch_size_before_insert):
                continue
            yield from self._yield_batch_and_reset()
        yield filter_not_none(self._fetched_items)

    def _consume_record(self) -> Iterator[T_BaseModel | None]:
        """Получаем сообщения из Kafka."""
        for message in self.consumer:
            yield from (
                self.serialize(record) for record in to_list_if_dict(message.value)
            )

    def _yield_batch_and_reset(self) -> Iterator[list[T_BaseModel]]:
        yield filter_not_none(self._fetched_items)
        self._fetched_items.clear()
        self._refresh_time()
        self._commit()

    def _commit(self) -> None:
        if not self._is_commit_only_manually:
            self.consumer.commit()
