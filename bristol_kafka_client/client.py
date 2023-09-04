from dataclasses import dataclass
from typing import Any, Callable, Generic, Iterator, Type, Union

# noinspection PyProtectedMember
from kafka import KafkaConsumer

from .exceptions import SerializerNotSetError
from .types import T_BaseModel

T_DictAny = dict[str, Any]


@dataclass
class KafkaClient(Generic[T_BaseModel]):
    """Клиент для работы с Kafka."""

    consumer: KafkaConsumer
    model: Union[Type[T_BaseModel], None] = None
    model_getter: Union[Callable[[T_DictAny], Type[T_BaseModel]], None] = None
    _is_commit_only_manually: bool = False

    def serialize(self, message: T_DictAny) -> T_BaseModel:
        """Получаем модель для сериализации."""
        if self.model:
            return self.model(**message)
        elif self.model_getter:
            return self.model_getter(message)(**message)
        raise SerializerNotSetError()

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
            yield from (self.serialize(record) for record in message.value)
