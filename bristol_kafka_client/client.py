from dataclasses import dataclass
from functools import cached_property
from typing import Generic, Iterator, Type

# noinspection PyProtectedMember
from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition
from kafka.consumer.fetcher import ConsumerRecord

from .types import T_BaseModel

DataWithPartitionsAndOffset = tuple[
    T_BaseModel, TopicPartition, OffsetAndMetadata,
]


@dataclass
class KafkaClient(Generic[T_BaseModel]):
    """Клиент для работы с Kafka."""

    consumer: KafkaConsumer
    model: Type[T_BaseModel]

    @cached_property
    def _need_manual_commit(self) -> bool:
        return not self.consumer.config['enable_auto_commit']

    def consume_records(
        self, batch_size_before_insert: int = 100 * 5,
    ) -> Iterator[list[T_BaseModel]]:
        """Получаем сообщения от консьюмера в бесконечном цикле."""
        fetched_items: list[T_BaseModel] = []
        for fetched_item, partition, offset in self._consume_record():
            fetched_items.append(fetched_item)
            if len(fetched_items) >= batch_size_before_insert:
                yield fetched_items
                fetched_items.clear()
                if self._need_manual_commit:
                    self.consumer.commit_async({partition: offset})
        yield fetched_items

    def _consume_record(self) -> Iterator[DataWithPartitionsAndOffset]:
        """Получаем сообщения из Kafka."""
        for message in self.consumer:
            for record in message.value:
                yield self.model(**record), *self._get_partitions_and_offset(message)

    def _get_partitions_and_offset(
        self, message: ConsumerRecord,
    ) -> tuple[TopicPartition, OffsetAndMetadata]:
        topic_partition = TopicPartition(message.topic, message.partition)
        offset = OffsetAndMetadata(
            message.offset + 1, self.consumer.partitions_for_topic(message.topic),
        )
        return topic_partition, offset
