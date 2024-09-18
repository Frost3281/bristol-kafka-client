import json
from typing import Any, Literal, Protocol, Sequence, Union

from pydantic.main import IncEx


class KafkaData(Protocol):
    """Данные для отправки в Kafka."""

    def model_dump_json(  # noqa: PLR0913
        self,
        *,
        indent: int | None = None,
        include: IncEx = None,
        exclude: IncEx = None,
        context: Any | None = None,  # noqa: ANN401
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        round_trip: bool = False,
        warnings: bool | Literal['none', 'warn', 'error'] = True,
        serialize_as_any: bool = False,
    ) -> str:
        """Метод для сериализации модели."""


class KafkaProducer(Protocol):
    """Продюсер для Кафка."""

    def send(
        self,
        topic: str,
        value: Union[bytes, None],  # noqa: WPS110
        key: Union[bytes, None] = None,
        partition: Union[int, None] = None,
    ) -> None:
        """Метод отправки данных в Kafka."""


class AsyncKafkaProducer(Protocol):
    """Продюсер для Кафка (асинхронный)."""

    async def send(
        self,
        topic: str,
        value: Union[bytes, None],  # noqa: WPS110
        key: Union[bytes, None] = None,
        partition: Union[int, None] = None,
    ) -> None:
        """Метод отправки данных в Kafka."""


async def post_to_kafka(
    producer: AsyncKafkaProducer,
    data_to_send: Sequence[KafkaData],
    topic: str,
    key: Union[str, None] = None,
    *,
    dump_by_alias: bool = False,
) -> None:
    """Отправляем данные в Kafka."""
    await producer.send(
        topic,
        _transform_models_for_kafka(data_to_send, dump_by_alias=dump_by_alias),
        key=_to_bytes(key),
    )


def _to_bytes(string_to_encode: Union[str, None]) -> Union[bytes, None]:
    return bytes(string_to_encode, 'utf-8') if string_to_encode else None


def _transform_models_for_kafka(
    data_to_send: Sequence[KafkaData],
    *,
    dump_by_alias: bool,
) -> Union[bytes, None]:
    json_to_send = json.dumps(
        [
            item_to_send.model_dump_json(by_alias=dump_by_alias)
            for item_to_send in data_to_send
        ],
        indent=4,
        default=str,
        ensure_ascii=False,
    )
    return _to_bytes(json_to_send)
