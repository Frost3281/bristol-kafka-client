import json
from functools import partial
from typing import Any, Protocol, Union

import pydantic_core


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


async def post_to_kafka(  # noqa: PLR0913
    producer: AsyncKafkaProducer,
    data_to_send: Any,  # noqa: ANN401
    topic: str,
    key: Union[str, None] = None,
    *,
    dump_by_alias: bool = False,
    exclude_none_fields: bool = False,
) -> None:
    """Отправляем данные в Kafka."""
    await producer.send(
        topic,
        _to_kafka_bytes(
            data_to_send,
            dump_by_alias=dump_by_alias,
            exclude_none=exclude_none_fields,
        ),
        key=_to_bytes(key),
    )


def _to_bytes(string_to_encode: Union[str, None]) -> Union[bytes, None]:
    return bytes(string_to_encode, 'utf-8') if string_to_encode else None


def _to_kafka_bytes(
    data_to_send: Any,  # noqa: ANN401
    *,
    dump_by_alias: bool = False,
    exclude_none: bool = False,
) -> Union[bytes, None]:
    return _to_bytes(
        json.dumps(
            data_to_send,
            default=partial(
                pydantic_core.to_jsonable_python,
                by_alias=dump_by_alias,
                exclude_none=exclude_none,
            ),
        ),
    )
