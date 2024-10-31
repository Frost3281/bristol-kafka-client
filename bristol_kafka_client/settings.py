from typing import Any, Callable, Literal

import orjson
from pydantic import Field
from pydantic_settings import BaseSettings

ONE_SECOND_MS = 1000
ONE_MINUTE_MS = ONE_SECOND_MS * 10 * 6
ONE_HOUR_MS = ONE_MINUTE_MS * 10 * 6

MAX_MESSAGE_SIZE_BYTES = 247_483_647
DEFAULT_SESSION_TIMEOUT_MS = 10 * ONE_SECOND_MS
SESSION_TIMEOUT_MS = DEFAULT_SESSION_TIMEOUT_MS * 10
DEFAULT_MAX_POOL_MS = 5 * ONE_MINUTE_MS
DEFAULT_MAX_POOL_RECORDS = 100 * 5
FETCH_MAX_WAIT_MS = 100 * 5
CONSUMER_TIMEOUT_MS = ONE_HOUR_MS * 8


class BaseAuthMixin(BaseSettings):
    """Микс-ин для базовой аутентификации."""

    sasl_plain_username: str
    sasl_plain_password: str
    sasl_mechanism: str = 'PLAIN'
    security_protocol: str = 'SASL_PLAINTEXT'


class KafkaProducerSettings(BaseSettings):
    """Базовый конфиг продюсера."""

    bootstrap_servers: list[str]
    client_id: str
    topic: str = Field(exclude=True)
    compression_type: str = 'lz4'
    acks: Literal[0, 1, 'all'] = 'all'


class KafkaConsumerSettings(BaseSettings):
    """Базовый конфиг консьюмера."""

    topic: str
    group_id: str
    bootstrap_servers: list[str]
    auto_offset_reset: str = 'earliest'
    value_deserializer: Callable[..., Any] = orjson.loads
    enable_auto_commit: bool = False
    fetch_max_bytes: int = MAX_MESSAGE_SIZE_BYTES
    session_timeout_ms: int = SESSION_TIMEOUT_MS
    fetch_max_wait_ms: int = FETCH_MAX_WAIT_MS * 10 * 2
    max_poll_interval_ms: int = DEFAULT_MAX_POOL_MS * 7
    max_poll_records: int = int(DEFAULT_MAX_POOL_RECORDS / 2)
    consumer_timeout_ms: int = CONSUMER_TIMEOUT_MS


class KafkaConsumerSettingsWithBasicAuth(KafkaConsumerSettings, BaseAuthMixin):
    """Настройки с базовой аутентификацией."""


class KafkaProducerSettingsWithBasicAuth(KafkaProducerSettings, BaseAuthMixin):
    """Настройки продюсера с базовой аутентификацией."""
