import time
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, Type, TypeVar, Union

from .exceptions import SerializerNotSetError
from .types import T_BaseModel

# noinspection PyProtectedMember

T_Consumer = TypeVar('T_Consumer')
T_DictAny = dict[str, Any]


@dataclass
class BaseKafkaClient(Generic[T_BaseModel, T_Consumer]):
    """Базовый клиент для работы с Kafka."""

    consumer: T_Consumer
    model: Union[Type[T_BaseModel], None] = None
    model_getter: Union[Callable[[T_DictAny], T_BaseModel], None] = None
    max_time_wo_commit: int = 60 * 5
    _is_commit_only_manually: bool = False

    _fetched_items: list[T_BaseModel | None] = field(default_factory=list, init=False)
    _start_time: float = field(default_factory=time.perf_counter, init=False)

    def __post_init__(self) -> None:
        """Проверки."""
        for check in self._checks:
            check()

    def serialize(self, message: T_DictAny) -> T_BaseModel:
        """Получаем модель для сериализации."""
        if self.model:
            return self.model(**message)
        if self.model_getter:
            return self.model_getter(message)
        raise SerializerNotSetError  # для mypy

    @property
    def _checks(self) -> list[Callable[[], None]]:
        return [
            self._check_model_or_getter_setted,
        ]

    def _check_model_or_getter_setted(self) -> None:
        if not self.model and not self.model_getter:
            raise SerializerNotSetError

    def _is_batch_full_or_timeout_exceeded(
        self,
        batch_size_before_insert: int,
    ) -> bool:
        is_batch_full = len(self._fetched_items) >= batch_size_before_insert
        is_timeout = time.perf_counter() - self._start_time >= self.max_time_wo_commit
        return is_batch_full or is_timeout

    def _refresh_time(self) -> None:
        self._start_time = time.perf_counter()
