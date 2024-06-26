from dataclasses import dataclass
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
    _is_commit_only_manually: bool = False

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
