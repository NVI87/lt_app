"""
Kafka consumer для сохранения диагностических JSON-событий ETL в файлы.

Модуль читает Kafka-topic с диагностическими событиями тестируемого
ETL-сервиса. В debug-режиме topic может содержать как отчёты об ошибках,
так и объёмные отчёты об успешно обработанных событиях.

Каждое корректное JSON-сообщение сохраняется в отдельный файл. Имя файла
детерминированно строится из Kafka topic, partition и offset, поэтому
повторная обработка того же сообщения после падения процесса безопасна:
файл будет перезаписан атомарно.

Offset consumer group коммитится только после успешной записи JSON-файла.
Это даёт at-least-once семантику: при падении между файловой записью и
commit сообщение может быть обработано повторно, но не должно потеряться.

Модуль не содержит Streamlit UI, CLI, argparse или multiprocessing-кода.
Внешний supervisor запускает :meth:`KafkaJsonArtifactCollector.run` в
асинхронной задаче внутри worker-процесса и передаёт ``asyncio.Event`` для
штатной остановки.

Настройки загружаются через Pydantic Settings из environment variables
или локального ``.env`` с префиксом ``KAFKA_ARTIFACT_COLLECTOR_``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import ssl
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Optional

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord, TopicPartition
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


logger = logging.getLogger(__name__)

ProgressCallback = Callable[["KafkaArtifactSaved"], None]


class KafkaOffsetResetPolicy(StrEnum):
    """
    Политика выбора позиции для новой или потерявшей offset consumer group.

    ``EARLIEST`` подходит для полного сбора retained-диагностики.
    ``LATEST`` подходит для новой тестовой сессии, когда нужны только
    события, опубликованные после старта collector-а.
    """

    EARLIEST = "earliest"
    LATEST = "latest"


class KafkaArtifactCollectorSettings(BaseModel):
    """
    Настройки Kafka JSON artifact collector.

    :ivar bootstrap_servers: Kafka bootstrap servers.
    :ivar topic: Диагностический Kafka-topic ETL-сервиса.
    :ivar consumer_group_id: Отдельная consumer group collector-а.
    :ivar output_directory: Каталог JSON-артефактов сессии.
    :ivar auto_offset_reset: Поведение при отсутствии валидного commit.
    :ivar max_messages: Лимит сохранённых сообщений или ``None``.
    :ivar poll_timeout_ms: Максимальное ожидание Kafka batch.
    :ivar security_protocol: Kafka protocol, например PLAINTEXT или SASL_SSL.
    """

    model_config = ConfigDict(extra="ignore")

    bootstrap_servers: str
    topic: str
    consumer_group_id: str
    output_directory: Path

    auto_offset_reset: KafkaOffsetResetPolicy = (
        KafkaOffsetResetPolicy.EARLIEST
    )
    poll_timeout_ms: int = Field(default=1_000, gt=0)
    max_messages: Optional[int] = Field(default=None, gt=0)

    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_cafile: Optional[Path] = None
    ssl_certfile: Optional[Path] = None
    ssl_keyfile: Optional[Path] = None

    @field_validator("max_messages", mode="before")
    @classmethod
    def normalize_max_messages(cls, value: Any) -> Any:
        """Treat ``0`` as unlimited, matching the UI's ``0 = unlimited`` label."""
        if value == 0:
            return None
        return value

    @field_validator(
        "bootstrap_servers",
        "topic",
        "consumer_group_id",
        mode="before",
    )
    @classmethod
    def validate_non_empty_string(cls, value: str) -> str:
        """
        Проверить, что обязательная строковая настройка не пуста.

        :param value: Значение из environment или ``.env``.
        :returns: Очищенная строка.
        :raises ValueError: Если значение пустое.
        """
        normalized_value = str(value).strip()

        if not normalized_value:
            raise ValueError("Value must not be empty")

        return normalized_value

    @model_validator(mode="after")
    def validate_security_settings(self) -> "KafkaArtifactCollectorSettings":
        """
        Проверить обязательные SASL-настройки.

        :returns: Валидированный объект настроек.
        :raises ValueError: Если включён SASL без механизма или credentials.
        """
        if self.security_protocol.startswith("SASL"):
            if not self.sasl_mechanism:
                raise ValueError(
                    "sasl_mechanism is required for SASL protocol"
                )

            if not self.sasl_username or not self.sasl_password:
                raise ValueError(
                    "sasl_username and sasl_password are required for SASL"
                )

        return self

    @property
    def _has_ssl_files(self) -> bool:
        """Return True when any SSL file path is a non-empty path."""
        return any(
            self._ssl_path(path)
            for path in (self.ssl_cafile, self.ssl_certfile, self.ssl_keyfile)
        )

    @staticmethod
    def _ssl_path(path: Optional[Path]) -> bool:
        """Treat empty ``Path`` values as unset."""
        return path is not None and str(path).strip() not in ("", ".")

    def kafka_connection_options(self) -> dict[str, Any]:
        """
        Собрать параметры подключения для ``AIOKafkaConsumer``.

        :returns: Аргументы конструктора Kafka consumer.
        """
        options: dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_servers,
            "security_protocol": self.security_protocol,
        }

        if self.sasl_mechanism:
            options["sasl_mechanism"] = self.sasl_mechanism

        if self.sasl_username:
            options["sasl_plain_username"] = self.sasl_username

        if self.sasl_password:
            options["sasl_plain_password"] = self.sasl_password

        if self._has_ssl_files:
            ctx = ssl.create_default_context(
                cafile=str(self.ssl_cafile) if self._ssl_path(self.ssl_cafile) else None,
            )
            if self._ssl_path(self.ssl_certfile):
                ctx.load_cert_chain(
                    certfile=str(self.ssl_certfile),
                    keyfile=str(self.ssl_keyfile) if self._ssl_path(self.ssl_keyfile) else None,
                )
            options["ssl_context"] = ctx

        return options


@dataclass(frozen=True, slots=True)
class KafkaArtifactSaved:
    """
    Метаданные JSON-сообщения, успешно сохранённого на диск.

    Payload намеренно не передаётся в progress event: debug-сообщения могут
    быть очень объёмными, а Streamlit UI должен получать только metadata.

    :ivar topic: Kafka-topic сообщения.
    :ivar partition: Kafka partition сообщения.
    :ivar offset: Kafka offset сообщения.
    :ivar output_path: Путь к созданному JSON-файлу.
    :ivar saved_messages: Общее число сохранённых сообщений.
    """

    topic: str
    partition: int
    offset: int
    output_path: Path
    saved_messages: int


@dataclass(frozen=True, slots=True)
class KafkaArtifactCollectorResult:
    """
    Результат завершения Kafka artifact collector-а.

    :ivar saved_messages: Количество успешно сохранённых JSON-файлов.
    :ivar stopped_by_request: Признак остановки через ``stop_event``.
    :ivar output_directory: Каталог итоговых артефактов.
    """

    saved_messages: int
    stopped_by_request: bool
    output_directory: Path


class KafkaJsonArtifactCollector:
    """
    Асинхронный consumer диагностического Kafka-topic с записью JSON-артефактов.

    :param settings: Валидированные настройки collector-а.
    :param progress_callback: Callback для отправки metadata во внешний
        supervisor или межпроцессную очередь.
    """

    def __init__(
        self,
        settings: KafkaArtifactCollectorSettings,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> None:
        self._settings = settings
        self._progress_callback = progress_callback
        self._consumer = AIOKafkaConsumer(
            settings.topic,
            group_id=settings.consumer_group_id,
            auto_offset_reset=settings.auto_offset_reset.value,
            enable_auto_commit=False,
            **settings.kafka_connection_options(),
        )

    async def run(
        self,
        stop_event: asyncio.Event,
    ) -> KafkaArtifactCollectorResult:
        """
        Запустить чтение topic и сохранение JSON-сообщений.

        Collector получает Kafka batches с ограниченным timeout, чтобы
        регулярно реагировать на ``stop_event``. Для каждого сообщения:
        JSON записывается атомарно, после чего коммитится конкретный offset.

        :param stop_event: Сигнал штатного завершения worker-а.
        :returns: Результат работы collector-а.
        :raises UnicodeDecodeError: Если Kafka payload не UTF-8.
        :raises json.JSONDecodeError: Если payload не является корректным JSON.
        :raises OSError: При ошибке создания каталога или записи артефакта.
        """
        self._settings.output_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        saved_messages = 0

        try:
            await self._consumer.start()
            while not stop_event.is_set():
                batches = await self._consumer.getmany(
                    timeout_ms=self._settings.poll_timeout_ms,
                )

                for topic_partition, messages in batches.items():
                    for message in messages:
                        if stop_event.is_set():
                            return KafkaArtifactCollectorResult(
                                saved_messages=saved_messages,
                                stopped_by_request=True,
                                output_directory=self._settings.output_directory,
                            )

                        output_path = await asyncio.to_thread(
                            self._save_message_sync,
                            message,
                        )

                        await self._commit_message(message)

                        saved_messages += 1
                        self._emit_progress(
                            KafkaArtifactSaved(
                                topic=message.topic,
                                partition=message.partition,
                                offset=message.offset,
                                output_path=output_path,
                                saved_messages=saved_messages,
                            )
                        )

                        logger.info(
                            "Kafka diagnostic artifact saved: "
                            "topic=%s partition=%s offset=%s path=%s",
                            message.topic,
                            message.partition,
                            message.offset,
                            output_path,
                        )

                        if (
                            self._settings.max_messages is not None
                            and saved_messages
                            >= self._settings.max_messages
                        ):
                            return KafkaArtifactCollectorResult(
                                saved_messages=saved_messages,
                                stopped_by_request=False,
                                output_directory=self._settings.output_directory,
                            )

            return KafkaArtifactCollectorResult(
                saved_messages=saved_messages,
                stopped_by_request=True,
                output_directory=self._settings.output_directory,
            )

        finally:
            await self._consumer.stop()

    def _save_message_sync(
        self,
        message: ConsumerRecord,
    ) -> Path:
        """
        Синхронно сохранить одно Kafka-сообщение в JSON-файл.

        Метод вызывается через :func:`asyncio.to_thread`, поскольку
        декодирование объёмного JSON и файловая запись не должны блокировать
        event loop consumer-а.

        Сначала создаётся временный файл в той же директории, затем
        используется :func:`os.replace` для атомарной замены.

        :param message: Kafka record для сохранения.
        :returns: Путь к созданному JSON-файлу.
        :raises UnicodeDecodeError: Если payload не декодируется как UTF-8.
        :raises json.JSONDecodeError: Если payload невалиден JSON.
        :raises OSError: При ошибке файловой системы.
        """
        payload = json.loads(message.value.decode("utf-8"))

        output_path = self._build_output_path(message)
        temporary_path = output_path.with_suffix(".json.tmp")

        with temporary_path.open(
            mode="w",
            encoding="utf-8",
        ) as output_file:
            json.dump(
                payload,
                output_file,
                ensure_ascii=False,
                indent=2,
            )
            output_file.write("\n")
            output_file.flush()
            os.fsync(output_file.fileno())

        os.replace(temporary_path, output_path)

        return output_path

    async def _commit_message(
        self,
        message: ConsumerRecord,
    ) -> None:
        """
        Закоммитить offset только после успешной записи JSON-артефакта.

        Kafka commit хранит offset следующего сообщения, поэтому передаётся
        ``message.offset + 1``.

        :param message: Уже сохранённое Kafka-сообщение.
        """
        topic_partition = TopicPartition(
            topic=message.topic,
            partition=message.partition,
        )

        await self._consumer.commit(
            offsets={topic_partition: message.offset + 1},
        )

    def _build_output_path(
        self,
        message: ConsumerRecord,
    ) -> Path:
        """
        Сформировать детерминированный путь JSON-артефакта.

        :param message: Kafka record.
        :returns: Путь ``topic-partition-offset.json`` в output directory.
        """
        filename = (
            f"{message.topic}-"
            f"{message.partition}-"
            f"{message.offset}.json"
        )

        return self._settings.output_directory / filename

    def _emit_progress(
        self,
        progress: KafkaArtifactSaved,
    ) -> None:
        """
        Передать metadata сохранённого сообщения внешнему callback-у.

        Ошибка callback-а не должна ломать сбор диагностических артефактов.

        :param progress: Metadata сохранённого артефакта.
        """
        if self._progress_callback is None:
            return

        try:
            self._progress_callback(progress)
        except Exception:
            logger.exception("Kafka artifact collector callback failed")
            