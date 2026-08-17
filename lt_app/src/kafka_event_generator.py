# kafka_event_generator.py
"""
Генератор Kafka-событий для нагрузочного тестирования ETL-сервисов.

Модуль читает Kafka-сообщения из CSV-дампа и воспроизводит их в тестовый
Kafka-topic. Перед отправкой очередного сообщения генератор может измерять
lag заданной consumer group тестируемого ETL-сервиса и приостанавливать
публикацию, пока lag не снизится до допустимого значения.

Модуль не содержит CLI, UI и управления процессами. Внешний оркестратор
создаёт :class:`KafkaEventGeneratorSettings`, запускает
:meth:`KafkaEventGenerator.run` в отдельном процессе и передаёт
``asyncio.Event`` для штатной остановки.

CSV ожидается в формате с разделителем ``;`` и колонками:
``offset``, ``timestamp``, ``partition``, ``key``, ``value``.
Поле ``value`` передаётся в Kafka без JSON-десериализации и без модификации.

.. note::

   Lag отражает состояние всей указанной consumer group на target topic,
   а не только сообщений, опубликованных данным запуском генератора.
"""

from __future__ import annotations

import asyncio
import csv
import logging
import ssl
import time
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Optional

from aiokafka import AIOKafkaProducer
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.structs import TopicPartition
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


logger = logging.getLogger(__name__)

ProgressCallback = Callable[["KafkaGeneratorProgress"], None]


class KafkaTimestampMode(StrEnum):
    """Режим формирования Kafka timestamp для воспроизводимого события."""

    BROKER = "broker"
    NOW = "now"
    SOURCE = "source"


class KafkaEventGeneratorSettings(BaseModel):
    """
    Настройки генератора событий Kafka для нагрузочного тестирования.

    :ivar source_csv_path: CSV-дамп сообщений для воспроизведения.
    :ivar bootstrap_servers: Строка Kafka bootstrap servers.
    :ivar target_topic: Kafka-topic, в который публикуются сообщения.
    :ivar consumer_group_id: Consumer group тестируемого ETL-сервиса.
    :ivar max_lag: Публикация ждёт, пока lag станет меньше этого значения.
    :ivar max_messages: Общее число публикаций за один запуск.
    :ivar repeat_source: Повторять CSV с начала до достижения max_messages.
    :ivar preserve_source_partition: Использовать partition из CSV-дампа.
    """

    model_config = ConfigDict(extra="ignore")

    source_csv_path: Path
    bootstrap_servers: str
    target_topic: str
    consumer_group_id: str

    max_lag: int = Field(default=3, gt=0)
    max_messages: int = Field(default=100, gt=0)
    lag_control_enabled: bool = True
    lag_check_interval_sec: float = Field(default=1.0, gt=0)
    lag_check_every_messages: int = Field(default=1, gt=0)

    repeat_source: bool = False
    preserve_source_partition: bool = False
    timestamp_mode: KafkaTimestampMode = KafkaTimestampMode.BROKER
    message_delay_sec: float = Field(default=0.0, ge=0)

    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_cafile: Optional[Path] = None
    ssl_certfile: Optional[Path] = None
    ssl_keyfile: Optional[Path] = None

    @field_validator(
        "bootstrap_servers",
        "target_topic",
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
    def validate_security_settings(self) -> "KafkaEventGeneratorSettings":
        """
        Проверить согласованность SASL-настроек.

        :returns: Валидированный объект настроек.
        :raises ValueError: Если включён SASL без необходимых параметров.
        """
        if self.security_protocol.startswith("SASL"):
            if not self.sasl_mechanism:
                raise ValueError(
                    "sasl_mechanism is required for SASL security protocol"
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
        Собрать общие параметры подключения для aiokafka и kafka-python.

        Пароль намеренно не логируется и передаётся клиенту в исходном виде.

        :returns: Аргументы конструкторов Kafka-клиентов.
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
class ReplayEvent:
    """
    Сообщение, прочитанное из CSV-дампа Kafka.

    :ivar source_offset: Offset сообщения в исходном topic.
    :ivar source_partition: Partition сообщения в исходном topic.
    :ivar source_timestamp_ms: Timestamp из CSV в миллисекундах.
    :ivar key: Kafka key или ``None``.
    :ivar value: Исходный payload в bytes без преобразования.
    """

    source_offset: int
    source_partition: int
    source_timestamp_ms: Optional[int]
    key: Optional[bytes]
    value: bytes


@dataclass(frozen=True, slots=True)
class KafkaGeneratorProgress:
    """
    Текущее состояние выполнения генератора.

    Объект предназначен для передачи в UI, логгер, очередь межпроцессного
    обмена или систему метрик.

    :ivar sent_messages: Количество успешно отправленных сообщений.
    :ivar max_messages: Плановое количество сообщений.
    :ivar current_lag: Последний измеренный lag или ``None``.
    :ivar source_offset: Offset текущего исходного сообщения.
    :ivar source_partition: Partition текущего исходного сообщения.
    """

    sent_messages: int
    max_messages: int
    current_lag: Optional[int]
    source_offset: int
    source_partition: int


@dataclass(frozen=True, slots=True)
class KafkaGeneratorResult:
    """
    Результат выполнения генератора.

    :ivar sent_messages: Количество подтверждённо опубликованных сообщений.
    :ivar stopped_by_request: Признак остановки через stop event.
    :ivar last_lag: Последнее измеренное значение lag.
    """

    sent_messages: int
    stopped_by_request: bool
    last_lag: Optional[int]


def read_replay_events(source_csv_path: Path) -> list[ReplayEvent]:
    """
    Прочитать CSV-дамп Kafka и подготовить события к воспроизведению.

    Payload из колонки ``value`` не десериализуется: генератор должен
    воспроизводить байты исходного сообщения без изменения ``spaceId``,
    вложенных объектов или других полей.

    :param source_csv_path: Путь к CSV-дампу.
    :returns: Список событий, отсортированный по timestamp, partition, offset.
    :raises FileNotFoundError: Если CSV-файл не существует.
    :raises ValueError: Если CSV не содержит обязательные колонки или строка
        не соответствует ожидаемому формату.
    """
    if not source_csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {source_csv_path}")

    events: list[ReplayEvent] = []
    required_columns = {"offset", "timestamp", "partition", "key", "value"}

    with source_csv_path.open(newline="", encoding="utf-8") as source_file:
        reader = csv.DictReader(source_file, delimiter=";")

        if not reader.fieldnames:
            raise ValueError(f"CSV has no header: {source_csv_path}")

        missing_columns = required_columns.difference(reader.fieldnames)
        if missing_columns:
            raise ValueError(
                f"CSV misses required columns: {sorted(missing_columns)}"
            )

        for line_number, row in enumerate(reader, start=2):
            try:
                raw_key = row["key"] or None
                raw_timestamp = row["timestamp"] or None

                events.append(
                    ReplayEvent(
                        source_offset=int(row["offset"]),
                        source_partition=int(row["partition"]),
                        source_timestamp_ms=(
                            int(raw_timestamp) if raw_timestamp else None
                        ),
                        key=raw_key.encode("utf-8") if raw_key else None,
                        value=row["value"].encode("utf-8"),
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(
                    f"Invalid CSV row at line {line_number}: {exc}"
                ) from exc

    if not events:
        raise ValueError(f"CSV has no events: {source_csv_path}")

    return sorted(
        events,
        key=lambda event: (
            event.source_timestamp_ms or 0,
            event.source_partition,
            event.source_offset,
        ),
    )


def get_effective_consumer_lag(
    settings: KafkaEventGeneratorSettings,
) -> int:
    """
    Рассчитать фактический lag consumer group по всем partitions target topic.

    Если committed offset уже меньше beginning offset, старые сообщения
    удалены retention-ом. В таком случае lag считается от beginning offset,
    то есть только для данных, которые consumer ещё теоретически способен
    прочитать.

    Функция синхронная, потому что использует ``kafka-python``. Вызывать её
    из async-кода следует через :func:`asyncio.to_thread`.

    :param settings: Настройки генератора и подключения к Kafka.
    :returns: Суммарный effective lag по всем partitions.
    :raises ValueError: Если topic не существует или не имеет partitions.
    :raises kafka.errors.KafkaError: При ошибке подключения или Kafka API.
    """
    admin_client: Optional[KafkaAdminClient] = None
    consumer: Optional[KafkaConsumer] = None

    try:
        connection_options = settings.kafka_connection_options()

        admin_client = KafkaAdminClient(
            **connection_options,
            request_timeout_ms=10_000,
        )
        consumer = KafkaConsumer(
            **connection_options,
            request_timeout_ms=10_000,
            enable_auto_commit=False,
        )

        partitions = consumer.partitions_for_topic(settings.target_topic)
        if not partitions:
            raise ValueError(
                f"Topic has no partitions: {settings.target_topic}"
            )

        topic_partitions = [
            TopicPartition(settings.target_topic, partition)
            for partition in partitions
        ]

        group_offsets = admin_client.list_consumer_group_offsets(
            settings.consumer_group_id
        )
        beginning_offsets = consumer.beginning_offsets(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)

        total_lag = 0

        for topic_partition in topic_partitions:
            beginning_offset = beginning_offsets[topic_partition]
            end_offset = end_offsets[topic_partition]

            committed_metadata = group_offsets.get(topic_partition)
            committed_offset = (
                committed_metadata.offset
                if committed_metadata is not None
                else beginning_offset
            )

            effective_committed_offset = max(
                committed_offset,
                beginning_offset,
            )
            total_lag += max(0, end_offset - effective_committed_offset)

        return total_lag

    finally:
        if consumer is not None:
            consumer.close()

        if admin_client is not None:
            admin_client.close()


class KafkaEventGenerator:
    """
    Асинхронный генератор Kafka-событий для запуска в worker-процессе.

    Экземпляр не управляет процессами самостоятельно. Process supervisor
    создаёт генератор в дочернем процессе, создаёт ``asyncio.Event`` для
    штатной остановки и получает progress-события через callback или очередь.

    :param settings: Валидированные настройки генератора.
    :param progress_callback: Необязимый обработчик прогресса.
    """

    def __init__(
        self,
        settings: KafkaEventGeneratorSettings,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> None:
        self._settings = settings
        self._progress_callback = progress_callback
        self._producer = AIOKafkaProducer(
            **settings.kafka_connection_options(),
        )

    async def run(
        self,
        stop_event: asyncio.Event,
    ) -> KafkaGeneratorResult:
        """
        Запустить воспроизведение CSV в Kafka.

        Producer гарантированно закрывается при штатной остановке, исключении
        публикации, сетевой ошибке или отмене asyncio task.

        :param stop_event: Сигнал прекращения публикации.
        :returns: Итог выполнения генератора.
        :raises Exception: Ошибки Kafka и чтения CSV не проглатываются,
            чтобы внешний оркестратор мог пометить сессию как failed.
        """
        sent_messages = 0
        last_lag: Optional[int] = None

        try:
            await self._producer.start()
            events = read_replay_events(self._settings.source_csv_path)
            while sent_messages < self._settings.max_messages:
                for event in events:
                    if stop_event.is_set():
                        return KafkaGeneratorResult(
                            sent_messages=sent_messages,
                            stopped_by_request=True,
                            last_lag=last_lag,
                        )

                    if (
                        self._settings.lag_control_enabled
                        and sent_messages
                        % self._settings.lag_check_every_messages
                        == 0
                    ):
                        last_lag = await self._wait_for_allowed_lag(
                            stop_event=stop_event,
                        )

                    metadata = await self._producer.send_and_wait(
                        topic=self._settings.target_topic,
                        value=event.value,
                        key=event.key,
                        partition=(
                            event.source_partition
                            if self._settings.preserve_source_partition
                            else None
                        ),
                        timestamp_ms=self._resolve_timestamp_ms(event),
                    )

                    sent_messages += 1

                    self._emit_progress(
                        KafkaGeneratorProgress(
                            sent_messages=sent_messages,
                            max_messages=self._settings.max_messages,
                            current_lag=last_lag,
                            source_offset=event.source_offset,
                            source_partition=event.source_partition,
                        )
                    )

                    logger.info(
                        "Event sent: sent=%s/%s source_offset=%s "
                        "source_partition=%s target_partition=%s "
                        "target_offset=%s lag=%s",
                        sent_messages,
                        self._settings.max_messages,
                        event.source_offset,
                        event.source_partition,
                        metadata.partition,
                        metadata.offset,
                        last_lag,
                    )

                    if sent_messages >= self._settings.max_messages:
                        break

                    if self._settings.message_delay_sec > 0:
                        await self._wait_or_stop(
                            stop_event=stop_event,
                            timeout=self._settings.message_delay_sec,
                        )

                if not self._settings.repeat_source:
                    break

            return KafkaGeneratorResult(
                sent_messages=sent_messages,
                stopped_by_request=stop_event.is_set(),
                last_lag=last_lag,
            )

        finally:
            await self._producer.stop()

    async def _wait_for_allowed_lag(
        self,
        stop_event: asyncio.Event,
    ) -> int:
        """
        Ждать, пока effective lag окажется ниже ``settings.max_lag``.

        :param stop_event: Сигнал штатного завершения.
        :returns: Lag, при котором разрешена очередная отправка.
        """
        while not stop_event.is_set():
            lag = await asyncio.to_thread(
                get_effective_consumer_lag,
                self._settings,
            )

            if lag < self._settings.max_lag:
                return lag

            logger.info(
                "Generator paused by lag control: lag=%s limit=%s",
                lag,
                self._settings.max_lag,
            )

            await self._wait_or_stop(
                stop_event=stop_event,
                timeout=self._settings.lag_check_interval_sec,
            )

        return await asyncio.to_thread(
            get_effective_consumer_lag,
            self._settings,
        )

    def _resolve_timestamp_ms(
        self,
        event: ReplayEvent,
    ) -> Optional[int]:
        """
        Вычислить Kafka timestamp для отправляемого сообщения.

        :param event: Воспроизводимое событие CSV.
        :returns: Timestamp в миллисекундах либо ``None`` для broker timestamp.
        :raises ValueError: Если выбран source timestamp, но он отсутствует.
        """
        if self._settings.timestamp_mode is KafkaTimestampMode.BROKER:
            return None

        if self._settings.timestamp_mode is KafkaTimestampMode.NOW:
            return time.time_ns() // 1_000_000

        if event.source_timestamp_ms is None:
            raise ValueError(
                "Source timestamp is required for timestamp_mode='source'"
            )

        return event.source_timestamp_ms

    async def _wait_or_stop(
        self,
        stop_event: asyncio.Event,
        timeout: float,
    ) -> None:
        """
        Подождать timeout секунд, но завершиться раньше при stop event.

        :param stop_event: Сигнал остановки генератора.
        :param timeout: Максимальная длительность ожидания в секундах.
        """
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=timeout)
        except TimeoutError:
            return

    def _emit_progress(
        self,
        progress: KafkaGeneratorProgress,
    ) -> None:
        """
        Передать прогресс внешнему получателю.

        Ошибки callback не должны ронять Kafka producer, поэтому они
        логируются и подавляются.

        :param progress: Снимок текущего прогресса генератора.
        """
        if self._progress_callback is None:
            return

        try:
            self._progress_callback(progress)
        except Exception:
            logger.exception("Kafka generator progress callback failed")
