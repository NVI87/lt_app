# opensearch_index_monitor.py
"""
Монитор количества документов в индексах OpenSearch для нагрузочного тестирования.

Модуль периодически опрашивает заданный список индексов OpenSearch методом
``count`` и сохраняет результаты в CSV-артефакт тестовой сессии. Параллельно
он публикует структурированные события progress callback-у, чтобы внешний
оркестратор мог передавать их в Streamlit UI через межпроцессную очередь.

Модуль не содержит Streamlit UI, CLI, multiprocessing orchestration и не
управляет жизненным циклом внешнего процесса. Supervisor создаёт экземпляр
:class:`OpenSearchIndexMonitor`, запускает :meth:`OpenSearchIndexMonitor.run`
в отдельной asyncio-задаче или worker-процессе и передаёт ``asyncio.Event``
для штатной остановки.

Настройки загружаются из environment variables или локального ``.env`` с
префиксом ``OPENSEARCH_INDEX_MONITOR_``.

.. note::

   Монитор использует синхронный клиент ``opensearch-py``. Сетевой вызов
   ``client.count`` выполняется через :func:`asyncio.to_thread`, поэтому
   не блокирует event loop worker-а.
"""

from __future__ import annotations

import asyncio
import csv
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from opensearchpy import OpenSearch
from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


logger = logging.getLogger(__name__)

ProgressCallback = Callable[["OpenSearchIndexCount"], None]


class OpenSearchIndexMonitorSettings(BaseSettings):
    """
    Настройки OpenSearch index monitor.

    Список индексов передаётся через ``.env`` JSON-массивом. Например::

        OPENSEARCH_INDEX_MONITOR_INDEX_NAMES=[
          "attachment_etl_release_tst",
          "knowledge_etl_release_tst_lt_0"
        ]

    :ivar host: DNS-имя или IP-адрес OpenSearch.
    :ivar port: TCP-порт OpenSearch.
    :ivar username: Имя пользователя basic authentication.
    :ivar password: Пароль пользователя; хранится как SecretStr.
    :ivar index_names: Индексы, для которых снимается document count.
    :ivar output_csv_path: CSV-файл результатов текущей тестовой сессии.
    :ivar poll_interval_sec: Интервал между полными обходами индексов.
    :ivar use_ssl: Использовать HTTPS.
    :ivar verify_certs: Проверять TLS-сертификат сервера.
    :ivar request_timeout_sec: Таймаут одного OpenSearch-запроса.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="OPENSEARCH_INDEX_MONITOR_",
        extra="ignore",
    )

    host: str
    port: int = Field(default=9200, gt=0, le=65535)

    username: Optional[str] = None
    password: Optional[SecretStr] = None

    index_names: list[str] = Field(min_length=1)
    output_csv_path: Path

    poll_interval_sec: float = Field(default=1.0, gt=0)
    request_timeout_sec: float = Field(default=10.0, gt=0)

    use_ssl: bool = True
    verify_certs: bool = True
    ssl_assert_hostname: bool = True
    ssl_show_warn: bool = False
    ca_certs_path: Optional[Path] = None

    @field_validator("host", mode="before")
    @classmethod
    def validate_host(cls, value: str) -> str:
        """
        Проверить, что host не является пустой строкой.

        :param value: Значение host из environment или ``.env``.
        :returns: Нормализованный host.
        :raises ValueError: Если host пуст.
        """
        normalized_value = str(value).strip()

        if not normalized_value:
            raise ValueError("OpenSearch host must not be empty")

        return normalized_value

    @field_validator("index_names")
    @classmethod
    def validate_index_names(cls, value: list[str]) -> list[str]:
        """
        Проверить и нормализовать список наблюдаемых индексов.

        :param value: Имена OpenSearch-индексов.
        :returns: Непустой список нормализованных имён.
        :raises ValueError: Если обнаружено пустое имя индекса.
        """
        normalized_values = [index_name.strip() for index_name in value]

        if any(not index_name for index_name in normalized_values):
            raise ValueError("OpenSearch index names must not be empty")

        return normalized_values

    def client_options(self) -> dict[str, Any]:
        """
        Собрать параметры создания клиента ``opensearch-py``.

        Пароль не логируется и извлекается из :class:`pydantic.SecretStr`
        только непосредственно перед созданием клиента.

        :returns: Аргументы конструктора :class:`opensearchpy.OpenSearch`.
        """
        options: dict[str, Any] = {
            "hosts": [{"host": self.host, "port": self.port}],
            "use_ssl": self.use_ssl,
            "verify_certs": self.verify_certs,
            "ssl_assert_hostname": self.ssl_assert_hostname,
            "ssl_show_warn": self.ssl_show_warn,
            "timeout": self.request_timeout_sec,
        }

        if self.username is not None and self.password is not None:
            options["http_auth"] = (
                self.username,
                self.password.get_secret_value(),
            )

        if self.ca_certs_path is not None:
            options["ca_certs"] = str(self.ca_certs_path)

        return options


@dataclass(frozen=True, slots=True)
class OpenSearchIndexCount:
    """
    Результат одного измерения количества документов в индексе.

    :ivar timestamp: UTC-время начала запроса в ISO 8601.
    :ivar index_name: Имя проверяемого индекса.
    :ivar document_count: Количество документов или ``None`` при ошибке.
    :ivar error: Описание ошибки без секретов или ``None`` при успехе.
    """

    timestamp: datetime
    index_name: str
    document_count: Optional[int]
    error: Optional[str]

    @property
    def is_success(self) -> bool:
        """
        Проверить, успешно ли получен count.

        :returns: ``True``, если OpenSearch вернул document count.
        """
        return self.error is None


@dataclass(frozen=True, slots=True)
class OpenSearchIndexMonitorResult:
    """
    Итог выполнения OpenSearch monitor.

    :ivar completed_cycles: Число завершённых обходов списка индексов.
    :ivar written_records: Число строк, записанных в CSV.
    :ivar stopped_by_request: Признак остановки через stop event.
    :ivar output_csv_path: Путь к созданному CSV-артефакту.
    """

    completed_cycles: int
    written_records: int
    stopped_by_request: bool
    output_csv_path: Path


class OpenSearchIndexMonitor:
    """
    Асинхронный монитор document count заданных OpenSearch-индексов.

    Монитор создаётся внутри worker-процесса или управляемой asyncio-задачи.
    Один экземпляр использует один OpenSearch client и один выходной CSV-файл.

    :param settings: Валидированные настройки monitor-а.
    :param progress_callback: Необязимый синхронный обработчик измерений.
    """

    def __init__(
        self,
        settings: OpenSearchIndexMonitorSettings,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> None:
        self._settings = settings
        self._progress_callback = progress_callback
        self._client = OpenSearch(**settings.client_options())

    async def run(
        self,
        stop_event: asyncio.Event,
    ) -> OpenSearchIndexMonitorResult:
        """
        Запустить циклический опрос OpenSearch и запись результатов в CSV.

        При ошибке одного индекса monitor сохраняет ошибку отдельной строкой
        CSV и продолжает опрос остальных индексов. Критическая ошибка записи
        CSV или отмена внешней задачи прерывает выполнение.

        :param stop_event: Сигнал штатного завершения monitor-а.
        :returns: Итоговый результат работы monitor-а.
        :raises OSError: При невозможности создать или записать CSV.
        """
        output_csv_path = self._settings.output_csv_path
        output_csv_path.parent.mkdir(parents=True, exist_ok=True)

        completed_cycles = 0
        written_records = 0

        try:
            with output_csv_path.open(
                mode="a",
                newline="",
                encoding="utf-8",
            ) as output_file:
                writer = csv.DictWriter(
                    output_file,
                    fieldnames=(
                        "timestamp",
                        "index_name",
                        "document_count",
                        "error",
                    ),
                )

                if output_file.tell() == 0:
                    writer.writeheader()
                    output_file.flush()

                while not stop_event.is_set():
                    cycle_records = await self._collect_cycle()

                    for record in cycle_records:
                        writer.writerow(self._record_to_csv_row(record))
                        written_records += 1
                        self._emit_progress(record)

                    output_file.flush()
                    completed_cycles += 1

                    logger.info(
                        "OpenSearch monitor cycle completed: cycle=%s "
                        "records=%s output=%s",
                        completed_cycles,
                        len(cycle_records),
                        output_csv_path,
                    )

                    await self._wait_or_stop(
                        stop_event=stop_event,
                        timeout=self._settings.poll_interval_sec,
                    )

            return OpenSearchIndexMonitorResult(
                completed_cycles=completed_cycles,
                written_records=written_records,
                stopped_by_request=stop_event.is_set(),
                output_csv_path=output_csv_path,
            )

        finally:
            self._close_client()

    async def _collect_cycle(self) -> list[OpenSearchIndexCount]:
        """
        Снять document count для каждого настроенного индекса.

        Синхронные запросы OpenSearch запускаются в отдельном thread,
        чтобы не блокировать asyncio event loop.

        :returns: Результаты обхода индексов.
        """
        return await asyncio.to_thread(self._collect_cycle_sync)

    def _collect_cycle_sync(self) -> list[OpenSearchIndexCount]:
        """
        Синхронно опросить OpenSearch для одного полного цикла.

        Метод предназначен только для выполнения через :func:`asyncio.to_thread`.

        :returns: Результаты обхода индексов, включая ошибки отдельных индексов.
        """
        records: list[OpenSearchIndexCount] = []

        for index_name in self._settings.index_names:
            timestamp = datetime.now(timezone.utc)

            try:
                response = self._client.count(index=index_name)
                document_count = int(response["count"])

                records.append(
                    OpenSearchIndexCount(
                        timestamp=timestamp,
                        index_name=index_name,
                        document_count=document_count,
                        error=None,
                    )
                )
            except Exception as exc:
                logger.exception(
                    "OpenSearch count request failed: index=%s",
                    index_name,
                )

                records.append(
                    OpenSearchIndexCount(
                        timestamp=timestamp,
                        index_name=index_name,
                        document_count=None,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )

        return records

    async def _wait_or_stop(
        self,
        stop_event: asyncio.Event,
        timeout: float,
    ) -> None:
        """
        Подождать указанный интервал либо завершиться по stop event.

        :param stop_event: Сигнал штатного завершения monitor-а.
        :param timeout: Максимальная продолжительность ожидания в секундах.
        """
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=timeout)
        except TimeoutError:
            return

    def _emit_progress(self, record: OpenSearchIndexCount) -> None:
        """
        Передать измерение внешнему обработчику progress-событий.

        Ошибка UI callback-а не должна останавливать независимый мониторинг
        OpenSearch и запись CSV.

        :param record: Результат одного измерения индекса.
        """
        if self._progress_callback is None:
            return

        try:
            self._progress_callback(record)
        except Exception:
            logger.exception("OpenSearch monitor progress callback failed")

    def _close_client(self) -> None:
        """
        Закрыть OpenSearch client при завершении monitor-а.

        Метод сделан идемпотентным на случай исключения до начала цикла.
        """
        try:
            self._client.close()
        except Exception:
            logger.exception("OpenSearch client close failed")

    @staticmethod
    def _record_to_csv_row(
        record: OpenSearchIndexCount,
    ) -> dict[str, str | int | None]:
        """
        Преобразовать результат измерения в строку CSV.

        :param record: Структурированный результат OpenSearch count.
        :returns: Значения для :class:`csv.DictWriter`.
        """
        return {
            "timestamp": record.timestamp.isoformat(),
            "index_name": record.index_name,
            "document_count": record.document_count,
            "error": record.error,
        }
    