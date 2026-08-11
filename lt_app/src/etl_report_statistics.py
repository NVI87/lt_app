# etl_report_statistics.py
"""
Агрегатор статистики по JSON-отчётам ETL-пайплайна.

Модуль читает JSON-артефакты диагностического Kafka-topic, сохранённые
:class:`KafkaJsonArtifactCollector`, и строит табличную статистику по
обработке документов и вложений.

В итоговую статистику попадают только документы и вложения, дошедшие до
статуса ``DONE`` или ``SUB_DONE``. Неуспешные, неполные и не дошедшие до
финального статуса сущности сохраняются отдельно в CSV пропущенных записей.

Для каждого успешно обработанного вложения извлекаются метрики FSM-стадий:
``EXTRACTING``, ``CONVERTING``, ``TOKENIZING``, ``VECTORIZING``,
``INDEXING`` и другие стадии, присутствующие в исходном отчёте.

Модуль не содержит Streamlit UI, CLI, argparse и multiprocessing-кода.
Внешний supervisor запускает :meth:`EtlReportStatisticsBuilder.run` как
конечную asyncio-задачу после НТ или для построения промежуточного snapshot.

Настройки загружаются через Pydantic Settings из environment variables
или локального ``.env`` с префиксом ``ETL_REPORT_STATISTICS_``.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


logger = logging.getLogger(__name__)

DONE_STATES: frozenset[str] = frozenset({"DONE", "SUB_DONE"})

DONE_COLUMNS: tuple[str, ...] = (
    "report_file",
    "document_id",
    "document_name",
    "attach_id",
    "attach_name",
    "attach_type",
    "attach_size",
    "stage",
    "status",
    "wall_time_sec",
    "cpu_mean_pct",
    "cpu_peak_pct",
    "rss_mean",
    "rss_peak",
    "samples",
    "started_at",
)

SKIPPED_COLUMNS: tuple[str, ...] = (
    "report_file",
    "document_id",
    "document_name",
    "attach_id",
    "attach_name",
    "attach_type",
    "attach_size",
    "status",
    "reason",
)

READ_FAILURE_COLUMNS: tuple[str, ...] = (
    "report_file",
    "error_type",
    "error_message",
)

ProgressCallback = Callable[["EtlReportStatisticsProgress"], None]


class EtlReportStatisticsSettings(BaseSettings):
    """
    Настройки построения статистики по JSON-отчётам ETL.

    :ivar source_directory: Каталог с JSON-артефактами Kafka consumer-а.
    :ivar done_output_csv_path: CSV со строками DONE/SUB_DONE.
    :ivar skipped_output_csv_path: CSV с неуспешными и неполными сущностями.
    :ivar read_failures_output_csv_path: CSV ошибок чтения или JSON parsing.
    :ivar report_glob: Glob для поиска JSON-файлов.
    :ivar include_page_body_rows: Добавлять строку для DONE-документов
        без вложений.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="ETL_REPORT_STATISTICS_",
        extra="ignore",
    )

    source_directory: Path
    done_output_csv_path: Path
    skipped_output_csv_path: Path
    read_failures_output_csv_path: Path

    report_glob: str = "*.json"
    include_page_body_rows: bool = True
    sort_reports_by_name: bool = True

    @property
    def output_paths(self) -> tuple[Path, Path, Path]:
        """
        Вернуть все файлы-артефакты построения статистики.

        :returns: Пути CSV для DONE, skipped и read failures.
        """
        return (
            self.done_output_csv_path,
            self.skipped_output_csv_path,
            self.read_failures_output_csv_path,
        )


@dataclass(frozen=True, slots=True)
class EtlDoneStageRow:
    """
    Строка успешной обработки вложения или документа.

    Одна строка соответствует одной FSM-стадии одного вложения. Для DONE
    документа без вложений создаётся специальная строка ``page_body`` без
    stage metrics.

    :ivar report_file: Имя исходного JSON-отчёта.
    :ivar document_id: Идентификатор документа ETL.
    :ivar attach_id: Идентификатор вложения или ``None``.
    :ivar stage: Имя FSM-стадии или ``None`` для page body.
    :ivar wall_time_sec: Wall-clock длительность стадии.
    :ivar rss_peak: Пиковое RSS потребление процесса.
    """

    report_file: str
    document_id: Optional[str]
    document_name: Optional[str]
    attach_id: Optional[str]
    attach_name: Optional[str]
    attach_type: str
    attach_size: Optional[int]
    stage: Optional[str]
    status: str
    wall_time_sec: Optional[float]
    cpu_mean_pct: Optional[float]
    cpu_peak_pct: Optional[float]
    rss_mean: Optional[float]
    rss_peak: Optional[float]
    samples: Optional[int]
    started_at: Optional[datetime]


@dataclass(frozen=True, slots=True)
class EtlSkippedRow:
    """
    Сущность, не попавшая в статистику успешной обработки.

    :ivar report_file: Имя исходного JSON-отчёта.
    :ivar status: Финальный или текущий статус ETL-сущности.
    :ivar reason: Причина исключения из DONE-статистики.
    """

    report_file: str
    document_id: Optional[str]
    document_name: Optional[str]
    attach_id: Optional[str]
    attach_name: Optional[str]
    attach_type: Optional[str]
    attach_size: Optional[int]
    status: Optional[str]
    reason: str


@dataclass(frozen=True, slots=True)
class EtlReportReadFailure:
    """
    Ошибка чтения одного JSON-артефакта.

    :ivar report_file: Имя проблемного файла.
    :ivar error_type: Тип возникшего исключения.
    :ivar error_message: Текст ошибки без содержимого payload.
    """

    report_file: str
    error_type: str
    error_message: str


@dataclass(frozen=True, slots=True)
class EtlReportStatisticsProgress:
    """
    Progress-событие построения статистики.

    :ivar processed_reports: Количество уже обработанных файлов.
    :ivar total_reports: Количество файлов в snapshot.
    :ivar done_rows: Число извлечённых строк успешных стадий.
    :ivar skipped_rows: Число исключённых сущностей.
    :ivar read_failures: Число файлов с ошибкой чтения.
    """

    processed_reports: int
    total_reports: int
    done_rows: int
    skipped_rows: int
    read_failures: int


@dataclass(frozen=True, slots=True)
class EtlReportStatisticsResult:
    """
    Результат одного запуска агрегатора статистики.

    :ivar processed_reports: Количество файлов, прочитанных без ошибки.
    :ivar done_rows: Количество строк в DONE CSV.
    :ivar skipped_rows: Количество строк в skipped CSV.
    :ivar read_failures: Количество проблемных JSON-файлов.
    :ivar stopped_by_request: Признак отмены через stop event.
    """

    processed_reports: int
    done_rows: int
    skipped_rows: int
    read_failures: int
    stopped_by_request: bool
    done_output_csv_path: Path
    skipped_output_csv_path: Path
    read_failures_output_csv_path: Path


class EtlReportStatisticsBuilder:
    """
    Асинхронный построитель CSV-статистики по JSON-отчётам ETL.

    Экземпляр обрабатывает snapshot списка файлов из ``source_directory``.
    Файлы, появившиеся после старта :meth:`run`, попадут в следующий запуск
    анализатора.

    :param settings: Валидированные настройки агрегатора.
    :param progress_callback: Callback с progress metadata для supervisor-а.
    """

    def __init__(
        self,
        settings: EtlReportStatisticsSettings,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> None:
        self._settings = settings
        self._progress_callback = progress_callback

    async def run(
        self,
        stop_event: asyncio.Event,
    ) -> EtlReportStatisticsResult:
        """
        Построить CSV-таблицы по JSON-артефактам текущего snapshot.

        JSON parsing и операции pandas выполняются через
        :func:`asyncio.to_thread`, чтобы задача не блокировала event loop
        worker-процесса.

        :param stop_event: Сигнал штатной отмены анализа.
        :returns: Пути и счётчики созданных аналитических артефактов.
        :raises OSError: При невозможности прочитать каталог или записать CSV.
        """
        report_paths = self._get_report_paths()
        done_rows: list[EtlDoneStageRow] = []
        skipped_rows: list[EtlSkippedRow] = []
        read_failures: list[EtlReportReadFailure] = []

        processed_reports = 0
        stopped_by_request = False

        for report_path in report_paths:
            if stop_event.is_set():
                stopped_by_request = True
                break

            try:
                report = await asyncio.to_thread(
                    self._load_report,
                    report_path,
                )
                current_done_rows, current_skipped_rows = (
                    await asyncio.to_thread(
                        self._extract_rows,
                        report_path,
                        report,
                    )
                )
            except Exception as exc:
                logger.warning(
                    "ETL report cannot be processed: file=%s error=%s",
                    report_path.name,
                    exc,
                )
                read_failures.append(
                    EtlReportReadFailure(
                        report_file=report_path.name,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                )
            else:
                processed_reports += 1
                done_rows.extend(current_done_rows)
                skipped_rows.extend(current_skipped_rows)

            self._emit_progress(
                EtlReportStatisticsProgress(
                    processed_reports=processed_reports,
                    total_reports=len(report_paths),
                    done_rows=len(done_rows),
                    skipped_rows=len(skipped_rows),
                    read_failures=len(read_failures),
                )
            )

        await asyncio.to_thread(
            self._write_csv_artifacts,
            done_rows,
            skipped_rows,
            read_failures,
        )

        return EtlReportStatisticsResult(
            processed_reports=processed_reports,
            done_rows=len(done_rows),
            skipped_rows=len(skipped_rows),
            read_failures=len(read_failures),
            stopped_by_request=stopped_by_request,
            done_output_csv_path=self._settings.done_output_csv_path,
            skipped_output_csv_path=self._settings.skipped_output_csv_path,
            read_failures_output_csv_path=(
                self._settings.read_failures_output_csv_path
            ),
        )

    def _get_report_paths(self) -> list[Path]:
        """
        Получить список JSON-отчётов для текущего snapshot анализа.

        :returns: Пути JSON-файлов из source directory.
        :raises FileNotFoundError: Если каталог отчётов не существует.
        """
        source_directory = self._settings.source_directory

        if not source_directory.is_dir():
            raise FileNotFoundError(
                f"Report directory does not exist: {source_directory}"
            )

        report_paths = list(source_directory.glob(self._settings.report_glob))

        if self._settings.sort_reports_by_name:
            report_paths.sort(key=lambda path: path.name)

        return report_paths

    @staticmethod
    def _load_report(report_path: Path) -> dict[str, Any]:
        """
        Прочитать один JSON-отчёт ETL.

        :param report_path: Путь к JSON-файлу.
        :returns: Десериализованный JSON object.
        :raises json.JSONDecodeError: Если файл содержит невалидный JSON.
        :raises OSError: При ошибке файловой системы.
        :raises ValueError: Если корень JSON не является object.
        """
        with report_path.open(encoding="utf-8") as report_file:
            report = json.load(report_file)

        if not isinstance(report, dict):
            raise ValueError("JSON report root must be an object")

        return report

    def _extract_rows(
        self,
        report_path: Path,
        report: dict[str, Any],
    ) -> tuple[list[EtlDoneStageRow], list[EtlSkippedRow]]:
        """
        Извлечь DONE и skipped строки из одного ETL JSON-отчёта.

        :param report_path: Путь исходного JSON-артефакта.
        :param report: Десериализованный JSON object.
        :returns: Кортеж ``(done_rows, skipped_rows)``.
        """
        done_rows: list[EtlDoneStageRow] = []
        skipped_rows: list[EtlSkippedRow] = []

        message = self._as_dict(report.get("message"))
        head_task = self._as_dict(message.get("head_task"))
        meta = self._as_dict(head_task.get("meta"))
        value = self._as_dict(meta.get("value"))

        document_id = self._as_optional_string(meta.get("document_id"))
        document_name = self._as_optional_string(meta.get("name"))
        head_state = self._as_optional_string(head_task.get("state"))

        attachments = self._as_list(value.get("attachments"))
        subtasks = self._as_dict(message.get("subtasks"))
        sub_attachments = self._as_list(subtasks.get("attachments"))

        started_at = self._extract_started_at(head_task)

        if head_state not in DONE_STATES:
            skipped_rows.append(
                EtlSkippedRow(
                    report_file=report_path.name,
                    document_id=document_id,
                    document_name=document_name,
                    attach_id=None,
                    attach_name=None,
                    attach_type=None,
                    attach_size=None,
                    status=head_state,
                    reason="head_task_not_done",
                )
            )
            return done_rows, skipped_rows

        if not attachments:
            if self._settings.include_page_body_rows:
                done_rows.append(
                    EtlDoneStageRow(
                        report_file=report_path.name,
                        document_id=document_id,
                        document_name=document_name,
                        attach_id=None,
                        attach_name=None,
                        attach_type="page_body",
                        attach_size=None,
                        stage=None,
                        status=head_state,
                        wall_time_sec=None,
                        cpu_mean_pct=None,
                        cpu_peak_pct=None,
                        rss_mean=None,
                        rss_peak=None,
                        samples=None,
                        started_at=started_at,
                    )
                )

            return done_rows, skipped_rows

        sub_attachments_by_id = {
            self._as_optional_string(sub_attachment.get("id")): sub_attachment
            for sub_attachment in sub_attachments
            if isinstance(sub_attachment, dict)
        }

        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue

            attachment_id = self._as_optional_string(attachment.get("id"))
            attachment_name = self._as_optional_string(
                attachment.get("fileName")
            )
            attachment_type = self._as_optional_string(
                attachment.get("fileType")
            )
            attachment_size = self._as_optional_int(
                attachment.get("fileSize")
            )

            sub_attachment = sub_attachments_by_id.get(attachment_id, {})
            sub_state = self._as_optional_string(
                sub_attachment.get("state")
            )
            stats = self._as_dict(sub_attachment.get("stats"))

            if sub_state not in DONE_STATES:
                skipped_rows.append(
                    self._build_skipped_attachment_row(
                        report_file=report_path.name,
                        document_id=document_id,
                        document_name=document_name,
                        attachment_id=attachment_id,
                        attachment_name=attachment_name,
                        attachment_type=attachment_type,
                        attachment_size=attachment_size,
                        status=sub_state or head_state,
                        reason="attachment_not_done",
                    )
                )
                continue

            if not stats:
                skipped_rows.append(
                    self._build_skipped_attachment_row(
                        report_file=report_path.name,
                        document_id=document_id,
                        document_name=document_name,
                        attachment_id=attachment_id,
                        attachment_name=attachment_name,
                        attachment_type=attachment_type,
                        attachment_size=attachment_size,
                        status=sub_state,
                        reason="attachment_done_without_stats",
                    )
                )
                continue

            for stat_key, stat_value in stats.items():
                if not isinstance(stat_value, dict):
                    continue

                load = self._as_dict(stat_value.get("load"))
                stage = str(stat_key).split(":")[-1]

                done_rows.append(
                    EtlDoneStageRow(
                        report_file=report_path.name,
                        document_id=document_id,
                        document_name=document_name,
                        attach_id=attachment_id,
                        attach_name=attachment_name,
                        attach_type=attachment_type or "unknown",
                        attach_size=attachment_size,
                        stage=stage,
                        status=sub_state,
                        wall_time_sec=self._as_optional_float(
                            load.get("wall_time_sec")
                        ),
                        cpu_mean_pct=self._as_optional_float(
                            load.get("cpu_mean_pct")
                        ),
                        cpu_peak_pct=self._as_optional_float(
                            load.get("cpu_peak_pct")
                        ),
                        rss_mean=self._as_optional_float(load.get("rss_mean")),
                        rss_peak=self._as_optional_float(load.get("rss_peak")),
                        samples=self._as_optional_int(load.get("samples")),
                        started_at=started_at,
                    )
                )

        return done_rows, skipped_rows

    def _write_csv_artifacts(
        self,
        done_rows: list[EtlDoneStageRow],
        skipped_rows: list[EtlSkippedRow],
        read_failures: list[EtlReportReadFailure],
    ) -> None:
        """
        Сохранить табличные артефакты текущего запуска в CSV.

        CSV создаются даже при отсутствии данных, чтобы UI и отчётный слой
        всегда получали предсказуемый набор файлов и колонок.

        :param done_rows: Строки успешных FSM-стадий.
        :param skipped_rows: Строки не-DONE или неполных сущностей.
        :param read_failures: Ошибки чтения JSON-файлов.
        :raises OSError: При невозможности создать каталог или CSV.
        """
        for output_path in self._settings.output_paths:
            output_path.parent.mkdir(parents=True, exist_ok=True)

        done_frame = pd.DataFrame(
            [asdict(row) for row in done_rows],
            columns=DONE_COLUMNS,
        )
        skipped_frame = pd.DataFrame(
            [asdict(row) for row in skipped_rows],
            columns=SKIPPED_COLUMNS,
        )
        failures_frame = pd.DataFrame(
            [asdict(row) for row in read_failures],
            columns=READ_FAILURE_COLUMNS,
        )

        done_frame.to_csv(
            self._settings.done_output_csv_path,
            index=False,
            encoding="utf-8",
        )
        skipped_frame.to_csv(
            self._settings.skipped_output_csv_path,
            index=False,
            encoding="utf-8",
        )
        failures_frame.to_csv(
            self._settings.read_failures_output_csv_path,
            index=False,
            encoding="utf-8",
        )

    def _emit_progress(
        self,
        progress: EtlReportStatisticsProgress,
    ) -> None:
        """
        Передать progress event внешнему supervisor-у.

        :param progress: Снимок текущего состояния анализа.
        """
        if self._progress_callback is None:
            return

        try:
            self._progress_callback(progress)
        except Exception:
            logger.exception("ETL statistics progress callback failed")

    @staticmethod
    def _extract_started_at(
        head_task: dict[str, Any],
    ) -> Optional[datetime]:
        """
        Преобразовать ``trace.created_at`` Unix timestamp в UTC datetime.

        :param head_task: JSON object head task.
        :returns: UTC datetime или ``None``, если timestamp отсутствует.
        """
        trace = EtlReportStatisticsBuilder._as_dict(head_task.get("trace"))
        created_at = trace.get("created_at")

        try:
            return datetime.fromtimestamp(
                float(created_at),
                tz=timezone.utc,
            )
        except (TypeError, ValueError, OSError):
            return None

    @staticmethod
    def _build_skipped_attachment_row(
        report_file: str,
        document_id: Optional[str],
        document_name: Optional[str],
        attachment_id: Optional[str],
        attachment_name: Optional[str],
        attachment_type: Optional[str],
        attachment_size: Optional[int],
        status: Optional[str],
        reason: str,
    ) -> EtlSkippedRow:
        """
        Создать строку исключённого вложения.

        :param report_file: Имя исходного JSON-файла.
        :param document_id: Идентификатор документа.
        :param document_name: Имя документа.
        :param attachment_id: Идентификатор вложения.
        :param attachment_name: Имя вложения.
        :param attachment_type: Тип вложения.
        :param attachment_size: Размер вложения.
        :param status: Статус subtask или head task.
        :param reason: Машиночитаемая причина исключения.
        :returns: Строка skipped CSV.
        """
        return EtlSkippedRow(
            report_file=report_file,
            document_id=document_id,
            document_name=document_name,
            attach_id=attachment_id,
            attach_name=attachment_name,
            attach_type=attachment_type,
            attach_size=attachment_size,
            status=status,
            reason=reason,
        )

    @staticmethod
    def _as_dict(value: Any) -> dict[str, Any]:
        """
        Вернуть value как dict либо пустой dict.

        :param value: Произвольное JSON-значение.
        :returns: Dict или пустой dict.
        """
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _as_list(value: Any) -> list[Any]:
        """
        Вернуть value как list либо пустой list.

        :param value: Произвольное JSON-значение.
        :returns: List или пустой list.
        """
        return value if isinstance(value, list) else []

    @staticmethod
    def _as_optional_string(value: Any) -> Optional[str]:
        """
        Преобразовать JSON-значение в строку или None.

        :param value: Произвольное JSON-значение.
        :returns: Непустая строка или ``None``.
        """
        if value is None:
            return None

        normalized_value = str(value).strip()
        return normalized_value or None

    @staticmethod
    def _as_optional_int(value: Any) -> Optional[int]:
        """
        Преобразовать JSON-значение в int или None.

        :param value: Произвольное JSON-значение.
        :returns: Целое число или ``None``.
        """
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_optional_float(value: Any) -> Optional[float]:
        """
        Преобразовать JSON-значение в float или None.

        :param value: Произвольное JSON-значение.
        :returns: Число с плавающей точкой или ``None``.
        """
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None
        