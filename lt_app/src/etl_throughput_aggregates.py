# etl_throughput_aggregates.py
"""
Агрегатор пропускной способности ETL-конвейера: Kafka → OpenSearch.

Модуль читает CSV, созданный :class:`OpenSearchIndexMonitor`, и вычисляет:

- Поминутный прирост проиндексированных документов (delta-ряд).
- Скользящее среднее delta-ряда с окном 5 минут.
- Коэффициент покрытия: доля отправленных сообщений, дошедшая до индексов.

Все расчёты синхронные (pandas) и выполняются через :func:`asyncio.to_thread`.

Модуль не содержит Streamlit UI, CLI, argparse и multiprocessing-кода.

Настройки загружаются через Pydantic Settings из environment variables
или локального ``.env`` с префиксом ``ETL_THROUGHPUT_AGGREGATES_``.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from pydantic_settings import BaseSettings, SettingsConfigDict


logger = logging.getLogger(__name__)

OUTPUT_COLUMNS: tuple[str, ...] = (
    "computed_at",
    "total_indexed_documents",
    "total_sent_messages",
    "coverage_ratio",
    "avg_per_minute",
    "max_per_minute",
    "avg_per_minute_smoothed_5min",
    "max_per_minute_smoothed_5min",
)

ProgressCallback = Callable[["EtlThroughputAggregateProgress"], None]


class EtlThroughputAggregateSettings(BaseSettings):
    """
    Настройки построения throughput-агрегатов.

    :ivar source_monitor_csv_path: CSV, созданный OpenSearchIndexMonitor.
    :ivar output_csv_path: Выходной однорядный CSV throughput.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="ETL_THROUGHPUT_AGGREGATES_",
        extra="ignore",
    )

    source_monitor_csv_path: Path
    output_csv_path: Path


@dataclass(frozen=True, slots=True)
class EtlThroughputAggregateProgress:
    """
    Progress-событие построения throughput-агрегатов.

    :ivar phase: Текущий этап: ``loading``, ``aggregating`` или ``writing``.
    """

    phase: str


@dataclass(frozen=True, slots=True)
class EtlThroughputAggregateResult:
    """
    Результат одного запуска агрегатора пропускной способности.

    :ivar source_rows: Число строк во входном CSV (до фильтрации ошибок).
    :ivar minute_buckets: Число минутных бакетов после ресемплинга.
    :ivar stopped_by_request: Признак отмены через stop event.
    :ivar output_csv_path: Путь записанного выходного CSV.
    """

    source_rows: int
    minute_buckets: int
    stopped_by_request: bool
    output_csv_path: Path


class EtlThroughputAggregateBuilder:
    """
    Асинхронный построитель throughput-агрегатов.

    :param settings: Валидированные настройки агрегатора.
    :param progress_callback: Callback с progress metadata для supervisor-а.
    """

    def __init__(
        self,
        settings: EtlThroughputAggregateSettings,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> None:
        self._settings = settings
        self._progress_callback = progress_callback

    async def run(
        self,
        stop_event: asyncio.Event,
        sent_messages: int,
    ) -> EtlThroughputAggregateResult:
        """
        Построить throughput-агрегаты по мониторингу OpenSearch.

        :param stop_event: Сигнал штатной отмены задачи.
        :param sent_messages: Число сообщений, отправленных генератором в Kafka.
        :returns: Результат со счётчиками и путём выходного CSV.
        :raises FileNotFoundError: Если входной CSV отсутствует.
        :raises OSError: При ошибке записи выходного CSV.
        """
        if stop_event.is_set():
            self._emit_progress(
                EtlThroughputAggregateProgress(phase="writing"),
            )
            await asyncio.to_thread(self._write_empty_output)
            return EtlThroughputAggregateResult(
                source_rows=0,
                minute_buckets=0,
                stopped_by_request=True,
                output_csv_path=self._settings.output_csv_path,
            )

        return await asyncio.to_thread(
            self._run_sync,
            sent_messages,
        )

    def _run_sync(self, sent_messages: int) -> EtlThroughputAggregateResult:
        """
        Синхронное ядро: загрузка, агрегация, запись.

        :param sent_messages: Число отправленных сообщений генератором.
        :returns: Результат агрегации.
        :raises FileNotFoundError: Если входной CSV отсутствует.
        :raises OSError: При ошибке записи выходного CSV.
        """
        source_path = self._settings.source_monitor_csv_path

        if not source_path.is_file():
            raise FileNotFoundError(
                f"Monitor CSV not found: {source_path}"
            )

        self._emit_progress(
            EtlThroughputAggregateProgress(phase="loading"),
        )
        source_frame = pd.read_csv(source_path)
        source_rows = len(source_frame)

        self._emit_progress(
            EtlThroughputAggregateProgress(phase="aggregating"),
        )

        output_row = self._compute_throughput(source_frame, sent_messages)
        minute_buckets = 0

        output_row_dict = output_row.to_dict(orient="records")[0]

        self._emit_progress(
            EtlThroughputAggregateProgress(phase="writing"),
        )
        self._write_output(output_row)

        return EtlThroughputAggregateResult(
            source_rows=source_rows,
            minute_buckets=output_row_dict.get("minute_buckets", 0),
            stopped_by_request=False,
            output_csv_path=self._settings.output_csv_path,
        )

    @staticmethod
    def _compute_throughput(
        source_frame: pd.DataFrame,
        sent_messages: int,
    ) -> pd.DataFrame:
        """
        Рассчитать все throughput-показатели одного snapshot.

        :param source_frame: DataFrame из monitor CSV.
        :param sent_messages: Число отправленных сообщений в Kafka.
        :returns: Однорядный DataFrame с колонками OUTPUT_COLUMNS.
        """
        frame = source_frame.copy()

        errors_mask = frame["error"].notna()
        frame = frame[~errors_mask].copy()

        if frame.empty:
            return _build_empty_result(sent_messages)

        frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
        frame = frame[frame["timestamp"].notna()].copy()

        if frame.empty:
            return _build_empty_result(sent_messages)

        frame["minute"] = frame["timestamp"].dt.floor("min")

        last_per_minute_index = (
            frame.groupby(["minute", "index_name"], dropna=True)
            .agg({"document_count": "last"})
            .reset_index()
        )

        wide = last_per_minute_index.pivot(
            index="minute",
            columns="index_name",
            values="document_count",
        )

        wide = wide.sort_index()

        full_minutes = pd.date_range(
            start=wide.index.min(),
            end=wide.index.max(),
            freq="min",
            tz=wide.index.tz,
        )
        wide = wide.reindex(full_minutes)
        wide = wide.ffill()

        cumulative_total = wide.sum(axis=1)

        per_index_delta = wide.diff()

        total_delta = per_index_delta.sum(axis=1, min_count=1)

        total_delta_valid = total_delta.dropna()

        if len(total_delta_valid) == 0:
            avg_per_minute = None
            max_per_minute = None
        else:
            avg_per_minute = float(total_delta_valid.mean())
            max_per_minute = float(total_delta_valid.max())

        delta_smoothed = total_delta.rolling(
            window=5,
            min_periods=1,
        ).mean()

        delta_smoothed_valid = delta_smoothed.dropna()
        if len(delta_smoothed_valid) == 0:
            avg_smoothed = None
            max_smoothed = None
        else:
            avg_smoothed = float(delta_smoothed_valid.mean())
            max_smoothed = float(delta_smoothed_valid.max())

        last_total = cumulative_total.dropna()
        if len(last_total) > 0:
            total_indexed = int(last_total.iloc[-1])
        else:
            total_indexed = 0

        if sent_messages == 0:
            coverage_ratio = None
        else:
            coverage_ratio = total_indexed / sent_messages

        minute_buckets = len(wide)

        result = pd.DataFrame([{
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "total_indexed_documents": total_indexed,
            "total_sent_messages": sent_messages,
            "coverage_ratio": coverage_ratio,
            "avg_per_minute": avg_per_minute,
            "max_per_minute": max_per_minute,
            "avg_per_minute_smoothed_5min": avg_smoothed,
            "max_per_minute_smoothed_5min": max_smoothed,
            "minute_buckets": minute_buckets,
        }], columns=[*OUTPUT_COLUMNS, "minute_buckets"])

        return result

    def _write_output(self, output_row: pd.DataFrame) -> None:
        """
        Записать однорядный CSV throughput.

        :param output_row: DataFrame с одной строкой результата.
        :raises OSError: При ошибке записи.
        """
        output_path = self._settings.output_csv_path
        output_path.parent.mkdir(parents=True, exist_ok=True)

        columns_to_write = [
            col for col in OUTPUT_COLUMNS if col in output_row.columns
        ]
        output_row[columns_to_write].to_csv(
            output_path,
            index=False,
            encoding="utf-8",
        )

    def _write_empty_output(self) -> None:
        """
        Записать пустой (только заголовок) выходной CSV.
        """
        output_path = self._settings.output_csv_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=OUTPUT_COLUMNS).to_csv(
            output_path,
            index=False,
            encoding="utf-8",
        )

    def _emit_progress(
        self,
        progress: EtlThroughputAggregateProgress,
    ) -> None:
        """
        Передать progress event внешнему supervisor-у.

        :param progress: Текущий этап построения агрегатов.
        """
        if self._progress_callback is None:
            return

        try:
            self._progress_callback(progress)
        except Exception:
            logger.exception(
                "ETL throughput aggregate progress callback failed"
            )


def _build_empty_result(sent_messages: int) -> pd.DataFrame:
    """
    Создать однорядный DataFrame для пустого входного CSV.

    :param sent_messages: Число отправленных сообщений.
    :returns: DataFrame с null-значениями, кроме computed_at и sent_messages.
    """
    coverage_ratio = None if sent_messages == 0 else None
    return pd.DataFrame([{
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "total_indexed_documents": 0,
        "total_sent_messages": sent_messages,
        "coverage_ratio": coverage_ratio,
        "avg_per_minute": None,
        "max_per_minute": None,
        "avg_per_minute_smoothed_5min": None,
        "max_per_minute_smoothed_5min": None,
        "minute_buckets": 0,
    }], columns=[*OUTPUT_COLUMNS, "minute_buckets"])