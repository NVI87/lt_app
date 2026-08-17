# etl_stage_size_aggregates.py
"""
Агрегатор статистики по размеру вложений и FSM-стадиям ETL-пайплайна.

Модуль читает DONE CSV, созданный :class:`EtlReportStatisticsBuilder`, группирует
строки по ``(attach_type, size_bucket_mb, stage)`` и рассчитывает взвешенные
агрегаты времени, CPU и RSS памяти.

Бакетирование размера — степени двойки 1..1024 МБ с отнесением реального размера
к ближайшему бакету через ``round(log2(size_mb))``.

Для каждой группы вычисляются суммарное время, средневзвешенные (по ``samples``)
утилизации CPU и памяти, и пиковые значения.

Модуль не содержит Streamlit UI, CLI, argparse и multiprocessing-кода.
Внешний supervisor запускает :meth:`EtlStageSizeAggregateBuilder.run` как
конечную asyncio-задачу после НТ или для построения периодического snapshot.

Настройки загружаются через Pydantic Settings из environment variables
или локального ``.env`` с префиксом ``ETL_STAGE_SIZE_AGGREGATES_``.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict


logger = logging.getLogger(__name__)

BYTES_IN_MB = 1024 * 1024

SIZE_GRID_MB: tuple[int, ...] = (
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024,
)

OUTPUT_COLUMNS: tuple[str, ...] = (
    "attach_type",
    "size_bucket_mb",
    "stage",
    "row_count",
    "total_samples",
    "total_wall_time_sec",
    "weighted_mean_cpu_pct",
    "peak_cpu_pct",
    "weighted_mean_rss_bytes",
    "peak_rss_bytes",
)

ProgressCallback = Callable[["EtlStageSizeAggregateProgress"], None]


class EtlStageSizeAggregateSettings(BaseModel):
    """
    Настройки построения агрегатов по размеру и стадиям ETL.

    :ivar source_done_csv_path: DONE CSV, созданный EtlReportStatisticsBuilder.
    :ivar output_csv_path: Выходной CSV агрегатов.
    """

    model_config = ConfigDict(extra="ignore")

    source_done_csv_path: Path
    output_csv_path: Path


@dataclass(frozen=True, slots=True)
class EtlStageSizeAggregateProgress:
    """
    Progress-событие построения агрегатов.

    :ivar phase: Текущий этап: ``loading``, ``aggregating`` или ``writing``.
    """

    phase: str


@dataclass(frozen=True, slots=True)
class EtlStageSizeAggregateResult:
    """
    Результат одного запуска агрегатора по размеру и стадиям.

    :ivar source_rows: Число строк во входном CSV (до фильтрации).
    :ivar aggregate_rows: Число строк в выходном CSV агрегатов.
    :ivar excluded_unsized_rows: Строки, исключённые из-за отсутствия или
        некорректности attach_size.
    :ivar stopped_by_request: Признак отмены через stop event.
    :ivar output_csv_path: Путь записанного выходного CSV.
    """

    source_rows: int
    aggregate_rows: int
    excluded_unsized_rows: int
    stopped_by_request: bool
    output_csv_path: Path


class EtlStageSizeAggregateBuilder:
    """
    Асинхронный построитель агрегатов по размеру и FSM-стадиям.

    :param settings: Валидированные настройки агрегатора.
    :param progress_callback: Callback с progress metadata для supervisor-а.
    """

    def __init__(
        self,
        settings: EtlStageSizeAggregateSettings,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> None:
        self._settings = settings
        self._progress_callback = progress_callback

    async def run(
        self,
        stop_event: asyncio.Event,
    ) -> EtlStageSizeAggregateResult:
        """
        Построить агрегаты по DONE-статистике.

        :param stop_event: Сигнал штатной отмены задачи.
        :returns: Результат со счётчиками и путём выходного CSV.
        :raises FileNotFoundError: Если входной CSV отсутствует.
        :raises OSError: При ошибке записи выходного CSV.
        """
        if stop_event.is_set():
            self._emit_progress(EtlStageSizeAggregateProgress(phase="writing"))
            await asyncio.to_thread(self._write_empty_output)
            return EtlStageSizeAggregateResult(
                source_rows=0,
                aggregate_rows=0,
                excluded_unsized_rows=0,
                stopped_by_request=True,
                output_csv_path=self._settings.output_csv_path,
            )

        return await asyncio.to_thread(
            self._run_sync,
        )

    def _run_sync(self) -> EtlStageSizeAggregateResult:
        """
        Синхронное ядро: загрузка, агрегация, запись.

        Выполняется внутри :func:`asyncio.to_thread`.

        :returns: Результат агрегации.
        :raises FileNotFoundError: Если входной CSV отсутствует.
        :raises OSError: При ошибке записи выходного CSV.
        """
        source_path = self._settings.source_done_csv_path

        if not source_path.is_file():
            raise FileNotFoundError(f"DONE CSV not found: {source_path}")

        self._emit_progress(EtlStageSizeAggregateProgress(phase="loading"))
        source_frame = pd.read_csv(source_path)
        source_rows = len(source_frame)

        self._emit_progress(EtlStageSizeAggregateProgress(phase="aggregating"))
        aggregate_frame, excluded_unsized_rows = self._build_aggregates(
            source_frame,
        )

        self._emit_progress(EtlStageSizeAggregateProgress(phase="writing"))
        self._write_output(aggregate_frame)

        return EtlStageSizeAggregateResult(
            source_rows=source_rows,
            aggregate_rows=len(aggregate_frame),
            excluded_unsized_rows=excluded_unsized_rows,
            stopped_by_request=False,
            output_csv_path=self._settings.output_csv_path,
        )

    @staticmethod
    def _size_bytes_to_bucket_mb(size_bytes: Optional[int]) -> Optional[int]:
        """
        Отнести размер в байтах к ближайшему бакету сетки степеней двойки.

        :param size_bytes: Размер вложения в байтах.
        :returns: Бакет в МБ (1, 2, 4, ..., 1024) или ``None`` если размер
            отсутствует, неположителен или превышает сетку.
        """
        if size_bytes is None or (isinstance(size_bytes, float) and pd.isna(size_bytes)) or size_bytes <= 0:
            return None

        size_mb = size_bytes / BYTES_IN_MB

        if size_mb <= 0.0:
            return None

        exp = int(round(np.log2(size_mb)))
        exp = max(0, min(exp, 10))

        return 2 ** exp

    @staticmethod
    def _build_aggregates(
        source_frame: pd.DataFrame,
    ) -> tuple[pd.DataFrame, int]:
        """
        Сгруппировать DONE-строки и вычислить взвешенные агрегаты.

        :param source_frame: DataFrame из DONE CSV.
        :returns: Кортеж ``(aggregate_frame, excluded_unsized_rows)``.
        """
        frame = source_frame.copy()

        excluded_mask = frame["stage"].isna() | (
            frame.get("attach_type") == "page_body"
        )
        frame = frame[~excluded_mask].copy()

        frame["size_bucket_mb"] = frame["attach_size"].apply(
            EtlStageSizeAggregateBuilder._size_bytes_to_bucket_mb,
        )

        unsized_mask = frame["size_bucket_mb"].isna()
        excluded_unsized_rows = int(unsized_mask.sum())

        frame = frame[~unsized_mask].copy()

        for col in ("samples", "cpu_mean_pct", "cpu_peak_pct",
                     "rss_mean", "rss_peak", "wall_time_sec"):
            if col in frame.columns:
                frame[col] = pd.to_numeric(frame[col], errors="coerce")

        frame["samples"] = frame["samples"].fillna(0).astype(int)
        frame["size_bucket_mb"] = frame["size_bucket_mb"].astype(int)

        grouped = frame.groupby(
            ["attach_type", "size_bucket_mb", "stage"],
            dropna=True,
        )

        rows: list[dict] = []

        for (attach_type, bucket_mb, stage), group in grouped:
            row_count = len(group)

            total_samples = int(group["samples"].sum())

            total_wall_time_sec = group["wall_time_sec"].sum()
            total_wall_time_sec = (
                float(total_wall_time_sec)
                if pd.notna(total_wall_time_sec)
                else 0.0
            )

            weighted_mean_cpu = None
            peak_cpu = None
            weighted_mean_rss = None
            peak_rss = None

            weighted_mask = group["samples"] > 0

            if "cpu_mean_pct" in group.columns:
                cpu_weighted = group.loc[
                    weighted_mask
                    & group["cpu_mean_pct"].notna()
                ]
                if len(cpu_weighted) > 0:
                    cpu_sum = (
                        cpu_weighted["cpu_mean_pct"]
                        * cpu_weighted["samples"]
                    ).sum()
                    cpu_weight_sum = cpu_weighted["samples"].sum()
                    if cpu_weight_sum > 0:
                        weighted_mean_cpu = float(cpu_sum / cpu_weight_sum)

                peak_cpu_vals = group["cpu_peak_pct"].dropna()
                if len(peak_cpu_vals) > 0:
                    peak_cpu = float(peak_cpu_vals.max())

            if "rss_mean" in group.columns:
                rss_weighted = group.loc[
                    weighted_mask
                    & group["rss_mean"].notna()
                ]
                if len(rss_weighted) > 0:
                    rss_sum = (
                        rss_weighted["rss_mean"]
                        * rss_weighted["samples"]
                    ).sum()
                    rss_weight_sum = rss_weighted["samples"].sum()
                    if rss_weight_sum > 0:
                        weighted_mean_rss = float(rss_sum / rss_weight_sum)

                peak_rss_vals = group["rss_peak"].dropna()
                if len(peak_rss_vals) > 0:
                    peak_rss = float(peak_rss_vals.max())

            rows.append({
                "attach_type": str(attach_type),
                "size_bucket_mb": int(bucket_mb),
                "stage": str(stage),
                "row_count": row_count,
                "total_samples": total_samples,
                "total_wall_time_sec": total_wall_time_sec,
                "weighted_mean_cpu_pct": weighted_mean_cpu,
                "peak_cpu_pct": peak_cpu,
                "weighted_mean_rss_bytes": weighted_mean_rss,
                "peak_rss_bytes": peak_rss,
            })

        if not rows:
            return pd.DataFrame(columns=OUTPUT_COLUMNS), excluded_unsized_rows

        aggregate_frame = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
        aggregate_frame.sort_values(
            by=["attach_type", "size_bucket_mb", "stage"],
            inplace=True,
        )
        aggregate_frame.reset_index(drop=True, inplace=True)

        return aggregate_frame, excluded_unsized_rows

    def _write_output(self, aggregate_frame: pd.DataFrame) -> None:
        """
        Записать CSV агрегатов.

        :param aggregate_frame: DataFrame с агрегатами.
        :raises OSError: При ошибке записи.
        """
        output_path = self._settings.output_csv_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        aggregate_frame.to_csv(output_path, index=False, encoding="utf-8")

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
        progress: EtlStageSizeAggregateProgress,
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
                "ETL stage size aggregate progress callback failed"
            )