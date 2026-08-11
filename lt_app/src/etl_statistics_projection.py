# etl_statistics_projection.py
"""
Построение регрессионных моделей и прогнозов по статистике ETL-обработки.

Модуль читает DONE-статистику, созданную
:class:`EtlReportStatisticsBuilder`, и строит линейные регрессионные модели
для каждой пары ``(attach_type, stage)``.

Строятся две модели:

- ``wall_time_sec ~ a_time * size_mb + b_time``;
- ``rss_peak ~ a_rss * size_mb + b_rss``.

Для каждого набора целевых размеров создаются:

- прогноз по каждой FSM-стадии;
- агрегированный прогноз по типу вложения;
- метка extrapolation, если целевой размер лежит вне фактического диапазона.

Модуль является конечной аналитической задачей тестовой сессии. Он должен
запускаться после :class:`EtlReportStatisticsBuilder` и не содержит CLI,
Streamlit UI, multiprocessing orchestration или запуска приложения.

Настройки загружаются через Pydantic Settings из environment variables
или локального ``.env`` с префиксом ``ETL_STATISTICS_PROJECTION_``.

.. warning::

   Прогноз является статистической оценкой, а не гарантией производительности.
   Модели с малым числом наблюдений, низким R² или extrapolation за пределы
   фактических размеров должны быть явно помечены в итоговом отчёте.

.. Как читать результаты
coefficients.csv — сырые модели.
slope для time — секунды на MB, для memory — bytes на MB.
projection_by_stage.csv — прогноз на каждый target size и каждую FSM-стадию.
projection_total.csv:
    total_wall_time_sec — сумма прогнозов стадий;
    max_rss_peak_mb — максимум RSS среди стадий, не сумма;
    extrapolated_any=true — хотя бы одна модель строит прогноз вне своего observed range;
    has_low_confidence_model=true — есть fallback по одной точке или модель вовсе недоступна.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Optional

import numpy as np
import pandas as pd
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


logger = logging.getLogger(__name__)

BYTES_IN_MB = 1024 * 1024

REQUIRED_DONE_COLUMNS: frozenset[str] = frozenset(
    {
        "attach_type",
        "attach_size",
        "stage",
        "wall_time_sec",
    }
)

COEFFICIENT_COLUMNS: tuple[str, ...] = (
    "attach_type",
    "stage",
    "metric",
    "fit_mode",
    "n_samples",
    "size_mb_min",
    "size_mb_max",
    "slope",
    "intercept",
    "r2",
)

STAGE_PROJECTION_COLUMNS: tuple[str, ...] = (
    "attach_type",
    "stage",
    "target_size_mb",
    "metric",
    "n_samples_source",
    "observed_size_mb_min",
    "observed_size_mb_max",
    "extrapolated",
    "fit_mode",
    "r2",
    "predicted_wall_time_sec",
    "predicted_rss_peak_mb",
)

TOTAL_PROJECTION_COLUMNS: tuple[str, ...] = (
    "attach_type",
    "target_size_mb",
    "stage_count",
    "total_wall_time_sec",
    "max_rss_peak_mb",
    "extrapolated_any",
    "has_low_confidence_model",
)


ProgressCallback = Callable[["EtlProjectionProgress"], None]


class LinearFitMode(StrEnum):
    """Режим построения линейной модели."""

    LEAST_SQUARES = "least_squares"
    ORIGIN_FALLBACK = "origin_fallback"
    UNAVAILABLE = "unavailable"


class ProjectionMetric(StrEnum):
    """Метрика, для которой строится регрессионная модель."""

    WALL_TIME_SEC = "wall_time_sec"
    RSS_PEAK_BYTES = "rss_peak_bytes"


class EtlStatisticsProjectionSettings(BaseSettings):
    """
    Настройки построения прогнозов ETL-обработки.

    Список целевых размеров в ``.env`` задаётся JSON-массивом, например::

        ETL_STATISTICS_PROJECTION_TARGET_SIZES_MB=[1,5,10,50,100,500]

    :ivar source_done_csv_path: CSV со статистикой DONE-стадий.
    :ivar coefficients_output_csv_path: Выходной CSV коэффициентов моделей.
    :ivar stage_projection_output_csv_path: Выходной CSV прогнозов по стадиям.
    :ivar total_projection_output_csv_path: Выходной CSV агрегированных
        прогнозов по типам вложений.
    :ivar target_sizes_mb: Целевые размеры файлов для прогнозирования.
    :ivar allow_single_point_origin_fit: Разрешить грубую модель вида
        ``y = a * x`` при единственной точке или нулевом разбросе size.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="ETL_STATISTICS_PROJECTION_",
        extra="ignore",
    )

    source_done_csv_path: Path
    coefficients_output_csv_path: Path
    stage_projection_output_csv_path: Path
    total_projection_output_csv_path: Path

    target_sizes_mb: list[float] = Field(
        default_factory=lambda: [
            1.0,
            5.0,
            10.0,
            25.0,
            50.0,
            100.0,
            200.0,
            500.0,
            1000.0,
        ]
    )

    allow_single_point_origin_fit: bool = True
    clamp_negative_predictions: bool = True

    @field_validator("target_sizes_mb")
    @classmethod
    def validate_target_sizes_mb(
        cls,
        value: list[float],
    ) -> list[float]:
        """
        Проверить и нормализовать целевые размеры прогноза.

        :param value: Размеры вложений в мегабайтах.
        :returns: Уникальные положительные размеры в возрастающем порядке.
        :raises ValueError: Если список пуст или содержит неположительное число.
        """
        normalized_values = sorted({float(size) for size in value})

        if not normalized_values:
            raise ValueError("target_sizes_mb must not be empty")

        if any(size <= 0 for size in normalized_values):
            raise ValueError("target_sizes_mb values must be greater than zero")

        return normalized_values

    @property
    def output_paths(self) -> tuple[Path, Path, Path]:
        """
        Вернуть все выходные CSV-артефакты модуля.

        :returns: Пути coefficients, stage projection и total projection CSV.
        """
        return (
            self.coefficients_output_csv_path,
            self.stage_projection_output_csv_path,
            self.total_projection_output_csv_path,
        )


@dataclass(frozen=True, slots=True)
class LinearRegressionFit:
    """
    Результат линейной регрессии одной метрики.

    :ivar metric: Моделируемая метрика.
    :ivar mode: Способ построения модели.
    :ivar n_samples: Число валидных точек наблюдений.
    :ivar size_mb_min: Минимальный размер из обучающей выборки.
    :ivar size_mb_max: Максимальный размер из обучающей выборки.
    :ivar slope: Коэффициент ``a`` в модели ``y = a * x + b``.
    :ivar intercept: Коэффициент ``b`` в модели ``y = a * x + b``.
    :ivar r2: Коэффициент детерминации или ``None``.
    """

    metric: ProjectionMetric
    mode: LinearFitMode
    n_samples: int
    size_mb_min: Optional[float]
    size_mb_max: Optional[float]
    slope: Optional[float]
    intercept: Optional[float]
    r2: Optional[float]

    @property
    def is_available(self) -> bool:
        """
        Проверить, пригодна ли модель для прогноза.

        :returns: ``True``, если рассчитаны slope и intercept.
        """
        return self.slope is not None and self.intercept is not None


@dataclass(frozen=True, slots=True)
class EtlStageModel:
    """
    Набор time и memory моделей для одной FSM-стадии и типа вложения.

    :ivar attach_type: Тип вложения.
    :ivar stage: Имя FSM-стадии.
    :ivar time_fit: Модель wall time.
    :ivar memory_fit: Модель peak RSS.
    """

    attach_type: str
    stage: str
    time_fit: LinearRegressionFit
    memory_fit: LinearRegressionFit


@dataclass(frozen=True, slots=True)
class EtlProjectionProgress:
    """
    Progress-событие построителя прогнозов.

    :ivar phase: Этап ``loading``, ``fitting``, ``projecting`` или ``writing``.
    :ivar model_count: Число созданных stage-моделей.
    :ivar stage_projection_rows: Число строк stage projection.
    """

    phase: str
    model_count: int
    stage_projection_rows: int


@dataclass(frozen=True, slots=True)
class EtlStatisticsProjectionResult:
    """
    Результат построения регрессионных прогнозов.

    :ivar source_rows: Валидные DONE-строки, использованные как наблюдения.
    :ivar stage_models: Число построенных моделей ``(attach_type, stage)``.
    :ivar coefficient_rows: Число строк в coefficients CSV.
    :ivar stage_projection_rows: Число строк в stage projection CSV.
    :ivar total_projection_rows: Число строк в total projection CSV.
    :ivar stopped_by_request: Признак остановки через stop event.
    """

    source_rows: int
    stage_models: int
    coefficient_rows: int
    stage_projection_rows: int
    total_projection_rows: int
    stopped_by_request: bool
    coefficients_output_csv_path: Path
    stage_projection_output_csv_path: Path
    total_projection_output_csv_path: Path


class EtlStatisticsProjectionBuilder:
    """
    Построитель линейных моделей и прогнозов по DONE-статистике ETL.

    :param settings: Валидированные настройки модуля.
    :param progress_callback: Callback с progress metadata для supervisor-а.
    """

    def __init__(
        self,
        settings: EtlStatisticsProjectionSettings,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> None:
        self._settings = settings
        self._progress_callback = progress_callback

    async def run(
        self,
        stop_event: asyncio.Event,
    ) -> EtlStatisticsProjectionResult:
        """
        Создать coefficients, stage projection и total projection CSV.

        Поскольку pandas и numpy синхронные, чтение CSV, fit и запись
        артефактов выполняются вне asyncio event loop через
        :func:`asyncio.to_thread`.

        :param stop_event: Сигнал штатной отмены задачи.
        :returns: Счётчики и пути созданных CSV.
        :raises FileNotFoundError: Если source DONE CSV отсутствует.
        :raises ValueError: Если source CSV не содержит нужных колонок.
        :raises OSError: При ошибке записи выходных CSV.
        """
        if stop_event.is_set():
            return self._build_stopped_result()

        self._emit_progress(
            EtlProjectionProgress(
                phase="loading",
                model_count=0,
                stage_projection_rows=0,
            )
        )

        source_frame = await asyncio.to_thread(self._load_done_statistics)

        if stop_event.is_set():
            return self._build_stopped_result(source_rows=len(source_frame))

        self._emit_progress(
            EtlProjectionProgress(
                phase="fitting",
                model_count=0,
                stage_projection_rows=0,
            )
        )

        stage_models = await asyncio.to_thread(
            self._build_stage_models,
            source_frame,
        )

        if stop_event.is_set():
            return self._build_stopped_result(
                source_rows=len(source_frame),
                stage_models=len(stage_models),
            )

        self._emit_progress(
            EtlProjectionProgress(
                phase="projecting",
                model_count=len(stage_models),
                stage_projection_rows=0,
            )
        )

        coefficient_frame = await asyncio.to_thread(
            self._build_coefficients_frame,
            stage_models,
        )
        stage_projection_frame = await asyncio.to_thread(
            self._build_stage_projection_frame,
            stage_models,
        )
        total_projection_frame = await asyncio.to_thread(
            self._build_total_projection_frame,
            stage_projection_frame,
        )

        self._emit_progress(
            EtlProjectionProgress(
                phase="writing",
                model_count=len(stage_models),
                stage_projection_rows=len(stage_projection_frame),
            )
        )

        await asyncio.to_thread(
            self._write_output_artifacts,
            coefficient_frame,
            stage_projection_frame,
            total_projection_frame,
        )

        return EtlStatisticsProjectionResult(
            source_rows=len(source_frame),
            stage_models=len(stage_models),
            coefficient_rows=len(coefficient_frame),
            stage_projection_rows=len(stage_projection_frame),
            total_projection_rows=len(total_projection_frame),
            stopped_by_request=False,
            coefficients_output_csv_path=(
                self._settings.coefficients_output_csv_path
            ),
            stage_projection_output_csv_path=(
                self._settings.stage_projection_output_csv_path
            ),
            total_projection_output_csv_path=(
                self._settings.total_projection_output_csv_path
            ),
        )

    def _load_done_statistics(self) -> pd.DataFrame:
        """
        Прочитать и нормализовать DONE-статистику предыдущего модуля.

        В обучение попадают только строки с заполненными stage, wall time,
        attachment size и корректным положительным size_mb.

        :returns: Валидные наблюдения с добавленной колонкой ``size_mb``.
        :raises FileNotFoundError: Если входной CSV отсутствует.
        :raises ValueError: Если CSV не содержит обязательные колонки.
        """
        source_path = self._settings.source_done_csv_path

        if not source_path.is_file():
            raise FileNotFoundError(f"DONE CSV not found: {source_path}")

        source_frame = pd.read_csv(source_path)

        missing_columns = REQUIRED_DONE_COLUMNS.difference(source_frame.columns)
        if missing_columns:
            raise ValueError(
                f"DONE CSV misses required columns: {sorted(missing_columns)}"
            )

        normalized_frame = source_frame.copy()

        for column in ("attach_size", "wall_time_sec", "rss_peak"):
            if column in normalized_frame.columns:
                normalized_frame[column] = pd.to_numeric(
                    normalized_frame[column],
                    errors="coerce",
                )

        normalized_frame = normalized_frame[
            normalized_frame["stage"].notna()
            & normalized_frame["attach_type"].notna()
            & normalized_frame["attach_size"].notna()
            & normalized_frame["wall_time_sec"].notna()
        ].copy()

        normalized_frame["size_mb"] = (
            normalized_frame["attach_size"] / BYTES_IN_MB
        )

        normalized_frame = normalized_frame[
            np.isfinite(normalized_frame["size_mb"])
            & np.isfinite(normalized_frame["wall_time_sec"])
            & (normalized_frame["size_mb"] > 0)
        ].copy()

        return normalized_frame

    def _build_stage_models(
        self,
        source_frame: pd.DataFrame,
    ) -> list[EtlStageModel]:
        """
        Построить time и memory модели для каждой пары type/stage.

        :param source_frame: Валидные DONE-наблюдения.
        :returns: Список stage-моделей.
        """
        models: list[EtlStageModel] = []

        for (attach_type, stage), group in source_frame.groupby(
            ["attach_type", "stage"],
            dropna=True,
        ):
            time_fit = self._fit_linear(
                metric=ProjectionMetric.WALL_TIME_SEC,
                size_mb=group["size_mb"],
                values=group["wall_time_sec"],
            )

            memory_fit = self._fit_linear(
                metric=ProjectionMetric.RSS_PEAK_BYTES,
                size_mb=group["size_mb"],
                values=group["rss_peak"]
                if "rss_peak" in group.columns
                else pd.Series(dtype=float),
            )

            models.append(
                EtlStageModel(
                    attach_type=str(attach_type),
                    stage=str(stage),
                    time_fit=time_fit,
                    memory_fit=memory_fit,
                )
            )

        return sorted(
            models,
            key=lambda model: (model.attach_type, model.stage),
        )

    def _fit_linear(
        self,
        metric: ProjectionMetric,
        size_mb: pd.Series,
        values: pd.Series,
    ) -> LinearRegressionFit:
        """
        Построить линейную модель ``value = slope * size_mb + intercept``.

        Если имеется как минимум две точки с разными размерам, используется
        least squares regression. При единственной точке или одинаковом
        размере допускается fallback ``y = a * x`` при включённой настройке
        ``allow_single_point_origin_fit``.

        :param metric: Метрика модели.
        :param size_mb: Размеры исходных вложений в MB.
        :param values: Наблюдения целевой метрики.
        :returns: Результат регрессии или unavailable-модель.
        """
        x = np.asarray(size_mb, dtype=float)
        y = np.asarray(values, dtype=float)

        valid_mask = np.isfinite(x) & np.isfinite(y)
        x = x[valid_mask]
        y = y[valid_mask]

        if len(x) == 0:
            return self._build_unavailable_fit(metric)

        size_mb_min = float(np.min(x))
        size_mb_max = float(np.max(x))

        if len(x) >= 2 and not np.allclose(x, x[0]):
            slope, intercept = np.polyfit(x, y, 1)
            prediction = slope * x + intercept

            ss_residual = float(np.sum((y - prediction) ** 2))
            ss_total = float(np.sum((y - np.mean(y)) ** 2))

            r2 = (
                1.0 - ss_residual / ss_total
                if ss_total > 0
                else None
            )

            return LinearRegressionFit(
                metric=metric,
                mode=LinearFitMode.LEAST_SQUARES,
                n_samples=len(x),
                size_mb_min=size_mb_min,
                size_mb_max=size_mb_max,
                slope=float(slope),
                intercept=float(intercept),
                r2=r2,
            )

        if (
            self._settings.allow_single_point_origin_fit
            and float(np.mean(x)) > 0
        ):
            slope = float(np.mean(y) / np.mean(x))

            return LinearRegressionFit(
                metric=metric,
                mode=LinearFitMode.ORIGIN_FALLBACK,
                n_samples=len(x),
                size_mb_min=size_mb_min,
                size_mb_max=size_mb_max,
                slope=slope,
                intercept=0.0,
                r2=None,
            )

        return self._build_unavailable_fit(
            metric=metric,
            n_samples=len(x),
            size_mb_min=size_mb_min,
            size_mb_max=size_mb_max,
        )

    @staticmethod
    def _build_unavailable_fit(
        metric: ProjectionMetric,
        n_samples: int = 0,
        size_mb_min: Optional[float] = None,
        size_mb_max: Optional[float] = None,
    ) -> LinearRegressionFit:
        """
        Создать unavailable-модель.

        :param metric: Метрика без достаточных данных.
        :param n_samples: Число валидных наблюдений.
        :param size_mb_min: Минимальный observed size.
        :param size_mb_max: Максимальный observed size.
        :returns: Объект unavailable-модели.
        """
        return LinearRegressionFit(
            metric=metric,
            mode=LinearFitMode.UNAVAILABLE,
            n_samples=n_samples,
            size_mb_min=size_mb_min,
            size_mb_max=size_mb_max,
            slope=None,
            intercept=None,
            r2=None,
        )

    @staticmethod
    def _build_coefficients_frame(
        stage_models: list[EtlStageModel],
    ) -> pd.DataFrame:
        """
        Преобразовать модели в таблицу коэффициентов.

        :param stage_models: Построенные stage-модели.
        :returns: DataFrame коэффициентов time и memory моделей.
        """
        rows: list[dict[str, Any]] = []

        for model in stage_models:
            for fit in (model.time_fit, model.memory_fit):
                rows.append(
                    {
                        "attach_type": model.attach_type,
                        "stage": model.stage,
                        "metric": fit.metric.value,
                        "fit_mode": fit.mode.value,
                        "n_samples": fit.n_samples,
                        "size_mb_min": fit.size_mb_min,
                        "size_mb_max": fit.size_mb_max,
                        "slope": fit.slope,
                        "intercept": fit.intercept,
                        "r2": fit.r2,
                    }
                )

        return pd.DataFrame(rows, columns=COEFFICIENT_COLUMNS)

    def _build_stage_projection_frame(
        self,
        stage_models: list[EtlStageModel],
    ) -> pd.DataFrame:
        """
        Построить прогнозы по каждой stage-модели и целевому размеру.

        :param stage_models: Построенные time и memory модели.
        :returns: DataFrame прогнозов по FSM-стадиям.
        """
        rows: list[dict[str, Any]] = []

        for model in stage_models:
            for target_size_mb in self._settings.target_sizes_mb:
                time_prediction = self._predict(
                    fit=model.time_fit,
                    target_size_mb=target_size_mb,
                )
                memory_prediction_bytes = self._predict(
                    fit=model.memory_fit,
                    target_size_mb=target_size_mb,
                )

                observed_min = model.time_fit.size_mb_min
                observed_max = model.time_fit.size_mb_max

                rows.append(
                    {
                        "attach_type": model.attach_type,
                        "stage": model.stage,
                        "target_size_mb": target_size_mb,
                        "metric": "time_and_memory",
                        "n_samples_source": model.time_fit.n_samples,
                        "observed_size_mb_min": observed_min,
                        "observed_size_mb_max": observed_max,
                        "extrapolated": self._is_extrapolated(
                            target_size_mb=target_size_mb,
                            observed_min=observed_min,
                            observed_max=observed_max,
                        ),
                        "fit_mode": model.time_fit.mode.value,
                        "r2": model.time_fit.r2,
                        "predicted_wall_time_sec": time_prediction,
                        "predicted_rss_peak_mb": (
                            memory_prediction_bytes / BYTES_IN_MB
                            if memory_prediction_bytes is not None
                            else None
                        ),
                    }
                )

        return pd.DataFrame(rows, columns=STAGE_PROJECTION_COLUMNS)

    def _predict(
        self,
        fit: LinearRegressionFit,
        target_size_mb: float,
    ) -> Optional[float]:
        """
        Рассчитать прогноз по линейной модели.

        :param fit: Построенная линейная модель.
        :param target_size_mb: Целевой размер вложения в MB.
        :returns: Прогноз метрики либо ``None`` без пригодной модели.
        """
        if not fit.is_available:
            return None

        prediction = (
            float(fit.slope) * target_size_mb
            + float(fit.intercept)
        )

        if self._settings.clamp_negative_predictions:
            return max(0.0, prediction)

        return prediction

    @staticmethod
    def _is_extrapolated(
        target_size_mb: float,
        observed_min: Optional[float],
        observed_max: Optional[float],
    ) -> bool:
        """
        Проверить выход целевого размера за observed training range.

        :param target_size_mb: Размер прогноза в MB.
        :param observed_min: Минимальный size в обучающей выборке.
        :param observed_max: Максимальный size в обучающей выборке.
        :returns: ``True``, если прогноз является экстраполяцией.
        """
        if observed_min is None or observed_max is None:
            return True

        return (
            target_size_mb < observed_min
            or target_size_mb > observed_max
        )

    @staticmethod
    def _build_total_projection_frame(
        stage_projection_frame: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Агрегировать stage прогнозы в модель уровня типа вложения.

        ``total_wall_time_sec`` — сумма времени последовательных FSM-стадий.
        ``max_rss_peak_mb`` — максимум stage peak RSS, а не сумма памяти,
        поскольку стадии обычно выполняются последовательно.

        :param stage_projection_frame: Прогнозы по отдельным FSM-стадиям.
        :returns: DataFrame агрегированных прогнозов.
        """
        if stage_projection_frame.empty:
            return pd.DataFrame(columns=TOTAL_PROJECTION_COLUMNS)

        total_frame = (
            stage_projection_frame.groupby(
                ["attach_type", "target_size_mb"],
                dropna=False,
            )
            .agg(
                stage_count=("stage", "count"),
                total_wall_time_sec=(
                    "predicted_wall_time_sec",
                    lambda values: values.sum(min_count=1),
                ),
                max_rss_peak_mb=("predicted_rss_peak_mb", "max"),
                extrapolated_any=("extrapolated", "any"),
                has_low_confidence_model=(
                    "fit_mode",
                    lambda modes: (
                        LinearFitMode.ORIGIN_FALLBACK.value in set(modes)
                        or LinearFitMode.UNAVAILABLE.value in set(modes)
                    ),
                ),
            )
            .reset_index()
        )

        return total_frame.reindex(columns=TOTAL_PROJECTION_COLUMNS)

    def _write_output_artifacts(
        self,
        coefficient_frame: pd.DataFrame,
        stage_projection_frame: pd.DataFrame,
        total_projection_frame: pd.DataFrame,
    ) -> None:
        """
        Записать все projection CSV-артефакты.

        :param coefficient_frame: Таблица регрессионных коэффициентов.
        :param stage_projection_frame: Прогнозы по FSM-стадиям.
        :param total_projection_frame: Итоговые прогнозы по типам вложений.
        :raises OSError: При ошибке записи выходного CSV.
        """
        for output_path in self._settings.output_paths:
            output_path.parent.mkdir(parents=True, exist_ok=True)

        coefficient_frame.to_csv(
            self._settings.coefficients_output_csv_path,
            index=False,
            encoding="utf-8",
        )
        stage_projection_frame.to_csv(
            self._settings.stage_projection_output_csv_path,
            index=False,
            encoding="utf-8",
        )
        total_projection_frame.to_csv(
            self._settings.total_projection_output_csv_path,
            index=False,
            encoding="utf-8",
        )

    def _build_stopped_result(
        self,
        source_rows: int = 0,
        stage_models: int = 0,
    ) -> EtlStatisticsProjectionResult:
        """
        Сформировать результат штатно отменённой задачи.

        :param source_rows: Уже загруженные строки исходной статистики.
        :param stage_models: Уже построенные stage-модели.
        :returns: Результат с ``stopped_by_request=True``.
        """
        return EtlStatisticsProjectionResult(
            source_rows=source_rows,
            stage_models=stage_models,
            coefficient_rows=0,
            stage_projection_rows=0,
            total_projection_rows=0,
            stopped_by_request=True,
            coefficients_output_csv_path=(
                self._settings.coefficients_output_csv_path
            ),
            stage_projection_output_csv_path=(
                self._settings.stage_projection_output_csv_path
            ),
            total_projection_output_csv_path=(
                self._settings.total_projection_output_csv_path
            ),
        )

    def _emit_progress(
        self,
        progress: EtlProjectionProgress,
    ) -> None:
        """
        Передать progress metadata внешнему supervisor-у.

        :param progress: Состояние текущего этапа построения прогноза.
        """
        if self._progress_callback is None:
            return

        try:
            self._progress_callback(progress)
        except Exception:
            logger.exception("ETL projection progress callback failed")
