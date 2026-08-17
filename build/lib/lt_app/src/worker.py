# worker.py
"""
Supervisor жизненного цикла одной тестовой сессии нагрузочного тестирования.

Worker запускает модули генерации, мониторинга и сбора артефактов параллельно,
по завершении генератора последовательно выполняет post-processing: построение
статистики ETL-отчётов и регрессионных прогнозов.

Модуль не содержит Streamlit UI, CLI, argparse или multiprocessing-кода.
Внешний вызывающий код (app.py) создаёт настройки модулей, очередь прогресса
и запускает :meth:`SessionWorker.run` в отдельной asyncio-задаче. Для остановки
сессии вызывающий код использует метод :meth:`SessionWorker.stop`.

Артефакты изолируются по ``session_id`` в ``artifacts/<session_id>/``.
"""

from __future__ import annotations

import asyncio
import logging
import queue
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from lt_app.src.etl_report_statistics import (
    EtlReportStatisticsBuilder,
    EtlReportStatisticsProgress,
    EtlReportStatisticsResult,
    EtlReportStatisticsSettings,
)
from lt_app.src.etl_stage_size_aggregates import (
    EtlStageSizeAggregateBuilder,
    EtlStageSizeAggregateResult,
    EtlStageSizeAggregateSettings,
)
from lt_app.src.etl_statistics_projection import (
    EtlProjectionProgress,
    EtlStatisticsProjectionBuilder,
    EtlStatisticsProjectionResult,
    EtlStatisticsProjectionSettings,
)
from lt_app.src.etl_throughput_aggregates import (
    EtlThroughputAggregateBuilder,
    EtlThroughputAggregateResult,
    EtlThroughputAggregateSettings,
)
from lt_app.src.kafka_event_generator import (
    KafkaEventGenerator,
    KafkaEventGeneratorSettings,
    KafkaGeneratorProgress,
    KafkaGeneratorResult,
)
from lt_app.src.kafka_json_artifact_collector import (
    KafkaArtifactCollectorResult,
    KafkaArtifactCollectorSettings,
    KafkaArtifactSaved,
    KafkaJsonArtifactCollector,
)
from lt_app.src.opensearch_index_monitor import (
    OpenSearchIndexCount,
    OpenSearchIndexMonitor,
    OpenSearchIndexMonitorResult,
    OpenSearchIndexMonitorSettings,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SessionProgress:
    """
    Компактное событие прогресса сессии для Streamlit UI.

    Передаётся через :class:`asyncio.Queue` в UI-слой. Содержит только
    metadata и агрегаты, но не payload, secrets или содержимое артефактов.

    :ivar session_id: Идентификатор тестовой сессии.
    :ivar phase: Текущая фаза сессии: ``running``, ``statistics``,
        ``projection``, ``done``, ``failed``.
    :ivar generator: Последний прогресс генератора или ``None``.
    :ivar monitor: Последнее измерение OpenSearch или ``None``.
    :ivar collector: Последний сохранённый артефакт или ``None``.
    :ivar statistics: Прогресс построения статистики или ``None``.
    :ivar projection: Прогресс построения прогнозов или ``None``.
    :ivar stage_size_aggregates: Результат периодического Stage/Size snapshot
        или ``None``.
    :ivar throughput_aggregates: Результат периодического Throughput snapshot
        или ``None``.
    :ivar error: Сообщение об ошибке фазы или ``None``.
    :ivar timestamp: UTC-время создания события.
    """

    session_id: str
    phase: str
    generator: Optional[KafkaGeneratorProgress] = None
    monitor: Optional[OpenSearchIndexCount] = None
    collector: Optional[KafkaArtifactSaved] = None
    statistics: Optional[EtlReportStatisticsProgress] = None
    projection: Optional[EtlProjectionProgress] = None
    stage_size_aggregates: Optional[EtlStageSizeAggregateResult] = None
    throughput_aggregates: Optional[EtlThroughputAggregateResult] = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True, slots=True)
class SessionResult:
    """
    Итоговый результат тестовой сессии.

    Содержит результаты всех модулей, дошедших до завершения. Модули,
    которые не запускались из-за ошибки на предыдущей фазе, остаются ``None``.

    :ivar session_id: Идентификатор тестовой сессии.
    :ivar generator: Результат генератора событий.
    :ivar monitor: Результат монитора OpenSearch.
    :ivar collector: Результат сборщика артефактов.
    :ivar statistics: Результат построения статистики.
    :ivar projection: Результат построения прогнозов.
    :ivar stage_size_aggregates: Результат Stage/Size агрегатов или ``None``.
    :ivar throughput_aggregates: Результат Throughput агрегатов или ``None``.
    :ivar error: Описание критической ошибки или ``None``.
    :ivar artifacts_dir: Каталог артефактов сессии.
    """

    session_id: str
    generator: Optional[KafkaGeneratorResult] = None
    monitor: Optional[OpenSearchIndexMonitorResult] = None
    collector: Optional[KafkaArtifactCollectorResult] = None
    statistics: Optional[EtlReportStatisticsResult] = None
    projection: Optional[EtlStatisticsProjectionResult] = None
    stage_size_aggregates: Optional[EtlStageSizeAggregateResult] = None
    throughput_aggregates: Optional[EtlThroughputAggregateResult] = None
    error: Optional[str] = None
    artifacts_dir: Optional[Path] = None


class SessionWorker:
    """
    Supervisor одной тестовой сессии нагрузочного тестирования.

    Создаёт экземпляры пяти модулей из переданных настроек и управляет
    их жизненным циклом: параллельный запуск (фаза 1), затем последовательный
    post-processing (фазы 2 и 3).

    :param generator_settings: Настройки Kafka event generator.
    :param monitor_settings: Настройки OpenSearch index monitor.
    :param collector_settings: Настройки Kafka artifact collector.
    :param statistics_settings: Настройки построения статистики ETL-отчётов.
    :param projection_settings: Настройки построения регрессионных прогнозов.
    :param stage_size_settings: Настройки Stage/Size агрегатов.
    :param throughput_settings: Настройки Throughput агрегатов.
    :param session_id: Идентификатор тестовой сессии.
    :param progress_queue: Очередь для передачи событий прогресса в UI.
    :param report_interval_sec: Интервал периодических snapshot (60–3600).
    """

    def __init__(
        self,
        run_settings: dict[str, dict[str, Any]],
        session_id: str,
        progress_queue: queue.Queue[SessionProgress],
        report_interval_sec: float,
    ) -> None:
        self._session_id = session_id
        self._progress_queue = progress_queue
        self._report_interval_sec = report_interval_sec

        rs_ = run_settings

        self._generator = KafkaEventGenerator(
            settings=KafkaEventGeneratorSettings(**rs_.get("generator", {})),
            progress_callback=self._on_generator_progress,
        )
        self._monitor = OpenSearchIndexMonitor(
            settings=OpenSearchIndexMonitorSettings(**rs_.get("monitor", {})),
            progress_callback=self._on_monitor_progress,
        )
        self._collector = KafkaJsonArtifactCollector(
            settings=KafkaArtifactCollectorSettings(**rs_.get("collector", {})),
            progress_callback=self._on_collector_progress,
        )
        self._statistics = EtlReportStatisticsBuilder(
            settings=EtlReportStatisticsSettings(**rs_.get("statistics", {})),
            progress_callback=self._on_statistics_progress,
        )
        self._projection = EtlStatisticsProjectionBuilder(
            settings=EtlStatisticsProjectionSettings(**rs_.get("projection", {})),
            progress_callback=self._on_projection_progress,
        )
        self._stage_size = EtlStageSizeAggregateBuilder(
            settings=EtlStageSizeAggregateSettings(**rs_.get("stage_size", {})),
        )
        self._throughput = EtlThroughputAggregateBuilder(
            settings=EtlThroughputAggregateSettings(**rs_.get("throughput", {})),
        )

        self._stop_event = asyncio.Event()
        self._latest_sent_messages: int = 0

    def stop(self) -> None:
        """Сигнализировать всем запущенным модулям о необходимости штатной остановки."""
        self._stop_event.set()

    async def run(self) -> SessionResult:
        """
        Запустить полный жизненный цикл тестовой сессии.

        Фаза 1: параллельно generator, monitor, collector с периодическими
        snapshot-ами агрегатов.
        Фаза 2: последовательно statistics builder.
        Фаза 3: последовательно projection builder.
        Фаза 4: последовательно stage_size_aggregates builder.
        Фаза 5: последовательно throughput_aggregates builder.

        :returns: Итоговый результат сессии с результатами всех выполненных фаз.
        """
        logger.info("Session %s: starting", self._session_id)

        try:
            snapshot_task = asyncio.create_task(
                self._periodic_snapshot_loop(),
                name="periodic_snapshot",
            )
            try:
                gen_result, mon_result, col_result = await self._run_phase1()
            finally:
                snapshot_task.cancel()
                try:
                    await snapshot_task
                except asyncio.CancelledError:
                    pass

            postprocessing_stop_event = asyncio.Event()

            self._emit_progress(phase="statistics")
            stats_result = await self._statistics.run(postprocessing_stop_event)

            self._emit_progress(phase="projection")
            proj_result = await self._projection.run(postprocessing_stop_event)

            self._emit_progress(phase="stage_size_aggregates")
            stage_result = await self._stage_size.run(postprocessing_stop_event)

            self._emit_progress(phase="throughput_aggregates")
            throughput_result = await self._throughput.run(
                postprocessing_stop_event,
                sent_messages=gen_result.sent_messages if gen_result else 0,
            )

            result = SessionResult(
                session_id=self._session_id,
                generator=gen_result,
                monitor=mon_result,
                collector=col_result,
                statistics=stats_result,
                projection=proj_result,
                stage_size_aggregates=stage_result,
                throughput_aggregates=throughput_result,
                artifacts_dir=col_result.output_directory
                if col_result
                else None,
            )
            self._emit_progress(phase="done")
            logger.info("Session %s: completed", self._session_id)
            return result

        except Exception as exc:
            logger.exception("Session %s: failed", self._session_id)
            self._emit_progress(phase="failed", error=str(exc))
            return SessionResult(
                session_id=self._session_id,
                error=f"{type(exc).__name__}: {exc}",
            )

    async def _run_phase1(
        self,
    ) -> tuple[
        Optional[KafkaGeneratorResult],
        Optional[OpenSearchIndexMonitorResult],
        Optional[KafkaArtifactCollectorResult],
    ]:
        """
        Запустить генератор, монитор и collector параллельно.

        Ожидается, что первым завершится generator (по max_messages или
        окончанию CSV). Если monitor или collector завершаются первыми —
        это ошибка сессии.

        :returns: Кортеж результатов трёх модулей фазы 1.
        :raises RuntimeError: Если monitor или collector завершились раньше generator.
        """
        self._emit_progress(phase="running")

        task_gen = asyncio.create_task(
            self._generator.run(self._stop_event), name="generator"
        )
        task_mon = asyncio.create_task(
            self._monitor.run(self._stop_event), name="monitor"
        )
        task_col = asyncio.create_task(
            self._collector.run(self._stop_event), name="collector"
        )

        pending: set[asyncio.Task] = {task_gen, task_mon, task_col}
        gen_result: Optional[KafkaGeneratorResult] = None
        mon_result: Optional[OpenSearchIndexMonitorResult] = None
        col_result: Optional[KafkaArtifactCollectorResult] = None

        while pending:
            done, pending = await asyncio.wait(
                pending,
                return_when=asyncio.FIRST_COMPLETED,
            )

            crashed_task: Optional[asyncio.Task] = None

            # --- Pass 1: collect all results from this batch ---
            for task in done:
                task_name = task.get_name()

                try:
                    result = task.result()
                except Exception:
                    logger.exception(
                        "Session %s: phase 1 task %s crashed",
                        self._session_id,
                        task_name,
                    )
                    crashed_task = task
                    continue

                if task_name == "generator":
                    gen_result = result
                elif task_name == "monitor":
                    mon_result = result
                elif task_name == "collector":
                    col_result = result

            # --- Exception in this batch → stop everything and raise ---
            if crashed_task is not None:
                self._stop_event.set()
                for remaining in pending:
                    remaining.cancel()
                await self._wait_remaining(pending)
                raise RuntimeError(
                    f"Phase 1 task {crashed_task.get_name()} crashed"
                )

            # --- Pass 2: check premature completion AFTER the whole batch is assigned ---
            if gen_result is None:
                premature = []
                if mon_result is not None:
                    premature.append("monitor")
                if col_result is not None:
                    premature.append("collector")
                if premature:
                    self._stop_event.set()
                    await self._wait_remaining(pending)
                    raise RuntimeError(
                        f"Phase 1: {', '.join(premature)} finished before generator"
                    )

            # Generator finished — signal remaining tasks to stop
            if gen_result is not None and not self._stop_event.is_set():
                self._stop_event.set()

        return gen_result, mon_result, col_result

    async def _wait_remaining(self, pending: set[asyncio.Task]) -> None:
        """
        Дождаться завершения оставшихся задач после stop_event или cancel.

        :param pending: Множество ещё не завершённых задач.
        """
        if not pending:
            return

        results = await asyncio.gather(*pending, return_exceptions=True)
        for item in results:
            if isinstance(item, BaseException) and not isinstance(
                item, asyncio.CancelledError
            ):
                logger.error(
                    "Session %s: phase 1 remaining task error: %s",
                    self._session_id,
                    item,
                )

    async def _periodic_snapshot_loop(self) -> None:
        """
        Периодически пересчитывать все агрегаты, пока активна фаза 1.

        Каждый тик — полный пересчёт с текущего состояния диска. При ошибке
        тик логируется и цикл продолжается. Если на момент срабатывания
        таймера stop_event уже установлен — тик пропускается.
        """
        while True:
            await asyncio.sleep(self._report_interval_sec)

            if self._stop_event.is_set():
                continue

            try:
                stats_result = await self._statistics.run(self._stop_event)
                proj_result = await self._projection.run(self._stop_event)
                stage_result = await self._stage_size.run(self._stop_event)
                throughput_result = await self._throughput.run(
                    self._stop_event,
                    sent_messages=self._latest_sent_messages,
                )
                self._emit_periodic_snapshot(stage_result, throughput_result)
            except Exception:
                logger.exception(
                    "Session %s: periodic snapshot tick failed",
                    self._session_id,
                )

    def _emit_periodic_snapshot(
        self,
        stage_result: EtlStageSizeAggregateResult,
        throughput_result: EtlThroughputAggregateResult,
    ) -> None:
        """
        Отправить результат периодического snapshot в UI-очередь.

        Событие содержит только новые aggregate-поля; существующие поля
        statistics/projection остаются ``None``, так как они типизированы
        под progress-события, а не под final results.

        :param stage_result: Результат Stage/Size агрегатов этого тика.
        :param throughput_result: Результат Throughput агрегатов этого тика.
        """
        try:
            self._progress_queue.put_nowait(
                SessionProgress(
                    session_id=self._session_id,
                    phase="running",
                    stage_size_aggregates=stage_result,
                    throughput_aggregates=throughput_result,
                )
            )
        except asyncio.QueueFull:
            logger.warning(
                "Session %s: progress queue full for periodic snapshot",
                self._session_id,
            )

    def _emit_progress(
        self,
        phase: str,
        error: Optional[str] = None,
    ) -> None:
        """
        Отправить событие прогресса в UI-очередь.

        :param phase: Имя текущей фазы сессии.
        :param error: Текст ошибки или ``None``.
        """
        try:
            self._progress_queue.put_nowait(
                SessionProgress(
                    session_id=self._session_id,
                    phase=phase,
                    error=error,
                )
            )
        except asyncio.QueueFull:
            logger.warning(
                "Session %s: progress queue full, dropping phase=%s",
                self._session_id,
                phase,
            )

    def _on_generator_progress(self, progress: KafkaGeneratorProgress) -> None:
        self._latest_sent_messages = progress.sent_messages
        self._emit_module_progress(
            SessionProgress(
                session_id=self._session_id,
                phase="running",
                generator=progress,
            )
        )

    def _on_monitor_progress(self, progress: OpenSearchIndexCount) -> None:
        self._emit_module_progress(
            SessionProgress(
                session_id=self._session_id,
                phase="running",
                monitor=progress,
            )
        )

    def _on_collector_progress(self, progress: KafkaArtifactSaved) -> None:
        self._emit_module_progress(
            SessionProgress(
                session_id=self._session_id,
                phase="running",
                collector=progress,
            )
        )

    def _on_statistics_progress(
        self, progress: EtlReportStatisticsProgress
    ) -> None:
        self._emit_module_progress(
            SessionProgress(
                session_id=self._session_id,
                phase="statistics",
                statistics=progress,
            )
        )

    def _on_projection_progress(self, progress: EtlProjectionProgress) -> None:
        self._emit_module_progress(
            SessionProgress(
                session_id=self._session_id,
                phase="projection",
                projection=progress,
            )
        )

    def _emit_module_progress(self, progress: SessionProgress) -> None:
        """
        Передать модульное progress-событие в UI-очередь.

        Ошибки отправки подавляются: падение UI-слоя не должно ронять
        worker и тестовую сессию.

        :param progress: Событие прогресса конкретного модуля.
        """
        try:
            self._progress_queue.put_nowait(progress)
        except asyncio.QueueFull:
            logger.warning(
                "Session %s: progress queue full for module event",
                self._session_id,
            )