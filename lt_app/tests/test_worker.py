# test_worker.py
"""Smoke-тесты для SessionWorker."""

from __future__ import annotations

import asyncio
import queue
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import pytest

from lt_app.src.worker import SessionProgress, SessionResult, SessionWorker


class FakeKafkaEventGenerator:
    """Возвращает готовый результат после задержки 0.1s."""

    def __init__(self, settings, progress_callback=None):
        pass

    async def run(self, stop_event: asyncio.Event):
        await asyncio.sleep(0.1)
        from lt_app.src.kafka_event_generator import KafkaGeneratorResult

        return KafkaGeneratorResult(
            sent_messages=100,
            stopped_by_request=False,
            last_lag=5,
        )


class FakeOpenSearchIndexMonitor:
    """Сразу возвращает готовый результат."""

    def __init__(self, settings, progress_callback=None):
        pass

    async def run(self, stop_event: asyncio.Event):
        await stop_event.wait()
        from lt_app.src.opensearch_index_monitor import (
            OpenSearchIndexMonitorResult,
        )

        return OpenSearchIndexMonitorResult(
            completed_cycles=3,
            written_records=9,
            stopped_by_request=True,
            output_csv_path=Path("/tmp/fake_index_counts.csv"),
        )


class FakeKafkaJsonArtifactCollector:
    """Сразу возвращает готовый результат."""

    def __init__(self, settings, progress_callback=None):
        pass

    async def run(self, stop_event: asyncio.Event):
        await stop_event.wait()
        from lt_app.src.kafka_json_artifact_collector import (
            KafkaArtifactCollectorResult,
        )

        return KafkaArtifactCollectorResult(
            saved_messages=50,
            stopped_by_request=True,
            output_directory=Path("/tmp/fake_artifacts/session-1"),
        )


class FakeEtlReportStatisticsBuilder:
    """Сразу возвращает готовый результат."""

    def __init__(self, settings, progress_callback=None):
        pass

    async def run(self, stop_event: asyncio.Event):
        from lt_app.src.etl_report_statistics import (
            EtlReportStatisticsResult,
        )

        return EtlReportStatisticsResult(
            processed_reports=10,
            done_rows=20,
            skipped_rows=2,
            read_failures=1,
            stopped_by_request=False,
            done_output_csv_path=Path("/tmp/fake_stats_done.csv"),
            skipped_output_csv_path=Path("/tmp/fake_stats_skipped.csv"),
            read_failures_output_csv_path=Path(
                "/tmp/fake_stats_read_failures.csv"
            ),
        )


class FakeEtlStatisticsProjectionBuilder:
    """Сразу возвращает готовый результат."""

    def __init__(self, settings, progress_callback=None):
        pass

    async def run(self, stop_event: asyncio.Event):
        from lt_app.src.etl_statistics_projection import (
            EtlStatisticsProjectionResult,
        )

        return EtlStatisticsProjectionResult(
            source_rows=20,
            stage_models=4,
            coefficient_rows=4,
            stage_projection_rows=8,
            total_projection_rows=2,
            stopped_by_request=False,
            coefficients_output_csv_path=Path("/tmp/fake_coefficients.csv"),
            stage_projection_output_csv_path=Path(
                "/tmp/fake_projection_by_stage.csv"
            ),
            total_projection_output_csv_path=Path(
                "/tmp/fake_projection_total.csv"
            ),
        )


def _build_fake_worker(
    progress_queue: queue.Queue,
    session_id: str = "test-session-1",
) -> SessionWorker:
    """Создать SessionWorker с fake-модулями."""
    return SessionWorker(
        generator_settings=None,
        monitor_settings=None,
        collector_settings=None,
        statistics_settings=None,
        projection_settings=None,
        session_id=session_id,
        progress_queue=progress_queue,
    )


@pytest.mark.asyncio
async def test_session_worker_happy_path():
    """Все пять фаз завершаются без ошибок, result.error is None."""
    patchers = [
        patch("lt_app.src.worker.KafkaEventGenerator", FakeKafkaEventGenerator),
        patch("lt_app.src.worker.OpenSearchIndexMonitor", FakeOpenSearchIndexMonitor),
        patch("lt_app.src.worker.KafkaJsonArtifactCollector", FakeKafkaJsonArtifactCollector),
        patch("lt_app.src.worker.EtlReportStatisticsBuilder", FakeEtlReportStatisticsBuilder),
        patch("lt_app.src.worker.EtlStatisticsProjectionBuilder", FakeEtlStatisticsProjectionBuilder),
    ]

    for p in patchers:
        p.start()

    try:
        progress_queue = queue.Queue()
        worker = _build_fake_worker(progress_queue)
        result = await worker.run()

        assert isinstance(result, SessionResult)
        assert result.error is None
        assert result.generator is not None
        assert result.generator.sent_messages == 100
        assert result.monitor is not None
        assert result.monitor.completed_cycles == 3
        assert result.collector is not None
        assert result.collector.saved_messages == 50
        assert result.statistics is not None
        assert result.statistics.done_rows == 20
        assert result.projection is not None
        assert result.projection.stage_models == 4
        assert result.artifacts_dir == Path("/tmp/fake_artifacts/session-1")

        phases_seen: list[str] = []
        while not progress_queue.empty():
            event = progress_queue.get_nowait()
            phases_seen.append(event.phase)

        assert "running" in phases_seen
        assert "statistics" in phases_seen
        assert "projection" in phases_seen
        assert "done" in phases_seen
        assert "failed" not in phases_seen

    finally:
        for p in patchers:
            p.stop()


class CrashingFakeOpenSearchIndexMonitor(FakeOpenSearchIndexMonitor):
    """Monitor, который крашится с исключением."""

    async def run(self, stop_event: asyncio.Event):
        raise RuntimeError("OpenSearch connection refused")


@pytest.mark.asyncio
async def test_session_worker_monitor_crash():
    """При краше monitor worker останавливает остальные задачи и возвращает error."""
    patchers = [
        patch("lt_app.src.worker.KafkaEventGenerator", FakeKafkaEventGenerator),
        patch("lt_app.src.worker.OpenSearchIndexMonitor", CrashingFakeOpenSearchIndexMonitor),
        patch("lt_app.src.worker.KafkaJsonArtifactCollector", FakeKafkaJsonArtifactCollector),
        patch("lt_app.src.worker.EtlReportStatisticsBuilder", FakeEtlReportStatisticsBuilder),
        patch("lt_app.src.worker.EtlStatisticsProjectionBuilder", FakeEtlStatisticsProjectionBuilder),
    ]

    for p in patchers:
        p.start()

    try:
        progress_queue = queue.Queue()
        worker = _build_fake_worker(progress_queue)
        result = await worker.run()

        assert isinstance(result, SessionResult)
        assert result.error is not None
        assert "RuntimeError" in result.error

        last_event: Optional[SessionProgress] = None
        while not progress_queue.empty():
            last_event = progress_queue.get_nowait()

        assert last_event is not None
        assert last_event.phase == "failed"
        assert last_event.error is not None

    finally:
        for p in patchers:
            p.stop()