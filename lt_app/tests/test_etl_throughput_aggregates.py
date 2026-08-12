# test_etl_throughput_aggregates.py
"""Unit-тесты для EtlThroughputAggregateBuilder."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from lt_app.src.etl_throughput_aggregates import (
    OUTPUT_COLUMNS,
    EtlThroughputAggregateBuilder,
    EtlThroughputAggregateProgress,
    EtlThroughputAggregateResult,
    EtlThroughputAggregateSettings,
)


def _build_monitor_fixture_csv(tmp_path: Path) -> Path:
    r"""
    Создать CSV-фикстуру мониторинга с двумя индексами за 3 минуты.

    Структура:
    - Минута 2025-06-01 10:00: индекс "a" 6 опросов (counts 10..35, last=35),
      индекс "b" 6 опросов (counts 5..30, last=30). Всего 12 опросов в одной
      минуте — доказывает, что используется last-per-minute, не sum.
    - Минута 10:01: индекс "a" 2 опроса (last=55), индекс "b" только error-
      строки (проверка forward-fill: не должно быть false drop).
    - Минута 10:02: оба индекса с валидными опросами (a last=75, b last=50).
    """
    records: list[dict] = []

    # Minute 10:00 — many polls (12 total), only last per index matters
    for i in range(6):
        records.append({
            "timestamp": f"2025-06-01T10:00:{10 + i * 8:02d}Z",
            "index_name": "a",
            "document_count": 10 + i * 5,
            "error": None,
        })
        records.append({
            "timestamp": f"2025-06-01T10:00:{10 + i * 8:02d}Z",
            "index_name": "b",
            "document_count": 5 + i * 5,
            "error": None,
        })

    # Minute 10:01 — "b" has only error rows
    records.append({
        "timestamp": "2025-06-01T10:01:15Z",
        "index_name": "a",
        "document_count": 45,
        "error": None,
    })
    records.append({
        "timestamp": "2025-06-01T10:01:45Z",
        "index_name": "a",
        "document_count": 55,
        "error": None,
    })
    records.append({
        "timestamp": "2025-06-01T10:01:15Z",
        "index_name": "b",
        "document_count": None,
        "error": "ConnectionError: timeout",
    })
    records.append({
        "timestamp": "2025-06-01T10:01:45Z",
        "index_name": "b",
        "document_count": None,
        "error": "ConnectionError: timeout",
    })

    # Minute 10:02 — both valid
    records.append({
        "timestamp": "2025-06-01T10:02:10Z",
        "index_name": "a",
        "document_count": 65,
        "error": None,
    })
    records.append({
        "timestamp": "2025-06-01T10:02:50Z",
        "index_name": "a",
        "document_count": 75,
        "error": None,
    })
    records.append({
        "timestamp": "2025-06-01T10:02:10Z",
        "index_name": "b",
        "document_count": 40,
        "error": None,
    })
    records.append({
        "timestamp": "2025-06-01T10:02:50Z",
        "index_name": "b",
        "document_count": 50,
        "error": None,
    })

    df = pd.DataFrame(records)
    csv_path = tmp_path / "fixture_monitor.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8")
    return csv_path


class _CollectingProgress:
    def __init__(self) -> None:
        self.events: list[EtlThroughputAggregateProgress] = []

    def __call__(self, progress: EtlThroughputAggregateProgress) -> None:
        self.events.append(progress)


@pytest.mark.asyncio
async def test_builder_happy_path(tmp_path: Path):
    """Полный проход: 3 минуты (20 строк), forward-fill на error-минуте, корректные дельты."""
    source_csv = _build_monitor_fixture_csv(tmp_path)
    output_csv = tmp_path / "throughput.csv"
    progress = _CollectingProgress()

    settings = EtlThroughputAggregateSettings(
        source_monitor_csv_path=source_csv,
        output_csv_path=output_csv,
    )
    builder = EtlThroughputAggregateBuilder(
        settings=settings,
        progress_callback=progress,
    )

    result = await builder.run(asyncio.Event(), sent_messages=100)

    assert isinstance(result, EtlThroughputAggregateResult)
    assert result.source_rows == 20
    assert result.stopped_by_request is False
    assert result.output_csv_path == output_csv
    assert result.minute_buckets == 3

    assert output_csv.is_file()
    output_frame = pd.read_csv(output_csv)
    assert len(output_frame) == 1
    assert list(output_frame.columns) == list(OUTPUT_COLUMNS)

    row = output_frame.iloc[0]

    # Cumulative totals: min 0: a=35 b=30=65, min 1: a=55 b(ffill)=30=85,
    # min 2: a=75 b=50=125
    assert row["total_indexed_documents"] == 125
    assert row["total_sent_messages"] == 100
    assert row["coverage_ratio"] == pytest.approx(1.25)

    # Deltas: min1 total=85-65=20, min2 total=125-85=40
    assert row["avg_per_minute"] == pytest.approx(30.0)
    assert row["max_per_minute"] == pytest.approx(40.0)

    # 5-min trailing mean: [20, (20+40)/2=30]
    assert row["avg_per_minute_smoothed_5min"] == pytest.approx(25.0)
    assert row["max_per_minute_smoothed_5min"] == pytest.approx(30.0)

    assert row["computed_at"] is not None
    assert len(progress.events) == 3
    assert {e.phase for e in progress.events} == {
        "loading", "aggregating", "writing",
    }


@pytest.mark.asyncio
async def test_builder_sent_messages_zero(tmp_path: Path):
    """sent_messages=0 → coverage_ratio=None, не ZeroDivisionError."""
    source_csv = _build_monitor_fixture_csv(tmp_path)
    output_csv = tmp_path / "throughput_zero.csv"

    settings = EtlThroughputAggregateSettings(
        source_monitor_csv_path=source_csv,
        output_csv_path=output_csv,
    )
    builder = EtlThroughputAggregateBuilder(settings=settings)

    result = await builder.run(asyncio.Event(), sent_messages=0)

    assert result.stopped_by_request is False
    output_frame = pd.read_csv(output_csv)
    row = output_frame.iloc[0]
    assert row["total_sent_messages"] == 0
    assert pd.isna(row["coverage_ratio"])


@pytest.mark.asyncio
async def test_builder_stop_event_before_work(tmp_path: Path):
    """Stop event до начала работы — header-only CSV, нулевые счётчики."""
    source_csv = _build_monitor_fixture_csv(tmp_path)
    output_csv = tmp_path / "throughput_stopped.csv"

    settings = EtlThroughputAggregateSettings(
        source_monitor_csv_path=source_csv,
        output_csv_path=output_csv,
    )
    builder = EtlThroughputAggregateBuilder(settings=settings)

    stop_event = asyncio.Event()
    stop_event.set()

    result = await builder.run(stop_event, sent_messages=100)

    assert result.stopped_by_request is True
    assert result.source_rows == 0
    assert result.minute_buckets == 0
    assert output_csv.is_file()

    output_frame = pd.read_csv(output_csv)
    assert len(output_frame) == 0
    assert list(output_frame.columns) == list(OUTPUT_COLUMNS)


@pytest.mark.asyncio
async def test_builder_all_error_rows(tmp_path: Path):
    """Все строки — error → header-only output, все показатели None."""
    csv_path = tmp_path / "all_errors.csv"
    pd.DataFrame([
        {"timestamp": "2025-06-01T10:00:00Z", "index_name": "a",
         "document_count": None, "error": "fail"},
        {"timestamp": "2025-06-01T10:00:30Z", "index_name": "b",
         "document_count": None, "error": "fail"},
    ]).to_csv(csv_path, index=False, encoding="utf-8")

    output_csv = tmp_path / "throughput_errors.csv"
    settings = EtlThroughputAggregateSettings(
        source_monitor_csv_path=csv_path,
        output_csv_path=output_csv,
    )
    builder = EtlThroughputAggregateBuilder(settings=settings)

    result = await builder.run(asyncio.Event(), sent_messages=100)

    assert result.source_rows == 2
    assert result.minute_buckets == 0
    assert result.stopped_by_request is False
    assert output_csv.is_file()

    output_frame = pd.read_csv(output_csv)
    row = output_frame.iloc[0]
    assert row["total_indexed_documents"] == 0
    assert pd.isna(row["coverage_ratio"])
    assert pd.isna(row["avg_per_minute"])


@pytest.mark.asyncio
async def test_builder_source_not_found(tmp_path: Path):
    """Несуществующий CSV → FileNotFoundError."""
    settings = EtlThroughputAggregateSettings(
        source_monitor_csv_path=tmp_path / "nonexistent.csv",
        output_csv_path=tmp_path / "out.csv",
    )
    builder = EtlThroughputAggregateBuilder(settings=settings)

    with pytest.raises(FileNotFoundError, match="Monitor CSV not found"):
        await builder.run(asyncio.Event(), sent_messages=100)