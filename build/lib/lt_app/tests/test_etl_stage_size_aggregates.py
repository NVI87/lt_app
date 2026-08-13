# test_etl_stage_size_aggregates.py
"""Unit-тесты для EtlStageSizeAggregateBuilder."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import Optional

import pandas as pd
import pytest

from lt_app.src.etl_report_statistics import DONE_COLUMNS
from lt_app.src.etl_stage_size_aggregates import (
    BYTES_IN_MB,
    OUTPUT_COLUMNS,
    EtlStageSizeAggregateBuilder,
    EtlStageSizeAggregateProgress,
    EtlStageSizeAggregateResult,
    EtlStageSizeAggregateSettings,
)

STAGES = ["EXTRACTING", "CONVERTING", "TOKENIZING", "VECTORIZING", "INDEXING"]


def _build_fixture_done_csv(tmp_path: Path) -> Path:
    """Создать CSV-фикстуру с DONE-строками для тестов.

    Покрывает:
    - Два attach_type: "pdf" (размер ~2 MB) и "image" (размер ~8 MB)
    - Два size bucket: pdf → 2 MB, image → 8 MB
    - Все пять стадий (по 2 строки на каждую комбинацию type/stage = 20 rows)
    - Одна группа (pdf, 2 MB, EXTRACTING) с samples=0 на всех строках
    - Одна строка с attach_size=None → excluded_unsized
    - Одна page_body строка (stage=None, attach_type="page_body") → excluded entirely
    """
    rows: list[dict] = []

    for attach_type, size_bytes in [("pdf", 2 * BYTES_IN_MB), ("image", 8 * BYTES_IN_MB)]:
        for stage in STAGES:
            samples_val = 0 if (attach_type == "pdf" and stage == "EXTRACTING") else 10
            for i in range(2):
                rows.append({
                    "report_file": f"report_{attach_type}_{i}.json",
                    "document_id": f"doc_{attach_type}_{i}",
                    "document_name": f"Document {attach_type} {i}",
                    "attach_id": f"att_{attach_type}_{i}",
                    "attach_name": f"file_{attach_type}_{i}.bin",
                    "attach_type": attach_type,
                    "attach_size": size_bytes,
                    "stage": stage,
                    "status": "DONE",
                    "wall_time_sec": 1.5,
                    "cpu_mean_pct": 30.0,
                    "cpu_peak_pct": 45.0,
                    "rss_mean": 512.0,
                    "rss_peak": 768.0,
                    "samples": samples_val,
                    "started_at": "2025-01-01T00:00:00",
                })

    rows.append({
        "report_file": "report_unsized.json",
        "document_id": "doc_unsized",
        "document_name": "Document Unsized",
        "attach_id": "att_unsized",
        "attach_name": "file_unsized.bin",
        "attach_type": "pdf",
        "attach_size": None,
        "stage": "EXTRACTING",
        "status": "DONE",
        "wall_time_sec": 2.0,
        "cpu_mean_pct": 20.0,
        "cpu_peak_pct": 30.0,
        "rss_mean": 256.0,
        "rss_peak": 512.0,
        "samples": 5,
        "started_at": "2025-01-01T00:00:00",
    })

    rows.append({
        "report_file": "report_page_body.json",
        "document_id": "doc_page_body",
        "document_name": "Document Page Body",
        "attach_id": None,
        "attach_name": None,
        "attach_type": "page_body",
        "attach_size": None,
        "stage": None,
        "status": "DONE",
        "wall_time_sec": None,
        "cpu_mean_pct": None,
        "cpu_peak_pct": None,
        "rss_mean": None,
        "rss_peak": None,
        "samples": None,
        "started_at": None,
    })

    df = pd.DataFrame(rows, columns=DONE_COLUMNS)
    csv_path = tmp_path / "fixture_done.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8")
    return csv_path


class _CollectingCallback:
    """Callback, собирающий progress events."""

    def __init__(self) -> None:
        self.events: list[EtlStageSizeAggregateProgress] = []

    def __call__(self, progress: EtlStageSizeAggregateProgress) -> None:
        self.events.append(progress)


@pytest.mark.asyncio
async def test_builder_happy_path(tmp_path: Path):
    """Полный проход: загрузка, агрегация, запись, корректные значения."""
    source_csv = _build_fixture_done_csv(tmp_path)
    output_csv = tmp_path / "aggregates.csv"
    progress = _CollectingCallback()

    settings = EtlStageSizeAggregateSettings(
        source_done_csv_path=source_csv,
        output_csv_path=output_csv,
    )
    builder = EtlStageSizeAggregateBuilder(
        settings=settings,
        progress_callback=progress,
    )

    result = await builder.run(asyncio.Event())

    assert isinstance(result, EtlStageSizeAggregateResult)
    assert result.source_rows == 22
    assert result.excluded_unsized_rows == 1
    assert result.stopped_by_request is False
    assert result.output_csv_path == output_csv

    assert output_csv.is_file()
    output_frame = pd.read_csv(output_csv)

    assert list(output_frame.columns) == list(OUTPUT_COLUMNS)

    assert len(output_frame) == 10

    pdf_2_extracting = output_frame[
        (output_frame["attach_type"] == "pdf")
        & (output_frame["size_bucket_mb"] == 2)
        & (output_frame["stage"] == "EXTRACTING")
    ]
    assert len(pdf_2_extracting) == 1
    pdf_2_extracting = pdf_2_extracting.iloc[0]
    assert pdf_2_extracting["row_count"] == 2
    assert pdf_2_extracting["total_samples"] == 0
    assert pdf_2_extracting["total_wall_time_sec"] == 3.0
    assert pd.isna(pdf_2_extracting["weighted_mean_cpu_pct"])
    assert pd.isna(pdf_2_extracting["weighted_mean_rss_bytes"])
    assert pdf_2_extracting["peak_cpu_pct"] == 45.0
    assert pdf_2_extracting["peak_rss_bytes"] == 768.0

    pdf_2_converting = output_frame[
        (output_frame["attach_type"] == "pdf")
        & (output_frame["size_bucket_mb"] == 2)
        & (output_frame["stage"] == "CONVERTING")
    ].iloc[0]
    assert pdf_2_converting["row_count"] == 2
    assert pdf_2_converting["total_samples"] == 20
    assert pdf_2_converting["weighted_mean_cpu_pct"] == 30.0
    assert pdf_2_converting["weighted_mean_rss_bytes"] == 512.0

    assert len({e.phase for e in progress.events}) == 3
    assert progress.events[0].phase == "loading"
    assert progress.events[1].phase == "aggregating"
    assert progress.events[2].phase == "writing"


@pytest.mark.asyncio
async def test_builder_stop_event_before_work(tmp_path: Path):
    """Stop event установлен до начала работы — stopped_by_request=True, CSV пустой."""
    source_csv = _build_fixture_done_csv(tmp_path)
    output_csv = tmp_path / "aggregates_stopped.csv"

    settings = EtlStageSizeAggregateSettings(
        source_done_csv_path=source_csv,
        output_csv_path=output_csv,
    )
    builder = EtlStageSizeAggregateBuilder(settings=settings)

    stop_event = asyncio.Event()
    stop_event.set()

    result = await builder.run(stop_event)

    assert result.stopped_by_request is True
    assert result.source_rows == 0
    assert result.aggregate_rows == 0
    assert result.excluded_unsized_rows == 0
    assert output_csv.is_file()

    output_frame = pd.read_csv(output_csv)
    assert len(output_frame) == 0
    assert list(output_frame.columns) == list(OUTPUT_COLUMNS)


@pytest.mark.asyncio
async def test_builder_empty_source_produces_header_only(tmp_path: Path):
    """Пустой source CSV — header-only output, zero counts."""
    source_csv = tmp_path / "empty_done.csv"
    pd.DataFrame(columns=DONE_COLUMNS).to_csv(
        source_csv, index=False, encoding="utf-8",
    )
    output_csv = tmp_path / "aggregates_empty.csv"

    settings = EtlStageSizeAggregateSettings(
        source_done_csv_path=source_csv,
        output_csv_path=output_csv,
    )
    builder = EtlStageSizeAggregateBuilder(settings=settings)

    result = await builder.run(asyncio.Event())

    assert result.source_rows == 0
    assert result.aggregate_rows == 0
    assert result.excluded_unsized_rows == 0
    assert result.stopped_by_request is False
    assert output_csv.is_file()

    output_frame = pd.read_csv(output_csv)
    assert len(output_frame) == 0
    assert list(output_frame.columns) == list(OUTPUT_COLUMNS)


@pytest.mark.asyncio
async def test_builder_source_not_found(tmp_path: Path):
    """Несуществующий source CSV — FileNotFoundError."""
    settings = EtlStageSizeAggregateSettings(
        source_done_csv_path=tmp_path / "nonexistent.csv",
        output_csv_path=tmp_path / "out.csv",
    )
    builder = EtlStageSizeAggregateBuilder(settings=settings)

    with pytest.raises(FileNotFoundError, match="DONE CSV not found"):
        await builder.run(asyncio.Event())


@pytest.mark.asyncio
async def test_size_bytes_to_bucket_mb():
    """Прямой тест функции бакетирования."""
    fn = EtlStageSizeAggregateBuilder._size_bytes_to_bucket_mb

    assert fn(None) is None
    assert fn(0) is None
    assert fn(-100) is None

    assert fn(BYTES_IN_MB) == 1
    assert fn(int(1.9 * BYTES_IN_MB)) == 2
    assert fn(int(2.1 * BYTES_IN_MB)) == 2
    assert fn(int(5.5 * BYTES_IN_MB)) == 4
    assert fn(int(6.0 * BYTES_IN_MB)) == 8
    assert fn(int(1024 * BYTES_IN_MB)) == 1024
    assert fn(int(2000 * BYTES_IN_MB)) == 1024