# test_runtime_preflight.py
"""Pre-flight constructor checks for all runtime modules.

This test does not start Kafka/OpenSearch connections or run async work. It
constructs every runtime class with the canonical default settings to catch
signature mismatches (TypeError) and invalid defaults (ValidationError) in one
pass, before Docker/manual UI smoke testing.
"""

from __future__ import annotations

import asyncio

import pytest

from lt_app.src import runtime_settings as rs
from lt_app.src.etl_report_statistics import EtlReportStatisticsBuilder
from lt_app.src.etl_stage_size_aggregates import EtlStageSizeAggregateBuilder
from lt_app.src.etl_statistics_projection import EtlStatisticsProjectionBuilder
from lt_app.src.etl_throughput_aggregates import EtlThroughputAggregateBuilder
from lt_app.src.kafka_event_generator import KafkaEventGenerator
from lt_app.src.kafka_json_artifact_collector import KafkaJsonArtifactCollector
from lt_app.src.opensearch_index_monitor import OpenSearchIndexMonitor

from lt_app.src.etl_report_statistics import EtlReportStatisticsSettings
from lt_app.src.etl_stage_size_aggregates import EtlStageSizeAggregateSettings
from lt_app.src.etl_statistics_projection import EtlStatisticsProjectionSettings
from lt_app.src.etl_throughput_aggregates import EtlThroughputAggregateSettings
from lt_app.src.kafka_event_generator import KafkaEventGeneratorSettings
from lt_app.src.kafka_json_artifact_collector import KafkaArtifactCollectorSettings
from lt_app.src.opensearch_index_monitor import OpenSearchIndexMonitorSettings


def _frozen_defaults() -> dict[str, dict]:
    return rs.freeze_settings(rs.default_settings())


@pytest.mark.parametrize(
    ("section", "settings_cls", "runtime_cls"),
    [
        ("generator", KafkaEventGeneratorSettings, KafkaEventGenerator),
        ("monitor", OpenSearchIndexMonitorSettings, OpenSearchIndexMonitor),
        (
            "collector",
            KafkaArtifactCollectorSettings,
            KafkaJsonArtifactCollector,
        ),
        ("statistics", EtlReportStatisticsSettings, EtlReportStatisticsBuilder),
        (
            "projection",
            EtlStatisticsProjectionSettings,
            EtlStatisticsProjectionBuilder,
        ),
        (
            "stage_size",
            EtlStageSizeAggregateSettings,
            EtlStageSizeAggregateBuilder,
        ),
        (
            "throughput",
            EtlThroughputAggregateSettings,
            EtlThroughputAggregateBuilder,
        ),
    ],
)
def test_runtime_class_constructs_with_defaults(section, settings_cls, runtime_cls):
    """Each runtime class must construct without TypeError/ValidationError."""
    section_dict = _frozen_defaults().get(section, {})
    settings = settings_cls(**section_dict)

    # aiokafka classes must be constructed inside a running loop. Other runtime
    # classes can be constructed directly, but a uniform async wrapper keeps
    # this pre-flight check predictable across all 7 modules.
    async def _construct():
        return runtime_cls(settings=settings)

    instance = asyncio.run(_construct())

    assert instance is not None


def test_all_seven_runtime_classes_in_one_run():
    """Single smoke test that instantiates all runtime classes at once."""
    frozen = _frozen_defaults()

    builders = [
        ("generator", KafkaEventGeneratorSettings, KafkaEventGenerator),
        ("monitor", OpenSearchIndexMonitorSettings, OpenSearchIndexMonitor),
        (
            "collector",
            KafkaArtifactCollectorSettings,
            KafkaJsonArtifactCollector,
        ),
        ("statistics", EtlReportStatisticsSettings, EtlReportStatisticsBuilder),
        (
            "projection",
            EtlStatisticsProjectionSettings,
            EtlStatisticsProjectionBuilder,
        ),
        (
            "stage_size",
            EtlStageSizeAggregateSettings,
            EtlStageSizeAggregateBuilder,
        ),
        (
            "throughput",
            EtlThroughputAggregateSettings,
            EtlThroughputAggregateBuilder,
        ),
    ]

    async def _construct_all():
        for section, settings_cls, runtime_cls in builders:
            settings = settings_cls(**frozen.get(section, {}))
            runtime_cls(settings=settings)

    asyncio.run(_construct_all())
