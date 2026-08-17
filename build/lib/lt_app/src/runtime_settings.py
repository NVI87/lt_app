# runtime_settings.py
"""
Canonical configuration registry for runtime settings.

This is the single source of truth for every configuration field consumed by
any runtime component (generator, collector, monitor, statistics, projection,
stage_size, throughput, and worker lifecycle). Every field is available through
three channels: YAML, environment variable, and UI control.

Precedence is temporal, not hierarchical: the last operation in time on a
specific field path wins, regardless of source (defaults, ENV, YAML, UI edit).

Before session start, ``effective_settings`` is the one mutable dict of all
resolved values. At start, ``run_settings = freeze_settings(effective_settings)``
creates an immutable snapshot for the worker.

Only this module reads ``os.environ`` / ``os.getenv``. Runtime modules receive
plain ``pydantic.BaseModel`` instances (not ``BaseSettings``) and never read
environment variables themselves.
"""

from __future__ import annotations

import copy
import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Optional

import yaml

# ---------------------------------------------------------------------------
# Field descriptor types
# ---------------------------------------------------------------------------


class WidgetType(StrEnum):
    TEXT = "text"
    TEXT_AREA = "text_area"
    INT = "int"
    FLOAT = "float"
    CHECKBOX = "checkbox"
    SELECT = "select"
    PASSWORD = "password"
    LIST = "list"


# ---------------------------------------------------------------------------
# Field descriptor
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FieldDescriptor:
    """Declarative description of one canonical configuration field.

    :ivar section: Section name (``run``, ``generator``, ``monitor``, etc.).
    :ivar name: Field name within the section.
    :ivar type_: Python type (``str``, ``int``, ``float``, ``bool``, ``list``, ``Path``).
    :ivar default: Default value for new sessions.
    :ivar required: Whether the field must be non-empty/non-None before session start.
    :ivar env_var: Full ``LT_APP__SECTION__FIELD`` environment variable name.
    :ivar widget: UI widget type.
    :ivar item_type: For list fields, the type of each element (``str``, ``float``, etc.).
    :ivar label: Optional override for the widget label.
    :ivar options: Comma-separated option values for ``select`` widgets.
    :ivar gt: Minimum value (exclusive) for numeric fields.
    :ivar ge: Minimum value (inclusive) for numeric fields.
    :ivar le: Maximum value (inclusive) for numeric fields.
    """

    section: str
    name: str
    type_: type
    default: Any
    required: bool
    env_var: str
    widget: WidgetType
    item_type: Optional[type] = None
    label: Optional[str] = None
    options: Optional[str] = None
    gt: Optional[float] = None
    ge: Optional[float] = None
    le: Optional[float] = None


# ---------------------------------------------------------------------------
# Field registry — 66 fields across 8 sections
# ---------------------------------------------------------------------------
# session_id is explicitly excluded from this registry.
# It is runtime-generated (not user-configurable) and consumed only by
# SessionWorker / artifact directory naming. There is no YAML, ENV, or UI
# mapping for it — it lives in st.session_state.session_id and is passed
# directly to the worker constructor, not through run_settings.
# ---------------------------------------------------------------------------

FIELD_REGISTRY: list[FieldDescriptor] = [
    # -- run (1 field) --
    FieldDescriptor(
        section="run",
        name="report_interval_minutes",
        type_=int,
        default=5,
        required=True,
        env_var="LT_APP__RUN__REPORT_INTERVAL_MINUTES",
        widget=WidgetType.INT,
        gt=0,
        le=60,
    ),
    # -- generator (20 fields) --
    FieldDescriptor(
        section="generator",
        name="source_csv_path",
        type_=Path,
        default=Path("/data/sample.csv"),
        required=True,
        env_var="LT_APP__GENERATOR__SOURCE_CSV_PATH",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="generator",
        name="bootstrap_servers",
        type_=str,
        default="localhost:9092",
        required=True,
        env_var="LT_APP__GENERATOR__BOOTSTRAP_SERVERS",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="generator",
        name="target_topic",
        type_=str,
        default="etl.target.topic",
        required=True,
        env_var="LT_APP__GENERATOR__TARGET_TOPIC",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="generator",
        name="consumer_group_id",
        type_=str,
        default="etl-load-test-group",
        required=True,
        env_var="LT_APP__GENERATOR__CONSUMER_GROUP_ID",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="generator",
        name="max_lag",
        type_=int,
        default=3,
        required=False,
        env_var="LT_APP__GENERATOR__MAX_LAG",
        widget=WidgetType.INT,
        gt=0,
    ),
    FieldDescriptor(
        section="generator",
        name="max_messages",
        type_=int,
        default=100,
        required=False,
        env_var="LT_APP__GENERATOR__MAX_MESSAGES",
        widget=WidgetType.INT,
        gt=0,
    ),
    FieldDescriptor(
        section="generator",
        name="lag_control_enabled",
        type_=bool,
        default=True,
        required=False,
        env_var="LT_APP__GENERATOR__LAG_CONTROL_ENABLED",
        widget=WidgetType.CHECKBOX,
    ),
    FieldDescriptor(
        section="generator",
        name="lag_check_interval_sec",
        type_=float,
        default=1.0,
        required=False,
        env_var="LT_APP__GENERATOR__LAG_CHECK_INTERVAL_SEC",
        widget=WidgetType.FLOAT,
        gt=0,
    ),
    FieldDescriptor(
        section="generator",
        name="lag_check_every_messages",
        type_=int,
        default=1,
        required=False,
        env_var="LT_APP__GENERATOR__LAG_CHECK_EVERY_MESSAGES",
        widget=WidgetType.INT,
        gt=0,
    ),
    FieldDescriptor(
        section="generator",
        name="repeat_source",
        type_=bool,
        default=False,
        required=False,
        env_var="LT_APP__GENERATOR__REPEAT_SOURCE",
        widget=WidgetType.CHECKBOX,
    ),
    FieldDescriptor(
        section="generator",
        name="preserve_source_partition",
        type_=bool,
        default=False,
        required=False,
        env_var="LT_APP__GENERATOR__PRESERVE_SOURCE_PARTITION",
        widget=WidgetType.CHECKBOX,
    ),
    FieldDescriptor(
        section="generator",
        name="timestamp_mode",
        type_=str,
        default="broker",
        required=False,
        env_var="LT_APP__GENERATOR__TIMESTAMP_MODE",
        widget=WidgetType.SELECT,
        options="broker,now,source",
    ),
    FieldDescriptor(
        section="generator",
        name="message_delay_sec",
        type_=float,
        default=0.0,
        required=False,
        env_var="LT_APP__GENERATOR__MESSAGE_DELAY_SEC",
        widget=WidgetType.FLOAT,
        ge=0,
    ),
    FieldDescriptor(
        section="generator",
        name="security_protocol",
        type_=str,
        default="PLAINTEXT",
        required=False,
        env_var="LT_APP__GENERATOR__SECURITY_PROTOCOL",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="generator",
        name="sasl_mechanism",
        type_=str,
        default="",
        required=False,
        env_var="LT_APP__GENERATOR__SASL_MECHANISM",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="generator",
        name="sasl_username",
        type_=str,
        default="",
        required=False,
        env_var="LT_APP__GENERATOR__SASL_USERNAME",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="generator",
        name="sasl_password",
        type_=str,
        default="",
        required=False,
        env_var="LT_APP__GENERATOR__SASL_PASSWORD",
        widget=WidgetType.PASSWORD,
    ),
    FieldDescriptor(
        section="generator",
        name="ssl_cafile",
        type_=Path,
        default=Path(""),
        required=False,
        env_var="LT_APP__GENERATOR__SSL_CAFILE",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="generator",
        name="ssl_certfile",
        type_=Path,
        default=Path(""),
        required=False,
        env_var="LT_APP__GENERATOR__SSL_CERTFILE",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="generator",
        name="ssl_keyfile",
        type_=Path,
        default=Path(""),
        required=False,
        env_var="LT_APP__GENERATOR__SSL_KEYFILE",
        widget=WidgetType.TEXT,
    ),
    # -- monitor (13 fields) --
    FieldDescriptor(
        section="monitor",
        name="host",
        type_=str,
        default="localhost",
        required=True,
        env_var="LT_APP__MONITOR__HOST",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="monitor",
        name="port",
        type_=int,
        default=9200,
        required=False,
        env_var="LT_APP__MONITOR__PORT",
        widget=WidgetType.INT,
        gt=0,
        le=65535,
    ),
    FieldDescriptor(
        section="monitor",
        name="username",
        type_=str,
        default="",
        required=False,
        env_var="LT_APP__MONITOR__USERNAME",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="monitor",
        name="password",
        type_=str,
        default="",
        required=False,
        env_var="LT_APP__MONITOR__PASSWORD",
        widget=WidgetType.PASSWORD,
    ),
    FieldDescriptor(
        section="monitor",
        name="index_names",
        type_=list,
        default=["test-index-a", "test-index-b"],
        required=True,
        env_var="LT_APP__MONITOR__INDEX_NAMES",
        widget=WidgetType.TEXT_AREA,
        item_type=str,
        label="index_names (comma-separated)",
    ),
    FieldDescriptor(
        section="monitor",
        name="output_csv_path",
        type_=Path,
        default=Path(""),
        required=True,
        env_var="LT_APP__MONITOR__OUTPUT_CSV_PATH",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="monitor",
        name="poll_interval_sec",
        type_=float,
        default=1.0,
        required=False,
        env_var="LT_APP__MONITOR__POLL_INTERVAL_SEC",
        widget=WidgetType.FLOAT,
        gt=0,
    ),
    FieldDescriptor(
        section="monitor",
        name="request_timeout_sec",
        type_=float,
        default=10.0,
        required=False,
        env_var="LT_APP__MONITOR__REQUEST_TIMEOUT_SEC",
        widget=WidgetType.FLOAT,
        gt=0,
    ),
    FieldDescriptor(
        section="monitor",
        name="use_ssl",
        type_=bool,
        default=True,
        required=False,
        env_var="LT_APP__MONITOR__USE_SSL",
        widget=WidgetType.CHECKBOX,
    ),
    FieldDescriptor(
        section="monitor",
        name="verify_certs",
        type_=bool,
        default=True,
        required=False,
        env_var="LT_APP__MONITOR__VERIFY_CERTS",
        widget=WidgetType.CHECKBOX,
    ),
    FieldDescriptor(
        section="monitor",
        name="ssl_assert_hostname",
        type_=bool,
        default=True,
        required=False,
        env_var="LT_APP__MONITOR__SSL_ASSERT_HOSTNAME",
        widget=WidgetType.CHECKBOX,
    ),
    FieldDescriptor(
        section="monitor",
        name="ssl_show_warn",
        type_=bool,
        default=False,
        required=False,
        env_var="LT_APP__MONITOR__SSL_SHOW_WARN",
        widget=WidgetType.CHECKBOX,
    ),
    FieldDescriptor(
        section="monitor",
        name="ca_certs_path",
        type_=Path,
        default=Path(""),
        required=False,
        env_var="LT_APP__MONITOR__CA_CERTS_PATH",
        widget=WidgetType.TEXT,
    ),
    # -- collector (14 fields) --
    FieldDescriptor(
        section="collector",
        name="bootstrap_servers",
        type_=str,
        default="localhost:9092",
        required=True,
        env_var="LT_APP__COLLECTOR__BOOTSTRAP_SERVERS",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="collector",
        name="topic",
        type_=str,
        default="etl.diagnostics.topic",
        required=True,
        env_var="LT_APP__COLLECTOR__TOPIC",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="collector",
        name="consumer_group_id",
        type_=str,
        default="etl-load-test-group",
        required=True,
        env_var="LT_APP__COLLECTOR__CONSUMER_GROUP_ID",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="collector",
        name="output_directory",
        type_=Path,
        default=Path(""),
        required=True,
        env_var="LT_APP__COLLECTOR__OUTPUT_DIRECTORY",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="collector",
        name="auto_offset_reset",
        type_=str,
        default="earliest",
        required=False,
        env_var="LT_APP__COLLECTOR__AUTO_OFFSET_RESET",
        widget=WidgetType.SELECT,
        options="earliest,latest",
    ),
    FieldDescriptor(
        section="collector",
        name="poll_timeout_ms",
        type_=int,
        default=1000,
        required=False,
        env_var="LT_APP__COLLECTOR__POLL_TIMEOUT_MS",
        widget=WidgetType.INT,
        gt=0,
    ),
    FieldDescriptor(
        section="collector",
        name="max_messages",
        type_=int,
        default=0,
        required=False,
        env_var="LT_APP__COLLECTOR__MAX_MESSAGES",
        widget=WidgetType.INT,
        label="max_messages (0 = unlimited)",
    ),
    FieldDescriptor(
        section="collector",
        name="security_protocol",
        type_=str,
        default="PLAINTEXT",
        required=False,
        env_var="LT_APP__COLLECTOR__SECURITY_PROTOCOL",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="collector",
        name="sasl_mechanism",
        type_=str,
        default="",
        required=False,
        env_var="LT_APP__COLLECTOR__SASL_MECHANISM",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="collector",
        name="sasl_username",
        type_=str,
        default="",
        required=False,
        env_var="LT_APP__COLLECTOR__SASL_USERNAME",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="collector",
        name="sasl_password",
        type_=str,
        default="",
        required=False,
        env_var="LT_APP__COLLECTOR__SASL_PASSWORD",
        widget=WidgetType.PASSWORD,
    ),
    FieldDescriptor(
        section="collector",
        name="ssl_cafile",
        type_=Path,
        default=Path(""),
        required=False,
        env_var="LT_APP__COLLECTOR__SSL_CAFILE",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="collector",
        name="ssl_certfile",
        type_=Path,
        default=Path(""),
        required=False,
        env_var="LT_APP__COLLECTOR__SSL_CERTFILE",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="collector",
        name="ssl_keyfile",
        type_=Path,
        default=Path(""),
        required=False,
        env_var="LT_APP__COLLECTOR__SSL_KEYFILE",
        widget=WidgetType.TEXT,
    ),
    # -- statistics (7 fields) --
    FieldDescriptor(
        section="statistics",
        name="source_directory",
        type_=Path,
        default=Path(""),
        required=True,
        env_var="LT_APP__STATISTICS__SOURCE_DIRECTORY",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="statistics",
        name="done_output_csv_path",
        type_=Path,
        default=Path(""),
        required=True,
        env_var="LT_APP__STATISTICS__DONE_OUTPUT_CSV_PATH",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="statistics",
        name="skipped_output_csv_path",
        type_=Path,
        default=Path(""),
        required=True,
        env_var="LT_APP__STATISTICS__SKIPPED_OUTPUT_CSV_PATH",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="statistics",
        name="read_failures_output_csv_path",
        type_=Path,
        default=Path(""),
        required=True,
        env_var="LT_APP__STATISTICS__READ_FAILURES_OUTPUT_CSV_PATH",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="statistics",
        name="report_glob",
        type_=str,
        default="*.json",
        required=False,
        env_var="LT_APP__STATISTICS__REPORT_GLOB",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="statistics",
        name="include_page_body_rows",
        type_=bool,
        default=True,
        required=False,
        env_var="LT_APP__STATISTICS__INCLUDE_PAGE_BODY_ROWS",
        widget=WidgetType.CHECKBOX,
    ),
    FieldDescriptor(
        section="statistics",
        name="sort_reports_by_name",
        type_=bool,
        default=True,
        required=False,
        env_var="LT_APP__STATISTICS__SORT_REPORTS_BY_NAME",
        widget=WidgetType.CHECKBOX,
    ),
    # -- projection (7 fields) --
    FieldDescriptor(
        section="projection",
        name="source_done_csv_path",
        type_=Path,
        default=Path(""),
        required=True,
        env_var="LT_APP__PROJECTION__SOURCE_DONE_CSV_PATH",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="projection",
        name="coefficients_output_csv_path",
        type_=Path,
        default=Path(""),
        required=True,
        env_var="LT_APP__PROJECTION__COEFFICIENTS_OUTPUT_CSV_PATH",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="projection",
        name="stage_projection_output_csv_path",
        type_=Path,
        default=Path(""),
        required=True,
        env_var="LT_APP__PROJECTION__STAGE_PROJECTION_OUTPUT_CSV_PATH",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="projection",
        name="total_projection_output_csv_path",
        type_=Path,
        default=Path(""),
        required=True,
        env_var="LT_APP__PROJECTION__TOTAL_PROJECTION_OUTPUT_CSV_PATH",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="projection",
        name="target_sizes_mb",
        type_=list,
        default=[1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 200.0, 500.0, 1000.0],
        required=False,
        env_var="LT_APP__PROJECTION__TARGET_SIZES_MB",
        widget=WidgetType.TEXT_AREA,
        item_type=float,
        label="target_sizes_mb (comma-separated)",
    ),
    FieldDescriptor(
        section="projection",
        name="allow_single_point_origin_fit",
        type_=bool,
        default=True,
        required=False,
        env_var="LT_APP__PROJECTION__ALLOW_SINGLE_POINT_ORIGIN_FIT",
        widget=WidgetType.CHECKBOX,
    ),
    FieldDescriptor(
        section="projection",
        name="clamp_negative_predictions",
        type_=bool,
        default=True,
        required=False,
        env_var="LT_APP__PROJECTION__CLAMP_NEGATIVE_PREDICTIONS",
        widget=WidgetType.CHECKBOX,
    ),
    # -- stage_size (2 fields) --
    FieldDescriptor(
        section="stage_size",
        name="source_done_csv_path",
        type_=Path,
        default=Path(""),
        required=True,
        env_var="LT_APP__STAGE_SIZE__SOURCE_DONE_CSV_PATH",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="stage_size",
        name="output_csv_path",
        type_=Path,
        default=Path(""),
        required=True,
        env_var="LT_APP__STAGE_SIZE__OUTPUT_CSV_PATH",
        widget=WidgetType.TEXT,
    ),
    # -- throughput (2 fields) --
    FieldDescriptor(
        section="throughput",
        name="source_monitor_csv_path",
        type_=Path,
        default=Path(""),
        required=True,
        env_var="LT_APP__THROUGHPUT__SOURCE_MONITOR_CSV_PATH",
        widget=WidgetType.TEXT,
    ),
    FieldDescriptor(
        section="throughput",
        name="output_csv_path",
        type_=Path,
        default=Path(""),
        required=True,
        env_var="LT_APP__THROUGHPUT__OUTPUT_CSV_PATH",
        widget=WidgetType.TEXT,
    ),
]

# ---------------------------------------------------------------------------
# Lookup indices (built once at import time)
# ---------------------------------------------------------------------------

_FIELD_BY_PATH: dict[str, FieldDescriptor] = {
    f"{fd.section}.{fd.name}": fd for fd in FIELD_REGISTRY
}

_FIELDS_BY_SECTION: dict[str, list[FieldDescriptor]] = {}
for _fd in FIELD_REGISTRY:
    _FIELDS_BY_SECTION.setdefault(_fd.section, []).append(_fd)

SECTION_ORDER: tuple[str, ...] = (
    "run",
    "generator",
    "monitor",
    "collector",
    "statistics",
    "projection",
    "stage_size",
    "throughput",
)

SECTION_LABELS: dict[str, str] = {
    "run": "Session Control",
    "generator": "Kafka Event Generator",
    "monitor": "OpenSearch Index Monitor",
    "collector": "Kafka Artifact Collector",
    "statistics": "ETL Report Statistics",
    "projection": "ETL Statistics Projection",
    "stage_size": "Stage/Size Aggregates",
    "throughput": "Throughput Aggregates",
}

# Fields whose values should be masked in UI diagnostics/logs (but NOT in YAML export).
_CREDENTIAL_FIELD_NAMES: frozenset[str] = frozenset(
    {"sasl_password", "password"}
)

# Set of section.field canonical paths for credentials — built once for fast lookup.
CREDENTIAL_PATHS: frozenset[str] = frozenset({
    f"{fd.section}.{fd.name}"
    for fd in FIELD_REGISTRY
    if fd.name in _CREDENTIAL_FIELD_NAMES
})


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_field(canonical_path: str) -> Optional[FieldDescriptor]:
    """Look up a field descriptor by ``section.name`` canonical path.

    :param canonical_path: Fully qualified field path, e.g. ``generator.max_lag``.
    :returns: :class:`FieldDescriptor` or ``None`` if the path is not in the registry.
    """
    return _FIELD_BY_PATH.get(canonical_path)


def get_section_fields(section: str) -> list[FieldDescriptor]:
    """Return all field descriptors for a given section.

    :param section: Section name, e.g. ``generator``.
    :returns: List of :class:`FieldDescriptor` in registry order.
    """
    return list(_FIELDS_BY_SECTION.get(section, []))


def default_settings() -> dict[str, dict[str, Any]]:
    """Build the defaults snapshot from the registry.

    Every field gets its ``default`` value.  Mutable defaults (lists) are
    deep-copied per-field so that callers receive independent objects.

    :returns: Nested ``{section: {field: value}}`` dict.
    """
    settings: dict[str, dict[str, Any]] = {}
    for fd in FIELD_REGISTRY:
        section_dict = settings.setdefault(fd.section, {})
        section_dict[fd.name] = copy.deepcopy(fd.default)
    return settings


def load_env_settings() -> dict[str, dict[str, Any]]:
    """Read ``os.environ`` and return a patch of resolved field values.

    Only environment variables matching ``LT_APP__<SECTION>__<FIELD>`` are
    consumed.  All other variables are ignored.  Values are coerced to the
    field's declared type.

    :returns: Nested ``{section: {field: value}}`` dict containing only the
        keys that were actually present in the environment.  Missing sections
        or fields are simply absent from the result.
    """
    patch: dict[str, dict[str, Any]] = {}
    for fd in FIELD_REGISTRY:
        raw = os.environ.get(fd.env_var)
        if raw is None or raw == "":
            continue
        try:
            value = _coerce_env_value(raw, fd)
        except (TypeError, ValueError):
            continue
        patch.setdefault(fd.section, {})[fd.name] = value
    return patch


def _coerce_env_value(raw: str, fd: FieldDescriptor) -> Any:
    """Coerce a raw environment variable string to the field's declared type.

    :param raw: Raw string from ``os.environ``.
    :param fd: :class:`FieldDescriptor` declaring the target type.
    :returns: Coerced value.
    :raises TypeError: If the type is not supported for env coercion.
    :raises ValueError: If the string cannot be parsed as the target type.
    """
    t = fd.type_
    if t is str:
        return raw
    if t is Path:
        return Path(raw)
    if t is int:
        return int(raw)
    if t is float:
        return float(raw)
    if t is bool:
        lowered = raw.strip().lower()
        if lowered in ("1", "true", "yes", "on"):
            return True
        if lowered in ("0", "false", "no", "off"):
            return False
        raise ValueError(f"Cannot coerce {raw!r} to bool for {fd.env_var}")
    if t is list:
        item_type = fd.item_type or str
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        return [item_type(p) for p in parts]
    raise TypeError(f"Unsupported env coercion type {t} for {fd.env_var}")


def merge_settings(
    base: dict[str, dict[str, Any]],
    *overlays: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Merge one or more setting overlays on top of a base, returning a new dict.

    Temporal precedence: later overlays win.  Only keys present in each overlay
    are overlaid; other keys in ``base`` are preserved as-is.

    :param base: Starting settings dict (typically ``default_settings()``).
    :param overlays: Zero or more overlay patches in order of increasing
        precedence (ENV, then YAML, then UI edits).
    :returns: A new merged ``{section: {field: value}}`` dict.
    """
    merged = copy.deepcopy(base)
    for overlay in overlays:
        for section, fields in overlay.items():
            target = merged.setdefault(section, {})
            for field_name, value in fields.items():
                target[field_name] = copy.deepcopy(value)
    return merged


def load_yaml_settings_from_text(
    yaml_text: str,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Parse a YAML string and return a registry-aligned settings patch.

    Unknown top-level keys are collected in ``ignored_paths`` and are never
    placed into the returned settings patch.  Known keys whose value is not a
    mapping are also ignored and reported.

    :param yaml_text: Raw YAML content as a string.
    :returns: ``(patch, ignored_paths)`` tuple.
    """
    data = yaml.safe_load(yaml_text)
    return _parse_yaml_data(data)


def load_yaml_settings_from_file(
    path: str | Path,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Read a YAML file and return a registry-aligned settings patch.

    :param path: Filesystem path to a ``.yaml`` or ``.yml`` file.
    :returns: ``(patch, ignored_paths)`` tuple.
    """
    raw = Path(path).read_text(encoding="utf-8")
    return load_yaml_settings_from_text(raw)


def _parse_yaml_data(
    data: Any,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Process a parsed YAML payload into a registry-aligned settings patch.

    :param data: The object returned by ``yaml.safe_load``.
    :returns: ``(patch, ignored_paths)`` tuple.
    """
    if not isinstance(data, dict):
        return {}, ["<root>: not a mapping"]

    patch: dict[str, dict[str, Any]] = {}
    ignored: list[str] = []
    known_sections = set(SECTION_ORDER)

    for section_key, section_value in data.items():
        section_key_str = str(section_key)
        if section_key_str not in known_sections:
            ignored.append(section_key_str)
            continue
        if not isinstance(section_value, dict):
            ignored.append(f"{section_key_str}: not a mapping")
            continue

        section_fields = {
            fd.name: fd
            for fd in _FIELDS_BY_SECTION.get(section_key_str, [])
        }
        section_patch: dict[str, Any] = {}
        for field_key, field_value in section_value.items():
            field_key_str = str(field_key)
            fd = section_fields.get(field_key_str)
            if fd is None:
                ignored.append(f"{section_key_str}.{field_key_str}")
                continue
            section_patch[field_key_str] = field_value

        if section_patch:
            patch[section_key_str] = section_patch

    return patch, ignored


def dump_yaml_settings(settings: dict[str, dict[str, Any]]) -> str:
    """Serialize the full nested settings dict to a YAML string.

    Credentials are included as-is (plain text).  Callers that need masked
    output should use :func:`mask_credentials_for_display` before calling this
    function if masking is desired for display/export.

    :param settings: Nested ``{section: {field: value}}`` dict.
    :returns: YAML string.
    """
    # Convert Path objects to strings so they render as plain paths in YAML.
    def _convert(value: Any) -> Any:
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, dict):
            return {k: _convert(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_convert(v) for v in value]
        return value

    sanitised = {section: _convert(fields) for section, fields in settings.items()}
    return yaml.dump(sanitised, default_flow_style=False, allow_unicode=True, sort_keys=False)


def validate_settings(
    settings: dict[str, dict[str, Any]],
) -> list[str]:
    """Validate settings against the registry.

    Checks: required fields must be non-empty/non-None; numeric fields must
    satisfy their ``gt``/``ge``/``le`` bounds.  Type coercion warnings are
    raised when a value's runtime type does not match the declared type.

    :param settings: Nested ``{section: {field: value}}`` dict.
    :returns: List of human-readable error messages (empty = valid).
    """
    errors: list[str] = []

    for fd in FIELD_REGISTRY:
        value = settings.get(fd.section, {}).get(fd.name)
        path = f"{fd.section}.{fd.name}"

        # --- required check ---
        if fd.required:
            if value is None:
                errors.append(f"{path}: required but missing (None)")
                continue
            if isinstance(value, (str, Path)) and str(value).strip() == "":
                errors.append(f"{path}: required but empty")
                continue
            if isinstance(value, list) and len(value) == 0:
                errors.append(f"{path}: required but empty list")
                continue

        if value is None:
            continue

        # --- numeric bounds checks ---
        if fd.gt is not None and isinstance(value, (int, float)):
            if value <= fd.gt:
                errors.append(
                    f"{path}: value {value} must be > {fd.gt}"
                )
        if fd.ge is not None and isinstance(value, (int, float)):
            if value < fd.ge:
                errors.append(
                    f"{path}: value {value} must be >= {fd.ge}"
                )
        if fd.le is not None and isinstance(value, (int, float)):
            if value > fd.le:
                errors.append(
                    f"{path}: value {value} must be <= {fd.le}"
                )

    return errors


def freeze_settings(
    settings: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Create a deep-frozen (immutable) snapshot for the worker.

    After this call, the returned dict should be treated as read-only.
    The caller should not mutate it further — it is the ``run_settings``
    passed to :class:`~lt_app.src.worker.SessionWorker`.

    :param settings: Mutable ``effective_settings`` dict.
    :returns: Deep-copied immutable snapshot.
    """
    return copy.deepcopy(settings)


def mask_credentials_for_display(
    settings: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Return a copy of settings with credential values replaced by ``***``.

    This is intended for UI diagnostics and logs only — never for YAML export
    or worker consumption.

    :param settings: Nested ``{section: {field: value}}`` dict.
    :returns: Shallow copy with credential fields masked.
    """
    masked = copy.deepcopy(settings)
    for path_str in CREDENTIAL_PATHS:
        section, _, field = path_str.partition(".")
        if section in masked and field in masked[section]:
            current = masked[section][field]
            if current is not None:
                masked[section][field] = "***"
    return masked