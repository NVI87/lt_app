# app.py
"""Streamlit UI для управления одной тестовой сессией нагрузочного тестирования."""

from __future__ import annotations

import asyncio
import collections
import queue
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st
import streamlit.runtime.scriptrunner
import yaml

from lt_app.src.worker import SessionProgress, SessionResult, SessionWorker

RERUN_DELAY_SEC = 2.5

# ---------------------------------------------------------------------------
# Section A: Module defaults
# ---------------------------------------------------------------------------

MODULE_DEFAULTS: dict[str, dict[str, Any]] = {
    "generator": {
        "source_csv_path": "", "bootstrap_servers": "", "target_topic": "",
        "consumer_group_id": "", "max_lag": 3, "max_messages": 100,
        "lag_control_enabled": True, "lag_check_interval_sec": 1.0,
        "lag_check_every_messages": 1, "repeat_source": False,
        "preserve_source_partition": False, "timestamp_mode": "BROKER",
        "message_delay_sec": 0.0, "security_protocol": "PLAINTEXT",
    },
    "monitor": {
        "host": "", "port": 9200, "index_names": "", "output_csv_path": "",
        "poll_interval_sec": 1.0, "request_timeout_sec": 10.0,
        "use_ssl": True, "verify_certs": True,
        "ssl_assert_hostname": True, "ssl_show_warn": False,
    },
    "collector": {
        "bootstrap_servers": "", "topic": "", "consumer_group_id": "",
        "output_directory": "", "auto_offset_reset": "earliest",
        "poll_timeout_ms": 1000, "max_messages": 0, "security_protocol": "PLAINTEXT",
    },
    "statistics": {
        "source_directory": "", "done_output_csv_path": "",
        "skipped_output_csv_path": "", "read_failures_output_csv_path": "",
        "report_glob": "*.json", "include_page_body_rows": True,
        "sort_reports_by_name": True,
    },
    "projection": {
        "source_done_csv_path": "", "coefficients_output_csv_path": "",
        "stage_projection_output_csv_path": "", "total_projection_output_csv_path": "",
        "target_sizes_mb": "1,5,10,25,50,100,200,500,1000",
        "allow_single_point_origin_fit": True, "clamp_negative_predictions": True,
    },
    "stage_size": {
        "source_done_csv_path": "", "output_csv_path": "",
    },
    "throughput": {
        "source_monitor_csv_path": "", "output_csv_path": "",
    },
}

MODULE_LABELS: dict[str, str] = {
    "generator": "Kafka Event Generator",
    "monitor": "OpenSearch Index Monitor",
    "collector": "Kafka Artifact Collector",
    "statistics": "ETL Report Statistics",
    "projection": "ETL Statistics Projection",
    "stage_size": "Stage/Size Aggregates",
    "throughput": "Throughput Aggregates",
}

# Per-module widget specs: field_name → (widget_type, label_override_or_None)
WIDGET_SPECS: dict[str, list[tuple[str, str, Optional[str]]]] = {
    "generator": [
        ("source_csv_path", "text", None),
        ("bootstrap_servers", "text", None),
        ("target_topic", "text", None),
        ("consumer_group_id", "text", None),
        ("max_lag", "int", None),
        ("max_messages", "int", None),
        ("lag_control_enabled", "checkbox", None),
        ("lag_check_interval_sec", "float", None),
        ("lag_check_every_messages", "int", None),
        ("repeat_source", "checkbox", None),
        ("preserve_source_partition", "checkbox", None),
        ("timestamp_mode", "select", "BROKER,SOURCE"),
        ("message_delay_sec", "float", None),
        ("security_protocol", "text", None),
    ],
    "monitor": [
        ("host", "text", None),
        ("port", "int", None),
        ("index_names", "text_area", "index_names (comma-separated)"),
        ("output_csv_path", "text", None),
        ("poll_interval_sec", "float", None),
        ("request_timeout_sec", "float", None),
        ("use_ssl", "checkbox", None),
        ("verify_certs", "checkbox", None),
        ("ssl_assert_hostname", "checkbox", None),
        ("ssl_show_warn", "checkbox", None),
    ],
    "collector": [
        ("bootstrap_servers", "text", None),
        ("topic", "text", None),
        ("consumer_group_id", "text", None),
        ("output_directory", "text", None),
        ("auto_offset_reset", "select", "earliest,latest"),
        ("poll_timeout_ms", "int", None),
        ("max_messages", "int", "max_messages (0 = unlimited)"),
        ("security_protocol", "text", None),
    ],
    "statistics": [
        ("source_directory", "text", None),
        ("done_output_csv_path", "text", None),
        ("skipped_output_csv_path", "text", None),
        ("read_failures_output_csv_path", "text", None),
        ("report_glob", "text", None),
        ("include_page_body_rows", "checkbox", None),
        ("sort_reports_by_name", "checkbox", None),
    ],
    "projection": [
        ("source_done_csv_path", "text", None),
        ("coefficients_output_csv_path", "text", None),
        ("stage_projection_output_csv_path", "text", None),
        ("total_projection_output_csv_path", "text", None),
        ("target_sizes_mb", "text_area", "target_sizes_mb (comma-separated)"),
        ("allow_single_point_origin_fit", "checkbox", None),
        ("clamp_negative_predictions", "checkbox", None),
    ],
    "stage_size": [
        ("source_done_csv_path", "text", None),
        ("output_csv_path", "text", None),
    ],
    "throughput": [
        ("source_monitor_csv_path", "text", None),
        ("output_csv_path", "text", None),
    ],
}


# ---------------------------------------------------------------------------
# Section B: settings -> Pydantic objects (validation)
# ---------------------------------------------------------------------------

def _parse_path(value: Any) -> Optional[Path]:
    if value is None:
        return None
    s = str(value).strip()
    return Path(s) if s else None


def _parse_optional_int(value: Any) -> Optional[int]:
    if value is None or str(value).strip() in ("", "0"):
        return None
    return int(value)


def _parse_list_str(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    s = str(value).strip()
    if not s:
        return []
    return [item.strip() for item in s.split(",") if item.strip()]


def _parse_list_float(value: Any) -> list[float]:
    if isinstance(value, list):
        return [float(v) for v in value]
    s = str(value).strip()
    if not s:
        return [1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 200.0, 500.0, 1000.0]
    return [float(item.strip()) for item in s.split(",") if item.strip()]


def build_settings_objects(
    settings_dict: dict[str, dict[str, Any]],
) -> Optional[dict[str, Any]]:
    """Собрать Pydantic Settings из словарей, сохранить validation_errors."""
    from lt_app.src.etl_report_statistics import EtlReportStatisticsSettings
    from lt_app.src.etl_stage_size_aggregates import EtlStageSizeAggregateSettings
    from lt_app.src.etl_statistics_projection import EtlStatisticsProjectionSettings
    from lt_app.src.etl_throughput_aggregates import EtlThroughputAggregateSettings
    from lt_app.src.kafka_event_generator import KafkaEventGeneratorSettings, KafkaTimestampMode
    from lt_app.src.kafka_json_artifact_collector import KafkaArtifactCollectorSettings, KafkaOffsetResetPolicy
    from lt_app.src.opensearch_index_monitor import OpenSearchIndexMonitorSettings

    errors: dict[str, str] = {}
    result: dict[str, Any] = {}

    try:
        g = settings_dict["generator"]
        result["generator"] = KafkaEventGeneratorSettings(
            source_csv_path=_parse_path(g["source_csv_path"]),
            bootstrap_servers=str(g["bootstrap_servers"]),
            target_topic=str(g["target_topic"]),
            consumer_group_id=str(g["consumer_group_id"]),
            max_lag=int(g["max_lag"]), max_messages=int(g["max_messages"]),
            lag_control_enabled=bool(g["lag_control_enabled"]),
            lag_check_interval_sec=float(g["lag_check_interval_sec"]),
            lag_check_every_messages=int(g["lag_check_every_messages"]),
            repeat_source=bool(g["repeat_source"]),
            preserve_source_partition=bool(g["preserve_source_partition"]),
            timestamp_mode=KafkaTimestampMode(str(g["timestamp_mode"])),
            message_delay_sec=float(g["message_delay_sec"]),
            security_protocol=str(g["security_protocol"]),
        )
    except Exception as exc:
        errors["generator"] = str(exc)

    try:
        m = settings_dict["monitor"]
        result["monitor"] = OpenSearchIndexMonitorSettings(
            host=str(m["host"]), port=int(m["port"]),
            index_names=_parse_list_str(m["index_names"]),
            output_csv_path=_parse_path(m["output_csv_path"]),
            poll_interval_sec=float(m["poll_interval_sec"]),
            request_timeout_sec=float(m["request_timeout_sec"]),
            use_ssl=bool(m["use_ssl"]), verify_certs=bool(m["verify_certs"]),
            ssl_assert_hostname=bool(m["ssl_assert_hostname"]),
            ssl_show_warn=bool(m["ssl_show_warn"]),
        )
    except Exception as exc:
        errors["monitor"] = str(exc)

    try:
        c = settings_dict["collector"]
        result["collector"] = KafkaArtifactCollectorSettings(
            bootstrap_servers=str(c["bootstrap_servers"]),
            topic=str(c["topic"]),
            consumer_group_id=str(c["consumer_group_id"]),
            output_directory=_parse_path(c["output_directory"]),
            auto_offset_reset=KafkaOffsetResetPolicy(str(c["auto_offset_reset"])),
            poll_timeout_ms=int(c["poll_timeout_ms"]),
            max_messages=_parse_optional_int(c["max_messages"]),
            security_protocol=str(c["security_protocol"]),
        )
    except Exception as exc:
        errors["collector"] = str(exc)

    try:
        s = settings_dict["statistics"]
        result["statistics"] = EtlReportStatisticsSettings(
            source_directory=_parse_path(s["source_directory"]),
            done_output_csv_path=_parse_path(s["done_output_csv_path"]),
            skipped_output_csv_path=_parse_path(s["skipped_output_csv_path"]),
            read_failures_output_csv_path=_parse_path(s["read_failures_output_csv_path"]),
            report_glob=str(s["report_glob"]),
            include_page_body_rows=bool(s["include_page_body_rows"]),
            sort_reports_by_name=bool(s["sort_reports_by_name"]),
        )
    except Exception as exc:
        errors["statistics"] = str(exc)

    try:
        p = settings_dict["projection"]
        result["projection"] = EtlStatisticsProjectionSettings(
            source_done_csv_path=_parse_path(p["source_done_csv_path"]),
            coefficients_output_csv_path=_parse_path(p["coefficients_output_csv_path"]),
            stage_projection_output_csv_path=_parse_path(p["stage_projection_output_csv_path"]),
            total_projection_output_csv_path=_parse_path(p["total_projection_output_csv_path"]),
            target_sizes_mb=_parse_list_float(p["target_sizes_mb"]),
            allow_single_point_origin_fit=bool(p["allow_single_point_origin_fit"]),
            clamp_negative_predictions=bool(p["clamp_negative_predictions"]),
        )
    except Exception as exc:
        errors["projection"] = str(exc)

    try:
        ss = settings_dict["stage_size"]
        result["stage_size"] = EtlStageSizeAggregateSettings(
            source_done_csv_path=_parse_path(ss["source_done_csv_path"]),
            output_csv_path=_parse_path(ss["output_csv_path"]),
        )
    except Exception as exc:
        errors["stage_size"] = str(exc)

    try:
        t = settings_dict["throughput"]
        result["throughput"] = EtlThroughputAggregateSettings(
            source_monitor_csv_path=_parse_path(t["source_monitor_csv_path"]),
            output_csv_path=_parse_path(t["output_csv_path"]),
        )
    except Exception as exc:
        errors["throughput"] = str(exc)

    st.session_state.validation_errors = errors
    return None if errors else result


# ---------------------------------------------------------------------------
# Section C + D: YAML export / import
# ---------------------------------------------------------------------------

def settings_to_yaml(settings_dict: dict[str, dict[str, Any]]) -> str:
    return yaml.safe_dump(settings_dict, allow_unicode=True, sort_keys=False)


def apply_yaml_import(
    settings_dict: dict[str, dict[str, Any]], yaml_data: Any,
) -> None:
    if not isinstance(yaml_data, dict):
        st.error("YAML root must be a dict with module keys (generator, monitor, ...)")
        return
    for module_name in MODULE_DEFAULTS:
        if module_name in yaml_data:
            loaded = yaml_data[module_name]
            if isinstance(loaded, dict):
                settings_dict[module_name].update(loaded)
            else:
                st.warning(f"YAML key '{module_name}' is not a dict — skipping")
    build_settings_objects(settings_dict)


# ---------------------------------------------------------------------------
# Section E: Sidebar — settings form
# ---------------------------------------------------------------------------

def _render_one_widget(
    module_name: str, field: str, value: Any, widget_type: str, label: Optional[str],
    disabled: bool,
) -> Any:
    """Render one widget based on spec and return its value."""
    widget_label = label if label else field
    widget_key = f"settings_{module_name}_{field}"

    if widget_type == "checkbox":
        return st.checkbox(widget_label, value=bool(value), key=widget_key, disabled=disabled)
    elif widget_type == "int":
        return st.number_input(widget_label, value=int(value) if value not in (None, "") else 0,
                               step=1, key=widget_key, disabled=disabled)
    elif widget_type == "float":
        return st.number_input(widget_label, value=float(value) if value not in (None, "") else 0.0,
                               step=0.1, format="%.2f", key=widget_key, disabled=disabled)
    elif widget_type == "select":
        opts = (label or "").split(",")
        current = str(value)
        idx = opts.index(current) if current in opts else 0
        return st.selectbox(field, opts, index=idx, key=widget_key, disabled=disabled)
    elif widget_type == "text_area":
        return st.text_area(widget_label, value=str(value) if value is not None else "",
                            key=widget_key, disabled=disabled)
    else:
        return st.text_input(widget_label, value=str(value) if value is not None else "",
                             key=widget_key, disabled=disabled)


def render_settings_sidebar() -> None:
    """Нарисовать сайдбар с настройками всех пяти модулей."""
    st.sidebar.header("Session Settings")
    running = st.session_state.phase == "running"

    for module_name in MODULE_DEFAULTS:
        with st.sidebar.expander(MODULE_LABELS[module_name], expanded=False):
            cfg = st.session_state.settings[module_name]
            for field, widget_type, label in WIDGET_SPECS[module_name]:
                cfg[field] = _render_one_widget(
                    module_name, field, cfg[field], widget_type, label, running,
                )

    # YAML Export
    st.sidebar.markdown("---")
    yaml_str = settings_to_yaml(st.session_state.settings)
    st.sidebar.download_button(
        label="Export settings to YAML", data=yaml_str,
        file_name="session_settings.yaml", mime="text/yaml", disabled=running,
    )

    # YAML Import
    st.sidebar.markdown("---")
    uploaded_file = st.sidebar.file_uploader(
        "Import settings from YAML", type=["yaml", "yml"], disabled=running,
    )
    if uploaded_file is not None:
        try:
            raw = uploaded_file.read().decode("utf-8")
            yaml_data = yaml.safe_load(raw)
            if st.sidebar.button("Apply imported settings", disabled=running):
                apply_yaml_import(st.session_state.settings, yaml_data)
                st.rerun()
        except yaml.YAMLError as exc:
            st.sidebar.error(f"YAML parse error: {exc}")

    # Validation errors
    if st.session_state.validation_errors:
        st.sidebar.markdown("---")
        st.sidebar.error("**Validation errors:**")
        for mod, err in st.session_state.validation_errors.items():
            st.sidebar.error(f"{mod}: {err}")

    # Session Control
    if st.session_state.get("report_interval_minutes") is None:
        st.session_state.report_interval_minutes = 5
    with st.sidebar.expander("Session Control", expanded=False):
        st.session_state.report_interval_minutes = st.number_input(
            "Report snapshot interval (minutes)",
            min_value=1, max_value=60, step=1,
            value=st.session_state.report_interval_minutes,
            key="settings_report_interval_minutes",
            disabled=running,
        )


# ---------------------------------------------------------------------------
# Section F: Background thread
# ---------------------------------------------------------------------------

def _run_worker_in_thread(
    settings_objects: dict[str, Any],
    progress_queue: queue.Queue[SessionProgress],
    worker_ref: list[Optional[SessionWorker]],
    result_ref: list[Optional[SessionResult]],
    session_id: str,
    report_interval_sec: float,
) -> None:
    ctx = streamlit.runtime.scriptrunner.get_script_run_ctx()
    if ctx is not None:
        streamlit.runtime.scriptrunner.add_script_run_ctx(threading.current_thread(), ctx)

    loop = asyncio.new_event_loop()
    st.session_state.worker_loop = loop
    asyncio.set_event_loop(loop)

    worker = SessionWorker(
        generator_settings=settings_objects["generator"],
        monitor_settings=settings_objects["monitor"],
        collector_settings=settings_objects["collector"],
        statistics_settings=settings_objects["statistics"],
        projection_settings=settings_objects["projection"],
        stage_size_settings=settings_objects["stage_size"],
        throughput_settings=settings_objects["throughput"],
        session_id=session_id,
        progress_queue=progress_queue,
        report_interval_sec=report_interval_sec,
    )
    worker_ref[0] = worker
    st.session_state.worker = worker

    future = asyncio.run_coroutine_threadsafe(worker.run(), loop)
    st.session_state.worker_future = future
    future.add_done_callback(lambda _f: loop.call_soon_threadsafe(loop.stop))

    try:
        loop.run_forever()
    finally:
        result_ref[0] = future.result()
        loop.close()


# ---------------------------------------------------------------------------
# Section G: UI helpers
# ---------------------------------------------------------------------------

def _safe_read_preview_csv(path_str: str) -> Optional[pd.DataFrame]:
    """Безопасно прочитать CSV для превью; вернуть None при любой ошибке."""
    if not path_str:
        return None
    path = Path(path_str)
    if not path.is_file():
        return None
    if path.stat().st_size == 0:
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def _render_download_button(label: str, csv_path: Optional[Path]) -> None:
    """Показать кнопку скачивания CSV или caption о недоступности."""
    if csv_path is None or not csv_path.is_file():
        st.caption(f"{label}: not available")
        return
    try:
        data = csv_path.read_bytes()
    except OSError:
        st.caption(f"{label}: cannot read file")
        return
    st.download_button(
        label=label, data=data, file_name=csv_path.name, mime="text/csv",
    )


# ---------------------------------------------------------------------------
# Section H: Main UI area
# ---------------------------------------------------------------------------

def _drain_progress_queue() -> None:
    pq = st.session_state.get("_progress_queue")
    if pq is None:
        return
    while True:
        try:
            st.session_state.progress_events.append(pq.get_nowait())
        except queue.Empty:
            break


def _render_phase_running() -> None:
    worker: Optional[SessionWorker] = st.session_state.get("worker")
    worker_loop: Optional[asyncio.AbstractEventLoop] = st.session_state.get("worker_loop")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Stop Test", type="primary"):
            if worker is not None and worker_loop is not None:
                worker_loop.call_soon_threadsafe(worker.stop)

    time.sleep(1)

    future = st.session_state.worker_future
    if future is not None and future.done():
        try:
            result = future.result()
        except Exception as exc:
            result = SessionResult(
                session_id=st.session_state.session_id,
                error=f"{type(exc).__name__}: {exc}",
            )
        st.session_state.final_result = result
        st.session_state.phase = "failed" if result.error else "done"
        st.rerun()

    _drain_progress_queue()
    events: collections.deque[SessionProgress] = st.session_state.progress_events

    if events:
        last = events[-1]
        st.subheader(f"Phase: {last.phase}")

    gen_events = [e for e in events if e.generator is not None]
    if gen_events:
        with st.expander(f"Generator ({len(gen_events)} events, last 5)", expanded=True):
            for e in list(gen_events)[-5:]:
                g = e.generator
                st.text(f"sent={g.sent_messages}/{g.max_messages}  lag={g.current_lag}  "
                        f"offset={g.source_offset} part={g.source_partition}")

    mon_events = [e for e in events if e.monitor is not None]
    if mon_events:
        with st.expander(f"OpenSearch Monitor ({len(mon_events)} events, last 5)", expanded=True):
            for e in list(mon_events)[-5:]:
                m = e.monitor
                st.text(f"index={m.index_name}  count={m.document_count}  error={m.error}")

    col_events = [e for e in events if e.collector is not None]
    if col_events:
        with st.expander(f"Artifact Collector ({len(col_events)} events, last 5)", expanded=True):
            for e in list(col_events)[-5:]:
                c = e.collector
                st.text(f"topic={c.topic} part={c.partition} offset={c.offset}  "
                        f"saved={c.saved_messages}  path={c.output_path}")

    # Aggregate previews
    for label, module_key, path_key in [
        ("Stage/Size Aggregates (preview)", "stage_size", "output_csv_path"),
        ("Throughput Aggregates (preview)", "throughput", "output_csv_path"),
    ]:
        with st.expander(label, expanded=False):
            path_str = str(
                st.session_state.settings.get(module_key, {}).get(path_key, "")
            )
            df = _safe_read_preview_csv(path_str)
            if df is not None:
                st.dataframe(df)
            else:
                st.caption(f"{label.split(' (preview)')[0]}: not available yet")

    time.sleep(RERUN_DELAY_SEC)
    st.rerun()


def _render_phase_done_or_failed() -> None:
    result: Optional[SessionResult] = st.session_state.get("final_result")

    if result is None:
        st.error("No result found")
        return

    if result.error:
        st.error(f"Session failed: {result.error}")
    else:
        st.success("Session completed successfully")

    if result.generator:
        g = result.generator
        st.metric("Messages Sent", g.sent_messages)
        st.metric("Final Lag", g.last_lag)

    if result.monitor:
        m = result.monitor
        st.metric("Monitor Cycles", m.completed_cycles)
        st.caption(f"CSV: {m.output_csv_path}")

    if result.collector:
        c = result.collector
        st.metric("Artifacts Saved", c.saved_messages)
        st.caption(f"Directory: {c.output_directory}")

    if result.statistics:
        s = result.statistics
        st.metric("Reports Processed", s.processed_reports)
        col1, col2, col3 = st.columns(3)
        col1.metric("DONE rows", s.done_rows)
        col2.metric("Skipped rows", s.skipped_rows)
        col3.metric("Read failures", s.read_failures)

    if result.projection:
        p = result.projection
        st.metric("Stage Models", p.stage_models)

    if result.stage_size_aggregates:
        ssa = result.stage_size_aggregates
        st.metric("Stage/Size Source Rows", ssa.source_rows)
        st.metric("Aggregate Rows", ssa.aggregate_rows)
        st.metric("Excluded Unsized Rows", ssa.excluded_unsized_rows)
        ssa_df = _safe_read_preview_csv(str(ssa.output_csv_path))
        if ssa_df is not None:
            st.subheader("Stage/Size Aggregates")
            st.dataframe(ssa_df)

    if result.throughput_aggregates:
        ta = result.throughput_aggregates
        st.metric("Throughput Minute Buckets", ta.minute_buckets)
        st.metric("Throughput Source Rows", ta.source_rows)
        ta_df = _safe_read_preview_csv(str(ta.output_csv_path))
        if ta_df is not None:
            st.subheader("Throughput Aggregates")
            st.dataframe(ta_df)

    if result.artifacts_dir:
        st.caption(f"Artifacts: {result.artifacts_dir}")

    # Download buttons for all CSV artifacts
    st.subheader("Download Artifacts")
    if result.statistics:
        s = result.statistics
        _render_download_button("DONE CSV", s.done_output_csv_path)
        _render_download_button("Skipped CSV", s.skipped_output_csv_path)
        _render_download_button("Read Failures CSV", s.read_failures_output_csv_path)
    if result.projection:
        p = result.projection
        _render_download_button("Coefficients CSV", p.coefficients_output_csv_path)
        _render_download_button("Stage Projection CSV", p.stage_projection_output_csv_path)
        _render_download_button("Total Projection CSV", p.total_projection_output_csv_path)
    if result.stage_size_aggregates:
        _render_download_button(
            "Stage/Size Aggregates CSV",
            result.stage_size_aggregates.output_csv_path,
        )
    if result.throughput_aggregates:
        _render_download_button(
            "Throughput CSV",
            result.throughput_aggregates.output_csv_path,
        )

    _, col_right = st.columns([3, 1])
    with col_right:
        if st.button("New Session"):
            st.session_state.phase = "idle"
            st.session_state.worker = None
            st.session_state.worker_loop = None
            st.session_state.worker_future = None
            st.session_state.final_result = None
            st.session_state.progress_events = collections.deque(maxlen=500)
            st.rerun()


# ---------------------------------------------------------------------------
# Section I: Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(page_title="LT App — Load Test Session", layout="wide")
    st.title("LT App — Load Test Session")

    # Init session_state
    defaults = {
        "settings": {m: dict(d) for m, d in MODULE_DEFAULTS.items()},
        "validation_errors": {},
        "phase": "idle",
        "worker": None,
        "worker_loop": None,
        "worker_future": None,
        "worker_thread": None,
        "final_result": None,
        "progress_events": collections.deque(maxlen=500),
        "session_id": f"session-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        "report_interval_minutes": 5,
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

    render_settings_sidebar()

    phase: str = st.session_state.phase

    if phase == "idle":
        if st.button("Start Test", type="primary"):
            settings_objects = build_settings_objects(st.session_state.settings)
            if settings_objects is None:
                st.error("Fix validation errors in the sidebar before starting.")
                st.stop()

            progress_queue: queue.Queue[SessionProgress] = queue.Queue()
            st.session_state._progress_queue = progress_queue

            worker_ref: list[Optional[SessionWorker]] = [None]
            result_ref: list[Optional[SessionResult]] = [None]

            thread = threading.Thread(
                target=_run_worker_in_thread,
                args=(settings_objects, progress_queue, worker_ref, result_ref,
                      st.session_state.session_id,
                      float(st.session_state.report_interval_minutes * 60)),
                daemon=True,
            )
            st.session_state.worker_thread = thread
            st.session_state.phase = "running"
            thread.start()
            st.rerun()

    elif phase == "running":
        _render_phase_running()

    elif phase in ("done", "failed"):
        _render_phase_done_or_failed()


main()