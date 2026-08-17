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

from lt_app.src import runtime_settings as rs
from lt_app.src.worker import SessionProgress, SessionResult, SessionWorker

RERUN_DELAY_SEC = 2.5

# ---------------------------------------------------------------------------
# Section A: Module defaults
# ---------------------------------------------------------------------------

# Aliases for brevity — all fields/sections come from the canonical registry.
_SECTION_ORDER = rs.SECTION_ORDER
_SECTION_LABELS = rs.SECTION_LABELS


# ---------------------------------------------------------------------------
# Section C + D: YAML export / import
# ---------------------------------------------------------------------------

def _render_one_widget(fd: rs.FieldDescriptor, value: Any, disabled: bool) -> Any:
    """Render one widget from a registry :class:`FieldDescriptor`."""
    widget_key = f"settings_{fd.section}_{fd.name}"
    label = fd.label if fd.label else fd.name

    if fd.widget == rs.WidgetType.CHECKBOX:
        return st.checkbox(label, value=bool(value), key=widget_key, disabled=disabled)
    elif fd.widget == rs.WidgetType.INT:
        safe = int(value) if value not in (None, "") else 0
        step = 1
        min_v = None
        max_v = None
        if fd.gt is not None:
            min_v = int(fd.gt) + 1
        elif fd.ge is not None:
            min_v = int(fd.ge)
        if fd.le is not None:
            max_v = int(fd.le)
        return st.number_input(label, value=safe, step=step,
                               min_value=min_v, max_value=max_v,
                               key=widget_key, disabled=disabled)
    elif fd.widget == rs.WidgetType.FLOAT:
        safe = float(value) if value not in (None, "") else 0.0
        step = 0.1
        min_v = None
        max_v = None
        if fd.gt is not None:
            min_v = float(fd.gt) + 0.01
        elif fd.ge is not None:
            min_v = float(fd.ge)
        if fd.le is not None:
            max_v = float(fd.le)
        return st.number_input(label, value=safe, step=step, format="%.2f",
                               min_value=min_v, max_value=max_v,
                               key=widget_key, disabled=disabled)
    elif fd.widget == rs.WidgetType.SELECT:
        opts = fd.options.split(",") if fd.options else []
        current = str(value)
        idx = opts.index(current) if current in opts else 0
        return st.selectbox(label, opts, index=idx, key=widget_key, disabled=disabled)
    elif fd.widget == rs.WidgetType.TEXT_AREA:
        list_display = ", ".join(str(v) for v in value) if isinstance(value, list) else str(value) if value is not None else ""
        text = st.text_area(label, value=list_display, key=widget_key, disabled=disabled)
        if fd.type_ is list:
            item_type = fd.item_type or str
            parts = [p.strip() for p in text.split(",") if p.strip()]
            return [item_type(p) for p in parts]
        return text
    elif fd.widget == rs.WidgetType.PASSWORD:
        return st.text_input(label, value=str(value) if value is not None else "",
                             type="password", key=widget_key, disabled=disabled)
    else:
        path_display = str(value) if isinstance(value, Path) else (str(value) if value is not None else "")
        return st.text_input(label, value=path_display, key=widget_key, disabled=disabled)


def render_settings_sidebar() -> None:
    """Render sidebar from the canonical registry with temporal precedence."""
    st.sidebar.header("Session Settings")
    running = st.session_state.phase == "running"

    # --- Pre-widget actions: YAML import, Apply ENV, Reset ---
    uploaded_file = st.sidebar.file_uploader(
        "Import settings from YAML", type=["yaml", "yml"], disabled=running,
    )
    if uploaded_file is not None:
        try:
            raw = uploaded_file.getvalue().decode("utf-8")
            yaml_data, ignored = rs.load_yaml_settings_from_text(raw)
            if st.sidebar.button("Apply imported settings", disabled=running):
                st.session_state.effective_settings = rs.merge_settings(
                    st.session_state.effective_settings, yaml_data
                )
                if ignored:
                    st.sidebar.warning(f"Ignored paths: {', '.join(ignored)}")
                # Sync widget state for every imported field
                for section, fields in yaml_data.items():
                    for field_name, field_value in fields.items():
                        fd = rs.get_field(f"{section}.{field_name}")
                        if fd is not None and fd.widget == rs.WidgetType.TEXT_AREA and isinstance(field_value, list):
                            st.session_state[f"settings_{section}_{field_name}"] = ", ".join(str(v) for v in field_value)
                        else:
                            st.session_state[f"settings_{section}_{field_name}"] = field_value
                st.session_state.validation_errors = []
                st.sidebar.success("Imported settings applied. Review the sidebar values, then start the test.")
        except yaml.YAMLError as exc:
            st.sidebar.error(f"YAML parse error: {exc}")

    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("Apply environment", disabled=running):
            env_patch = rs.load_env_settings()
            if env_patch:
                st.session_state.effective_settings = rs.merge_settings(
                    st.session_state.effective_settings, env_patch
                )
                for section, fields in env_patch.items():
                    for field_name, field_value in fields.items():
                        fd = rs.get_field(f"{section}.{field_name}")
                        if fd is not None and fd.widget == rs.WidgetType.TEXT_AREA and isinstance(field_value, list):
                            st.session_state[f"settings_{section}_{field_name}"] = ", ".join(str(v) for v in field_value)
                        else:
                            st.session_state[f"settings_{section}_{field_name}"] = field_value
                st.session_state.validation_errors = []
            else:
                st.sidebar.info("No LT_APP__* environment variables found.")
    with col2:
        if st.button("Reset to defaults", disabled=running):
            st.session_state.effective_settings = rs.default_settings()
            # Clear all widget state keys so widgets re-render from defaults
            for fd in rs.FIELD_REGISTRY:
                st.session_state.pop(f"settings_{fd.section}_{fd.name}", None)
            st.session_state.validation_errors = []

    # --- Section expanders (uniform, including run section) ---
    for section in rs.SECTION_ORDER:
        fields = rs.get_section_fields(section)
        if not fields:
            continue
        with st.sidebar.expander(rs.SECTION_LABELS[section], expanded=False):
            cfg = st.session_state.effective_settings.setdefault(section, {})
            for fd in fields:
                value = cfg.get(fd.name, fd.default)
                cfg[fd.name] = _render_one_widget(fd, value, running)

    # --- YAML Export ---
    st.sidebar.markdown("---")
    yaml_str = rs.dump_yaml_settings(st.session_state.effective_settings)
    st.sidebar.download_button(
        label="Export settings to YAML", data=yaml_str,
        file_name="session_settings.yaml", mime="text/yaml", disabled=running,
    )

    # --- Validation errors ---
    if st.session_state.validation_errors:
        st.sidebar.markdown("---")
        st.sidebar.error("**Validation errors:**")
        for err in st.session_state.validation_errors:
            st.sidebar.error(err)


# ---------------------------------------------------------------------------
# Section F: Background thread
# ---------------------------------------------------------------------------

def _run_worker_in_thread(
    run_settings: dict[str, dict[str, Any]],
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

    async def _build_worker() -> SessionWorker:
        return SessionWorker(
            run_settings=run_settings,
            session_id=session_id,
            progress_queue=progress_queue,
            report_interval_sec=report_interval_sec,
        )

    # SessionWorker.__init__ synchronously builds aiokafka clients that call
    # asyncio.get_running_loop(). The loop must actually be running here.
    worker = loop.run_until_complete(_build_worker())
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
    try:
        path = Path(path_str)
        if not path.is_file() or path.stat().st_size == 0:
            return None
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
                st.session_state.effective_settings.get(module_key, {}).get(path_key, "")
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
        "effective_settings": rs.default_settings(),
        "validation_errors": [],
        "phase": "idle",
        "worker": None,
        "worker_loop": None,
        "worker_future": None,
        "worker_thread": None,
        "final_result": None,
        "progress_events": collections.deque(maxlen=500),
        "session_id": f"session-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

    render_settings_sidebar()

    phase: str = st.session_state.phase

    if phase == "idle":
        if st.button("Start Test", type="primary"):
            effective = st.session_state.effective_settings
            validation_errors = rs.validate_settings(effective)
            if validation_errors:
                st.session_state.validation_errors = validation_errors
                st.error("Fix validation errors in the sidebar before starting.")
                st.stop()

            st.session_state.validation_errors = []
            run_settings = rs.freeze_settings(effective)

            progress_queue: queue.Queue[SessionProgress] = queue.Queue()
            st.session_state._progress_queue = progress_queue

            worker_ref: list[Optional[SessionWorker]] = [None]
            result_ref: list[Optional[SessionResult]] = [None]

            report_interval_min = float(
                effective.get("run", {}).get("report_interval_minutes", 5)
            )
            thread = threading.Thread(
                target=_run_worker_in_thread,
                args=(run_settings, progress_queue, worker_ref, result_ref,
                      st.session_state.session_id,
                      report_interval_min * 60),
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