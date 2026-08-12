# Task: Periodic report aggregates for load-test ETL session

## Context you must read before writing any code

This task extends an existing load-test orchestration app (`lt_app`) with five
existing modules: `kafka_event_generator.py`, `opensearch_index_monitor.py`,
`kafka_json_artifact_collector.py`, `etl_report_statistics.py`,
`etl_statistics_projection.py`, orchestrated by `worker.py`
(`SessionWorker`), rendered by `app.py` (Streamlit UI).

Do NOT guess field semantics. Read the actual source files listed below before
writing code for any subtask. Do not modify files outside the subtask's
explicit scope.

Pipeline being measured: each attachment goes through five FSM stages, in
this fixed order:

```
EXTRACTING -> CONVERTING -> TOKENIZING -> VECTORIZING -> INDEXING
```

`EXTRACTING`, `VECTORIZING`, `INDEXING` are async I/O-bound stages.
`CONVERTING`, `TOKENIZING` are heavy synchronous CPU/memory-bound stages.

## Global constraints (apply to every subtask)

- Python 3.11, pandas/numpy already available in the project — no new
  third-party dependencies.
- Every new builder module must follow the existing style: a
  `pydantic_settings.BaseSettings` settings class with its own `env_prefix`,
  frozen `@dataclass(slots=True)` progress/result types, a
  `Optional[Callable[[ProgressType], None]] = None` progress callback with
  try/except-suppressed delivery, an async `run(self, stop_event: asyncio.Event)`
  method, and CPU/IO-heavy work wrapped in `asyncio.to_thread`. Match the
  docstring conventions already used in `etl_statistics_projection.py`.
- Never divide by zero. Any group/window where a weight sum or sample count is
  zero must produce `None`/`NaN` for that specific derived value, not raise.
- Work in your own git branch/worktree. Start with a short written plan
  (functions to add/touch) before editing code. Implement and get one subtask
  reviewed before starting the next — do not batch all subtasks into one diff.
- Write unit tests for every new pure-function module (A and B) using small
  in-memory or fixture CSVs before touching the orchestration layer (C) or UI (D).

---

## Subtask A — stage/size aggregate builder

New file: `lt_app/src/etl_stage_size_aggregates.py`.

### Input

Read the file at `EtlReportStatisticsResult.done_output_csv_path` (same CSV
already produced by `EtlReportStatisticsBuilder`, columns defined by
`DONE_COLUMNS` in `etl_report_statistics.py`: `report_file, document_id,
document_name, attach_id, attach_name, attach_type, attach_size, stage,
status, wall_time_sec, cpu_mean_pct, cpu_peak_pct, rss_mean, rss_peak,
samples, started_at`). Read `etl_report_statistics.py` in full before coding
to confirm column meaning and nullability (rows with `stage=None` are
`page_body` rows for attachment-less documents and must be excluded from this
aggregate — filter on `stage.notna()` and `attach_type != "page_body"`).

### Size bucketing

Fixed size grid in MB (powers of two, matches the test file generator):
`[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]`.

Bucketing rule: convert `attach_size` (bytes) to MB (`/ (1024*1024)`), then
assign to the nearest grid point using `round(log2(size_mb))` clamped to the
grid's exponent range (`0..10`), i.e. `bucket_mb = 2 ** clamp(round(log2(size_mb)), 0, 10)`.
Rows with `attach_size` null, zero, or negative are excluded from this
aggregate (they cannot be size-bucketed) and must be counted and reported
separately (see result dataclass below), not silently dropped.

### Grouping and metrics

Group by `(attach_type, size_bucket_mb, stage)`. For each group compute:

- `row_count`: number of rows in the group.
- `total_samples`: `sum(samples)`, treating null `samples` as 0.
- `total_wall_time_sec`: `sum(wall_time_sec)`, ignoring nulls.
- `weighted_mean_cpu_pct`: `sum(cpu_mean_pct * samples) / sum(samples)` over
  rows where both `cpu_mean_pct` and `samples` are non-null and `samples > 0`;
  if the resulting weight sum is 0, the value is `None`.
- `peak_cpu_pct`: `max(cpu_peak_pct)` ignoring nulls; `None` if all null.
- `weighted_mean_rss_bytes`: same weighted formula as CPU, using `rss_mean`.
- `peak_rss_bytes`: `max(rss_peak)` ignoring nulls; `None` if all null.

### Output

Write CSV to a configurable `output_csv_path` with columns exactly:
`attach_type, size_bucket_mb, stage, row_count, total_samples,
total_wall_time_sec, weighted_mean_cpu_pct, peak_cpu_pct,
weighted_mean_rss_bytes, peak_rss_bytes`. Sort rows by
`(attach_type, size_bucket_mb, stage)`. Always write the CSV (with header,
zero rows) even if the source has no eligible rows, matching the
`_write_csv_artifacts` pattern in `etl_report_statistics.py`.

### Required types

```python
class EtlStageSizeAggregateSettings(BaseSettings):
    # env_prefix="ETL_STAGE_SIZE_AGGREGATES_"
    source_done_csv_path: Path
    output_csv_path: Path

@dataclass(frozen=True, slots=True)
class EtlStageSizeAggregateProgress:
    phase: str  # "loading", "aggregating", "writing"

@dataclass(frozen=True, slots=True)
class EtlStageSizeAggregateResult:
    source_rows: int
    aggregate_rows: int
    excluded_unsized_rows: int  # rows dropped for missing/invalid attach_size
    stopped_by_request: bool
    output_csv_path: Path

class EtlStageSizeAggregateBuilder:
    def __init__(self, settings: EtlStageSizeAggregateSettings,
                 progress_callback: Optional[Callable[[EtlStageSizeAggregateProgress], None]] = None) -> None: ...
    async def run(self, stop_event: asyncio.Event) -> EtlStageSizeAggregateResult: ...
```

### Acceptance test

Unit test with a small in-memory DataFrame/CSV fixture covering: two
attach_types, at least two size buckets, all five stages, one group with
`samples=0` for all rows (must yield `weighted_mean_*=None`, not an error),
one row with `attach_size` null (must be excluded and counted in
`excluded_unsized_rows`), and one `stage=None` page_body row (must be
excluded from grouping entirely).

---

## Subtask B — Kafka/OpenSearch throughput aggregate builder

New file: `lt_app/src/etl_throughput_aggregates.py`.

### Input

Read `etl_report_statistics.py`... no — read `opensearch_index_monitor.py`
in full first. Input is the CSV written by `OpenSearchIndexMonitor`
(columns: `timestamp` ISO-8601, `index_name`, `document_count`, `error`).
Rows with non-null `error` must be excluded from aggregation (failed count
requests carry no valid `document_count`).

The second input is not a file: it is `sent_messages`, an `int`, passed as a
constructor/run argument — the latest known value of
`KafkaGeneratorResult.sent_messages` (or `KafkaGeneratorProgress.sent_messages`
if the generator is still running when the periodic snapshot fires). The
caller (Subtask C) is responsible for supplying this value; this module must
not read Kafka or the generator's CSV directly.

### Aggregation across indices

At each observed timestamp, indices are polled in a full cycle by the
monitor. Sum `document_count` across all `index_name` values observed at
(or nearest to) that cycle to get a single scalar time series of "total
indexed documents over time." Resample this series into 1-minute buckets
using the last observed cumulative value in each minute (i.e., for each
minute boundary, take the most recent total `document_count` sum at or
before that boundary).

### Metrics

- `coverage_ratio`: `total_indexed_documents_at_latest_point / sent_messages`,
  where the numerator is the last available total across all indices. If
  `sent_messages` is 0 or the series is empty, this value is `None`.
- Per-minute delta series: `delta[n] = total[minute n] - total[minute n-1]`
  for `n >= 1` (first minute has no delta and is excluded from avg/max).
- `avg_per_minute`, `max_per_minute`: mean and max of the delta series
  (`None` if the delta series has zero elements).
- 5-minute centered or trailing rolling mean of the delta series (trailing
  window, i.e. `rolling(window=5, min_periods=1).mean()` — use trailing to
  avoid look-ahead bias since this runs on live, in-progress data); then
  `avg_per_minute_smoothed_5min` and `max_per_minute_smoothed_5min` are the
  mean and max of that smoothed series.

### Output

Single-row CSV with columns: `computed_at, total_indexed_documents,
total_sent_messages, coverage_ratio, avg_per_minute, max_per_minute,
avg_per_minute_smoothed_5min, max_per_minute_smoothed_5min`. `computed_at` is
the UTC timestamp of when this aggregate was computed (not derived from
input data). Always write the CSV even with a single row of `None`s if input
is empty.

### Required types

```python
class EtlThroughputAggregateSettings(BaseSettings):
    # env_prefix="ETL_THROUGHPUT_AGGREGATES_"
    source_monitor_csv_path: Path
    output_csv_path: Path

@dataclass(frozen=True, slots=True)
class EtlThroughputAggregateResult:
    source_rows: int
    minute_buckets: int
    stopped_by_request: bool
    output_csv_path: Path

class EtlThroughputAggregateBuilder:
    def __init__(self, settings: EtlThroughputAggregateSettings,
                 progress_callback: Optional[Callable[[Any], None]] = None) -> None: ...
    async def run(self, stop_event: asyncio.Event, sent_messages: int) -> EtlThroughputAggregateResult: ...
```

### Acceptance test

Unit test with a synthetic multi-index time series with a known, hand-
computed per-minute delta (e.g. two indices, each growing by a fixed
count per minute for 10 minutes, with one deliberate flat/error minute) —
assert `avg_per_minute`/`max_per_minute`/smoothed values match manual
calculation exactly. Also test `coverage_ratio` with `sent_messages=0`
(must be `None`, not `ZeroDivisionError`).

---

## Subtask C — periodic snapshot timer in worker.py

**Read `worker.py` in full before touching it.** This subtask is the highest
risk in this task: do not modify `_run_phase1`, `_wait_remaining`, or
`_emit_progress`. Only add new code.

### Goal

While phase 1 (`_run_phase1`: generator + monitor + collector running in
parallel) is in progress, periodically (every `report_interval_sec`, a new
settings field, valid range 1–60 minutes, i.e. 60–3600 seconds) recompute all
five report artifacts from the current on-disk snapshot:

1. `EtlReportStatisticsBuilder.run(...)` (existing)
2. `EtlStatisticsProjectionBuilder.run(...)` (existing)
3. `EtlStageSizeAggregateBuilder.run(...)` (new, Subtask A)
4. `EtlThroughputAggregateBuilder.run(..., sent_messages=<latest known value>)` (new, Subtask B)

Each periodic tick is a full recompute from scratch over all files currently
on disk — not incremental. The `sent_messages` value passed to the throughput
builder must be the latest value seen via `_on_generator_progress` (track it
in a new instance attribute, e.g. `self._latest_sent_messages: int = 0`,
updated inside the existing `_on_generator_progress` callback — this is an
addition to an existing method body, not a rewrite of its logic).

### Lifecycle rules

- The periodic timer task must be created only for the duration of
  `_run_phase1` (i.e. started right before or at the start of `_run_phase1`,
  cancelled right after `_run_phase1` returns or raises — use `try/finally`
  around the call to `_run_phase1` inside `run()`).
- Cancelling the timer task must not raise into the caller — swallow
  `asyncio.CancelledError` from the cancellation itself, but let it propagate
  correctly out of the timer's own coroutine per normal asyncio cancellation
  semantics (i.e. do not blanket-suppress `CancelledError` inside the timer
  loop's body — only catch it around the top-level `task.cancel()` +
  `await task` sequence in the calling code).
- Skip the tick entirely (do not run builders) if `stop_event.is_set()` at
  the moment the timer fires — avoid racing with an in-progress shutdown.
- If a periodic recompute raises an exception, log it (`logger.exception`)
  and continue the timer loop — a failed periodic snapshot must never crash
  the session or stop the timer for subsequent ticks.
- After `_run_phase1` completes, the existing sequential
  `statistics` -> `projection` phases run exactly as they do today. Add two
  new sequential phases after them: `stage_size_aggregates` and
  `throughput_aggregates`, each emitting a new `SessionProgress(phase=...)`
  event via the existing `_emit_progress` method (do not modify
  `_emit_progress` itself — only call it with new phase string literals).

### New dataclass fields

Extend `SessionResult` (append new optional fields, do not remove or reorder
existing ones — this is a frozen dataclass, so this is a deliberate,
minimal, backward-compatible addition):

```python
stage_size_aggregates: Optional[EtlStageSizeAggregateResult] = None
throughput_aggregates: Optional[EtlThroughputAggregateResult] = None
```

Extend `SessionProgress` similarly with:

```python
stage_size_aggregates: Optional[EtlStageSizeAggregateResult] = None
throughput_aggregates: Optional[EtlThroughputAggregateResult] = None
```

(Progress events for periodic ticks carry the full result of that tick, not
an incremental delta — consistent with "full recompute per tick.")

### New settings

`SessionWorker.__init__` gains two new required settings parameters:
`stage_size_settings: EtlStageSizeAggregateSettings` and
`throughput_settings: EtlThroughputAggregateSettings`, plus one new
plain argument `report_interval_sec: float` (validate `60 <= x <= 3600` at
the call site in `app.py`, not inside `worker.py`).

### Acceptance test

Smoke test on mocks (matching the existing "2 passed" smoke test style
mentioned in prior session notes): verify phase 1 still transitions through
`running` -> (statistics/projection/new phases) -> `done` exactly as before
when periodic snapshots are disabled (e.g. `report_interval_sec` larger than
the mock test's total run time), AND a second test where a short interval is
used with a fast mock generator, asserting at least one periodic snapshot
`SessionProgress` event with `phase` indicating a tick occurred, and that the
final `SessionResult.stage_size_aggregates` / `throughput_aggregates` are
populated exactly once (not duplicated by the last periodic tick colliding
with the final phase).

---

## Subtask D — UI rendering in app.py

**Scope: `app.py` only. Do not modify `worker.py` in this subtask.**

### During `running` / `statistics` / `projection` phases

In `_render_phase_running()`, after the existing generator/monitor/collector
event expanders, add two new `st.expander` blocks (only rendered if the
corresponding CSV file exists on disk and is non-empty):

- "Stage/Size Aggregates (preview)": read `stage_size_aggregates` output CSV
  with `pandas.read_csv` and render with `st.dataframe(...)`.
- "Throughput Aggregates (preview)": read `throughput_aggregates` output CSV
  and render with `st.dataframe(...)`.

Read paths from `st.session_state.settings` (new sidebar-configurable fields
for the two new module output paths — add them to `MODULE_DEFAULTS` and
`WIDGET_SPECS` under a new `"stage_size"` and `"throughput"` key, following
the exact existing pattern used for `"statistics"` and `"projection"`).

### In `_render_phase_done_or_failed()`

For each of the six CSV artifacts (`done`, `skipped`, `read_failures` from
`result.statistics`; `coefficients`, `stage_projection`, `total_projection`
from `result.projection`; plus the two new ones from
`result.stage_size_aggregates` and `result.throughput_aggregates`):

- Keep existing `st.caption(path)` calls unchanged.
- Add `st.download_button(label=..., data=<file bytes read at render time>,
  file_name=<path.name>, mime="text/csv")` for each. If the path is `None` or
  the file does not exist on disk, render `st.caption("<name>: not available")`
  instead of raising.
- Also render the two new aggregate tables with `st.dataframe(...)` in the
  final view, same as the running-phase preview.

### Constraints

- No new dependencies; pandas and streamlit are already imported in this
  module.
- Do not change `MODULE_DEFAULTS`/`WIDGET_SPECS` entries for existing five
  modules — only add two new keys.
- Add a new sidebar numeric input for `report_interval_sec` (minutes, 1–60,
  converted to seconds before being passed into settings construction in
  `build_settings_objects`).

### Acceptance check

Manual verification: start a session with mocked/short settings, confirm the
sidebar shows the two new module expanders, confirm preview tables appear
once the corresponding CSVs exist, confirm all eight download buttons appear
in the final view and produce non-empty file downloads matching the CSVs on
disk.

---

## Execution order (do not parallelize across subtasks)

1. Subtask A, with its unit test, reviewed.
2. Subtask B, with its unit test, reviewed.
3. Subtask C, building only on top of reviewed A and B, with its smoke test, reviewed.
4. Subtask D, building only on top of reviewed C's new `SessionResult`/`SessionProgress` fields.

Do not start a subtask until the previous one has passed its acceptance test.
