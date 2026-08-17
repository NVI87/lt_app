# Handoff — ETL load-test reporting

## Completed

- Added `etl_stage_size_aggregates.py`.
  - Reads DONE CSV.
  - Groups by `(attach_type, size_bucket_mb, stage)`.
  - Uses 1, 2, 4, ..., 1024 MB buckets.
  - Calculates total wall time, weighted CPU/RSS means, and peak CPU/RSS.

- Added `etl_throughput_aggregates.py`.
  - Reads OpenSearch monitor CSV.
  - Excludes failed monitor rows.
  - Uses the last cumulative count per `(minute, index)`.
  - Reindexes to a continuous minute sequence, forward-fills per index,
    computes per-index deltas, then sums deltas across indexes.
  - Computes coverage ratio and 5-minute trailing rolling metrics.

- Extended `worker.py`.
  - Runs periodic full snapshots only during phase 1.
  - Cancels and awaits the periodic task before final post-processing.
  - Uses a fresh `postprocessing_stop_event` for final builders.
  - Final throughput uses `gen_result.sent_messages`.
  - Protected methods were not changed:
    `_run_phase1`, `_wait_remaining`, `_emit_progress`.

- Extended `app.py`.
  - Adds Stage/Size and Throughput settings.
  - Adds report snapshot interval in minutes (1..60).
  - Shows aggregate CSV previews during running sessions.
  - Uses a 2.5-second rerun delay.
  - Adds final tables and download buttons for 8 CSV artifacts.

## Verification completed

```bash
.venv/bin/pytest lt_app/tests/ -v
# 13 passed

.venv/bin/python -m py_compile lt_app/app.py
# OK

.venv/bin/python -c "import lt_app.app; print('import: OK')"
# import: OK
```

## Not yet completed

- Manual Streamlit smoke test.
- Real Kafka/OpenSearch short load-test run.
- Verify periodic report after the configured interval.
- Verify all CSV downloads in browser.
- Update README / create QUICKSTART after the real smoke-run.
- Docker validation after local launch is confirmed.

## Rules for the next agent session

- Do not modify `worker.py` lifecycle methods without an explicit approved plan.
- Do not infer ETL field semantics; inspect real source data first.
- Work one subtask at a time:
  TASK -> PLAN -> APPROVAL -> IMPLEMENTATION -> REVIEW GATE.
- Do not commit, push, merge, rebase, or create a PR without explicit user instruction.
