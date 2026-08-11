#!/usr/bin/env python3
"""
Скрипт для сбора статистики по JSON-отчётам ETL пайплайна индексации
(intel-assist-index-errors-loc-*.json).

В расчёт статистики попадают ТОЛЬКО задачи/вложения, дошедшие до
финального статуса DONE / SUB_DONE. Задачи со статусом FAILED
(в т.ч. упавшие на CONVERTING/TOKENIZING без метрик) в статистику
по времени и ресурсам не включаются, но учитываются отдельно
в отчёте о доле неуспешных обработок.

Для каждого успешного (DONE) файла-вложения извлекается:
  - тип/размер вложения (attach_type, attach_size)
  - по каждой стадии FSM (EXTRACTING, CONVERTING, TOKENIZING, VECTORIZING, INDEXING):
      wall_time_sec, cpu_mean_pct, cpu_peak_pct, rss_mean, rss_peak, samples

Результат сохраняется в CSV (одна строка = одна стадия одного вложения)
и печатается сводная агрегированная статистика.

Использование:
    python etl_stats.py /path/to/reports_dir --out stats.csv
"""

import argparse
import glob
import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd

DONE_STATES = {"DONE", "SUB_DONE"}


def load_report(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def extract_rows(path, report):
    """Возвращает список строк (dict) — по одной на стадию FSM,
    только для вложений/документов, дошедших до DONE."""
    rows = []
    skipped = []  # для учёта не-DONE задач отдельно

    msg = report.get("message", {})
    head = msg.get("head_task", {})
    head_state = head.get("state")
    meta = head.get("meta", {})
    value = meta.get("value", {})
    attachments = value.get("attachments", [])
    trace = head.get("trace", {})
    started_ts = trace.get("created_at")
    started_dt = (
        datetime.fromtimestamp(started_ts, tz=timezone.utc) if started_ts else None
    )
    doc_id = meta.get("document_id")
    doc_name = meta.get("name")
    subtasks = msg.get("subtasks", {})
    sub_attachments = subtasks.get("attachments", [])

    if head_state not in DONE_STATES:
        skipped.append({
            "file": os.path.basename(path),
            "document_id": doc_id,
            "document_name": doc_name,
            "status": head_state,
        })
        return rows, skipped

    # Документ без вложений, но дошедший до DONE — фиксируем как факт,
    # без метрик по стадиям (для страниц без файлов стадии FSM не считаются).
    if not attachments:
        rows.append({
            "file": os.path.basename(path),
            "document_id": doc_id,
            "document_name": doc_name,
            "attach_type": "page_body",
            "attach_size": None,
            "stage": None,
            "status": head_state,
            "wall_time_sec": None,
            "cpu_mean_pct": None,
            "cpu_peak_pct": None,
            "rss_mean": None,
            "rss_peak": None,
            "samples": None,
            "started_at": started_dt,
        })
        return rows, skipped

    sub_by_id = {a.get("id"): a for a in sub_attachments}

    for att in attachments:
        att_id = att.get("id")
        att_type = att.get("fileType")
        att_size = att.get("fileSize")
        att_name = att.get("fileName")
        sub = sub_by_id.get(att_id)
        sub_state = (sub or {}).get("state")

        if sub_state not in DONE_STATES or not (sub or {}).get("stats"):
            skipped.append({
                "file": os.path.basename(path),
                "document_id": doc_id,
                "document_name": doc_name,
                "attach_name": att_name,
                "attach_type": att_type,
                "status": sub_state or head_state,
            })
            continue

        for stat_key, stat_val in sub["stats"].items():
            stage = stat_key.split(":")[-1]
            load = stat_val.get("load", {})
            rows.append({
                "file": os.path.basename(path),
                "document_id": doc_id,
                "document_name": doc_name,
                "attach_name": att_name,
                "attach_type": att_type,
                "attach_size": att_size,
                "stage": stage,
                "status": sub_state,
                "wall_time_sec": load.get("wall_time_sec"),
                "cpu_mean_pct": load.get("cpu_mean_pct"),
                "cpu_peak_pct": load.get("cpu_peak_pct"),
                "rss_mean": load.get("rss_mean"),
                "rss_peak": load.get("rss_peak"),
                "samples": load.get("samples"),
                "started_at": started_dt,
            })
    return rows, skipped

def collect(directory):
    all_rows = []
    all_skipped = []
    pattern = os.path.join(directory, "*.json")
    for path in sorted(glob.glob(pattern)):
        try:
            report = load_report(path)
        except Exception as e:
            print(f"[WARN] Не удалось прочитать {path}: {e}", file=sys.stderr)
            continue
        rows, skipped = extract_rows(path, report)
        all_rows.extend(rows)
        all_skipped.extend(skipped)
    return pd.DataFrame(all_rows), pd.DataFrame(all_skipped)

def summarize(df, skipped_df):
    print(f"Учтено в статистике (DONE): {len(df)} строк(и). "
          f"Пропущено как не-DONE: {len(skipped_df)}.")

    if not skipped_df.empty:
        print("\n=== Пропущенные (не дошли до DONE) ===")
        print(skipped_df.groupby("status").size())

    if df.empty:
        print("Нет успешно завершённых данных для анализа.")
        return

    stage_df = df[df["stage"].notna()]
    if not stage_df.empty:
        print("\n=== Статистика по стадиям обработки (только DONE) ===")
        stage_stats = (
            stage_df.groupby(["attach_type", "stage"])
            .agg(
                n=("wall_time_sec", "count"),
                wall_time_mean=("wall_time_sec", "mean"),
                wall_time_max=("wall_time_sec", "max"),
                rss_peak_mean_mb=("rss_peak", lambda s: s.mean() / 1024 / 1024),
                cpu_mean_pct=("cpu_mean_pct", "mean"),
            )
            .round(2)
        )
        print(stage_stats)

    print("\n=== Размер файла vs суммарное время обработки (по документам, DONE) ===")
    doc_time = (
        df[df["wall_time_sec"].notna()]
        .groupby(["document_id", "attach_type", "attach_size"])["wall_time_sec"]
        .sum()
        .reset_index()
        .sort_values("attach_size")
    )
    print(doc_time)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", help="Директория с JSON-отчётами")
    parser.add_argument("--out", default="etl_stats.csv", help="Путь для CSV-выгрузки (только DONE)")
    parser.add_argument("--out-skipped", default="etl_stats_skipped.csv",
                        help="Путь для CSV с не-DONE записями")
    args = parser.parse_args()

    df, skipped_df = collect(args.directory)
    df.to_csv(args.out, index=False)
    skipped_df.to_csv(args.out_skipped, index=False)
    print(f"Сохранено {len(df)} строк (DONE) в {args.out}, "
          f"{len(skipped_df)} пропущенных записей в {args.out_skipped}")
    summarize(df, skipped_df)

if __name__ == "__main__":
    main()
    # python etl_file_stats.py /err_events/ --out stats_done.csv --out-skipped stats_skipped.csv